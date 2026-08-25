"""下载中间件：在请求发出前、响应返回后插入处理逻辑。

## 三个钩子的语义

`process_request(request)` —— 请求发出前，按注册顺序**正序**执行：
    返回 None      → 交给下一个中间件（最常见）
    返回 Request   → 放弃本次下载，把新请求重新入队
    返回 Response  → 短路，直接进入响应链（如命中缓存）
    抛 IgnoreRequest → 丢弃该请求

`process_response(request, response)` —— 响应返回后，按**逆序**执行：
    返回 Response  → 交给下一个中间件
    返回 Request   → 重新入队（重试就走这条路）

`process_exception(request, exc)` —— 下载抛异常时，按**逆序**执行：
    返回 None      → 继续传给下一个中间件，都不处理则异常上抛给引擎
    返回 Response  → 用它顶替，异常被消化
    返回 Request   → 重新入队

## 为什么响应侧是逆序

请求正序、响应逆序构成对称的洋葱结构：先加工请求的中间件，最后加工响应。
这样「限速 → 加 UA → 下载 → 判定重试」的包裹关系才是自然的。

## 单域名并发上限

`concurrency_per_domain` 在这里落地，而不是做成一个中间件——中间件只有
process_request / process_response 两个分离的钩子，没法用一个 try/finally
把「取额度」和「还额度」扣在一起；下载一旦抛异常，额度就漏了，那个域名会被
永久卡死。这里的 download() 天然包住了完整的下载过程，是唯一能放稳的位置。

**信号量只圈住 fetch()，绝不能圈住整条中间件链。** 限速的等待在
ThrottleMiddleware.process_request 里，重试的退避在 RetryMiddleware
的 process_response 里，两者都是 sleep。把它们圈进来，额度就会被睡觉的协程
长期占着，「同域名最多 N 个在飞」立刻变成「同域名最多 N 个在排队」，
上限形同虚设。缓存命中（process_request 直接返回 Response）压根没走网络，
也就不该占额度——只圈 fetch 同样天然满足这一点。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from typing import TYPE_CHECKING, AsyncIterator

import structlog

from aiocrawler.downloader.base import BaseDownloader
from aiocrawler.models import Request, Response, domain_key

if TYPE_CHECKING:
    from aiocrawler.spider import BaseSpider

log = structlog.get_logger(__name__)


class Middleware:
    """中间件基类。子类只需覆盖关心的钩子，其余用默认放行实现。"""

    async def open_spider(self, spider: BaseSpider) -> None:
        ...

    async def process_request(self, request: Request) -> Request | Response | None:
        return None

    async def process_response(self, request: Request, response: Response) -> Response | Request:
        return response

    async def process_exception(self, request: Request, exc: Exception) -> Request | Response | None:
        return None

    async def close_spider(self, spider: BaseSpider) -> None:
        ...


class _DomainSlots:
    """按域名发放并发额度。

    条目用引用计数管理：某个域名当前没有请求在用它时，信号量立刻被回收。
    因此内存只与「此刻活跃的域名数」成正比，而不是「历史见过的域名数」——
    全网漫游型爬虫会遇到几十万个域名，按后者留存迟早把内存吃满。
    """

    __slots__ = ("_limit", "_sems", "_users")

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._sems: dict[str, asyncio.Semaphore] = {}
        self._users: Counter[str] = Counter()

    @contextlib.asynccontextmanager
    async def acquire(self, url: str) -> AsyncIterator[None]:
        domain = domain_key(url)
        sem = self._sems.get(domain)
        if sem is None:
            sem = asyncio.Semaphore(self._limit)
            self._sems[domain] = sem

        # 引用计数要在等待信号量**之前**就加上，否则排队期间条目可能被回收，
        # 后来者会新建一个信号量，两拨请求各自计数，上限就被绕过了
        self._users[domain] += 1
        try:
            async with sem:
                yield
        finally:
            self._users[domain] -= 1
            if self._users[domain] <= 0:
                del self._users[domain]
                self._sems.pop(domain, None)

    def active_domains(self) -> int:
        """当前占用额度的域名数，供测试与排查用。"""
        return len(self._sems)


class MiddlewareManager:
    """把中间件链包在下载器外面，对引擎暴露单个 download() 入口。"""

    def __init__(
        self,
        downloader: BaseDownloader,
        middlewares: list[Middleware] | None = None,
        *,
        concurrency_per_domain: int | None = None,
    ) -> None:
        """
        :param concurrency_per_domain: 单域名同时在途的请求数上限。
            None 表示不限——上限不小于全局并发时它永远触发不了，
            此时应当传 None 以省掉这层开销。
        """
        self._downloader = downloader
        self._mws = middlewares or []
        self._slots = (
            _DomainSlots(concurrency_per_domain)
            if concurrency_per_domain is not None
            else None
        )

    async def open(self, spider: BaseSpider) -> None:
        for mw in self._mws:
            await mw.open_spider(spider)

    async def close(self, spider: BaseSpider) -> None:
        for mw in reversed(self._mws):
            try:
                await mw.close_spider(spider)
            except Exception:
                log.exception("middleware_close_failed", middleware=type(mw).__name__)

    async def download(self, request: Request) -> Response | Request | None:
        """执行一次完整下载。

        返回 Response 表示成功；返回 Request 表示需要重新入队（重试/重定向）；
        返回 None 表示该请求已被放弃。IgnoreRequest 由调用方（引擎）捕获。
        """
        # ---- 请求侧：正序 ----
        for mw in self._mws:
            result = await mw.process_request(request)
            if result is None:
                continue
            if isinstance(result, Response):
                return await self._handle_response(request, result)
            # 是 Request：放弃本次下载，让引擎重新调度
            return result

        # ---- 实际下载 ----
        # 额度只圈这一段：限速与重试退避的 sleep 都在中间件钩子里，
        # 圈进来会让额度被睡觉的协程占住（见模块文档）
        try:
            if self._slots is None:
                response = await self._downloader.fetch(request)
            else:
                async with self._slots.acquire(request.url):
                    response = await self._downloader.fetch(request)
        except Exception as exc:
            for mw in reversed(self._mws):
                result = await mw.process_exception(request, exc)
                if result is not None:
                    return result
            raise  # 无人消化，交给引擎记失败

        # ---- 响应侧：逆序 ----
        return await self._handle_response(request, response)

    async def _handle_response(self, request: Request, response: Response) -> Response | Request:
        for mw in reversed(self._mws):
            result = await mw.process_response(request, response)
            if isinstance(result, Request):
                return result
            if result is None:
                # process_response 忘了 return 是很常见的笔误。放任 None 传下去，
                # 引擎会把它当作「请求已被放弃」，于是这个页面被静默丢掉——
                # 没有报错，只是数据莫名其妙少了一页
                raise TypeError(
                    f"{type(mw).__name__}.process_response() 返回了 None；"
                    "它必须返回 Response 或 Request（放行时请 return response）"
                )
            response = result
        return response
