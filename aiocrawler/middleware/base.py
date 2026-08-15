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
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from aiocrawler.downloader.base import BaseDownloader
from aiocrawler.models import Request, Response

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


class MiddlewareManager:
    """把中间件链包在下载器外面，对引擎暴露单个 download() 入口。"""

    def __init__(self, downloader: BaseDownloader, middlewares: list[Middleware] | None = None) -> None:
        self._downloader = downloader
        self._mws = middlewares or []

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
        try:
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
            response = result
        return response
