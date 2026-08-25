"""爬取引擎：把调度器、下载器、Spider、管道串成一条流水线。

## 结束判定

这是爬虫引擎最容易写错的地方。「队列为空」并不等于「爬完了」——某个 worker
可能正在解析页面，马上就会产出新链接。因此必须同时满足两个条件：

    调度队列为空  且  没有任何在途请求（_inflight == 0）

关键顺序约束：`_inflight` 的递减必须发生在**新请求全部入队之后**。这由
_handle() 的 try/finally 结构保证——generator 消费完（新请求已 push）才会
走到 finally。顺序颠倒就会出现「最后一批链接尚未入队，引擎却判定结束」的
静默丢数据问题。
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from typing import Any

import structlog

from aiocrawler.downloader.base import BaseDownloader
from aiocrawler.downloader.browser import BrowserDownloader
from aiocrawler.downloader.http import HttpDownloader
from aiocrawler.downloader.router import DownloaderRouter
from aiocrawler.exceptions import IgnoreRequest
from aiocrawler.middleware import build_default_middlewares
from aiocrawler.middleware.base import Middleware, MiddlewareManager
from aiocrawler.models import Item, Request
from aiocrawler.pipeline.base import BasePipeline, PipelineManager
from aiocrawler.scheduler.base import BaseScheduler
from aiocrawler.scheduler.memory import MemoryScheduler
from aiocrawler.settings import Settings
from aiocrawler.spider import BaseSpider
from aiocrawler.stats import Stats

log = structlog.get_logger(__name__)

# 队列暂时为空但仍有在途请求时的轮询间隔。
# 取值权衡：太大拖慢收尾，太小空转耗 CPU。50ms 在两者间较平衡。
_IDLE_POLL = 0.05


class Engine:
    def __init__(
        self,
        spider: BaseSpider,
        settings: Settings | None = None,
        *,
        scheduler: BaseScheduler | None = None,
        downloader: BaseDownloader | None = None,
        pipelines: list[BasePipeline] | None = None,
        middlewares: list[Middleware] | None = None,
    ) -> None:
        base = settings or Settings()
        # 只有当调用方没处理过 custom_settings 时才在这里合并。load_settings()
        # 已经按「custom_settings < [spider.x] < 命令行」的顺序叠好了层，这里
        # 再叠一次会把 custom_settings 顶到最高优先级，命令行参数就此失效。
        self.settings = (
            base if base.custom_settings_applied else base.merged(spider.custom_settings)
        )

        self.spider = spider
        self.stats = Stats()
        self.scheduler: BaseScheduler = scheduler or MemoryScheduler()
        self.downloader: BaseDownloader = downloader or self._build_downloader()
        self.pipelines = PipelineManager(pipelines or [])
        # 传 None 用默认链；传空列表则显式表示「不要任何中间件」（测试常用）
        chain = build_default_middlewares(self.settings) if middlewares is None else middlewares
        self.middlewares = MiddlewareManager(
            self.downloader, chain, concurrency_per_domain=self._per_domain_limit()
        )

        self._inflight = 0
        self._stopping = False
        self._reporter: asyncio.Task[None] | None = None
        self._prev_handlers: list[signal.Signals] = []

    def _per_domain_limit(self) -> int | None:
        """单域名并发上限，不可能触发时返回 None。

        上限不小于全局并发数时，同域名请求根本凑不够那么多，信号量永远不会
        阻塞——这时留着它只是白白多一层 acquire/release。
        """
        limit = self.settings.concurrency_per_domain
        return limit if limit < self.settings.concurrency else None

    def _build_downloader(self) -> BaseDownloader:
        """默认装配混合下载器。

        浏览器部分是惰性的：只有当 spider 真的产出 renderer="browser" 的请求时，
        Chromium 才会被启动。因此纯 HTTP 的爬虫不会为此付出任何代价，也不要求
        必须安装 playwright。
        """
        s = self.settings
        http = HttpDownloader(
            timeout=s.timeout,
            follow_redirects=s.follow_redirects,
            http2=s.http2,
            verify_ssl=s.verify_ssl,
            default_headers=s.default_headers,
            proxy=s.proxy,
            max_response_bytes=s.max_response_bytes,
        )
        browser = BrowserDownloader(
            headless=s.browser_headless,
            contexts=s.browser_contexts,
            timeout=s.browser_timeout,
            wait_until=s.browser_wait_until,
            block_resources=frozenset(s.browser_block_resources),
            proxy=s.proxy,
            verify_ssl=s.verify_ssl,
        )
        return DownloaderRouter(http, browser, lazy_browser=True)

    # ------------------------------------------------------------------ 对外

    async def run(self) -> Stats:
        self._install_signal_handlers()
        try:
            await self.spider.on_start()
            await self.scheduler.open()
            await self.downloader.start()
            await self.middlewares.open(self.spider)
            await self.pipelines.open(self.spider)
        except BaseException:
            # 装配到一半失败时，前面已经打开的组件同样需要收尾，
            # 否则会漏掉数据库连接、浏览器进程这类外部资源
            await self._shutdown()
            raise
        self._reporter = asyncio.create_task(self._report_progress())

        try:
            seeded = 0
            for request in self.spider.start_requests():
                if await self._schedule(request):
                    seeded += 1
            if seeded == 0:
                log.warning("no_start_requests", spider=self.spider.name)

            log.info(
                "crawl_started",
                spider=self.spider.name,
                concurrency=self.settings.concurrency,
                seed_requests=seeded,
            )

            async with asyncio.TaskGroup() as tg:
                for i in range(self.settings.concurrency):
                    tg.create_task(self._worker(i), name=f"worker-{i}")
        finally:
            await self._shutdown()

        log.info("crawl_finished", spider=self.spider.name, summary=self.stats.summary())
        return self.stats

    # ------------------------------------------------------------------ 内部

    async def _worker(self, wid: int) -> None:
        while True:
            if self._stopping:
                return
            request = await self.scheduler.pop()

            if request is None:
                # 队列空了，但只有在没有在途请求时才能确认爬取真的结束
                if self._inflight == 0:
                    if not await self._wait_for_work():
                        return
                    continue
                await asyncio.sleep(_IDLE_POLL)
                continue

            self._inflight += 1
            try:
                await self._handle(request)
            except Exception:
                # 单个请求的失败不应该拖垮 worker，否则并发度会逐步衰减到 0
                self.stats.inc("request/failed")
                log.exception("request_handling_failed", url=request.url)
            finally:
                # ack 必须在新请求入队之后（_handle 已返回），持久化调度器
                # 才能安全地把这条记录移出队列。
                # ack 失败（如数据库瞬时不可用）只影响这一条记录的记账，不能让
                # 异常逃出 worker——TaskGroup 会因此取消其余所有 worker，
                # 把一次局部故障放大成整轮抓取中断。
                try:
                    await self.scheduler.ack(request)
                except Exception:
                    self.stats.inc("request/ack_failed")
                    log.exception("scheduler_ack_failed", url=request.url)
                finally:
                    self._inflight -= 1

    async def _wait_for_work(self) -> bool:
        """本地已无事可做时，决定是收工还是再等等。

        单机场景（idle_timeout=0）直接收工：队列空 + 无在途，就是真的爬完了。

        分布式场景必须等待：共享队列此刻为空，很可能只是别的节点正在解析页面，
        新链接马上就会进来。不等就走，节点会在抓取中途提前离场。

        返回 True 表示有新活干了，False 表示可以收工。
        """
        timeout = self.settings.idle_timeout
        if timeout <= 0:
            return False

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if self._stopping:
                return False
            await asyncio.sleep(_IDLE_POLL)
            if self._inflight > 0 or await self.scheduler.size() > 0:
                return True

        log.info("idle_timeout_reached", seconds=timeout)
        return False

    async def _handle(self, request: Request) -> None:
        try:
            result = await self.middlewares.download(request)
        except IgnoreRequest as exc:
            self.stats.inc("request/ignored")
            log.debug("request_ignored", url=request.url, reason=str(exc))
            return
        except Exception as exc:
            self.stats.inc("request/failed")
            log.warning(
                "download_failed",
                url=request.url,
                error=f"{type(exc).__name__}: {exc}",
            )
            return

        if result is None:
            # 中间件彻底放弃了该请求（典型情况：重试次数耗尽）
            self.stats.inc("request/abandoned")
            return

        if isinstance(result, Request):
            # 中间件要求重新调度。重试请求带 dont_filter=True，不会被去重拦下
            self.stats.inc("request/retried")
            await self._schedule(result)
            return

        response = result
        self.stats.inc("response/received")
        self.stats.inc(f"response/status/{response.status}")

        callback = self.spider.get_callback(request.callback)
        async for obj in callback(response):
            if isinstance(obj, Request):
                await self._schedule(obj)
            elif isinstance(obj, Item):
                await self._collect(obj)
            else:
                log.warning(
                    "unexpected_yield",
                    type=type(obj).__name__,
                    hint="parse() 只应 yield Item 或 Request",
                )

    async def _schedule(self, request: Request) -> bool:
        accepted = await self.scheduler.push(request)
        self.stats.inc("request/scheduled" if accepted else "request/duplicated")
        return accepted

    async def _collect(self, item: Item) -> None:
        if await self.pipelines.process(item, self.spider):
            self.stats.inc("item/stored")
            limit = self.settings.max_items
            if limit is not None and self.stats.get("item/stored") >= limit:
                log.info("max_items_reached", limit=limit)
                self._stopping = True
        else:
            self.stats.inc("item/dropped")

    async def _report_progress(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.settings.stats_interval)
                log.info(
                    "progress",
                    queued=await self.scheduler.size(),
                    inflight=self._inflight,
                    **self.stats.snapshot(),
                )
        except asyncio.CancelledError:
            pass

    async def _shutdown(self) -> None:
        if self._reporter is not None:
            self._reporter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reporter
            self._reporter = None

        # 顺序要紧：先关管道（把缓冲刷进存储），再关中间件、下载器和调度器。
        # 每一步各自兜异常：任何一步抛错都不能连累后面的步骤，否则一个组件
        # 关闭失败就会漏掉数据库连接、浏览器进程，甚至跳过 spider 的收尾钩子。
        for label, closer in (
            ("pipelines", lambda: self.pipelines.close(self.spider)),
            ("middlewares", lambda: self.middlewares.close(self.spider)),
            ("downloader", self.downloader.close),
            ("scheduler", self.scheduler.close),
            ("spider", self.spider.on_close),
        ):
            try:
                await closer()
            except Exception:
                log.exception("component_close_failed", component=label)

        self._restore_signal_handlers()

    def _install_signal_handlers(self) -> None:
        """Ctrl-C 时优雅收尾：停止取新请求，让在途请求跑完，刷净缓冲。

        再按一次 Ctrl-C 立刻恢复默认行为直接中断——收尾本身也可能卡住
        （比如存储后端没响应），必须给使用者留一条硬退出的路。
        """
        loop = asyncio.get_running_loop()
        self._prev_handlers = []

        def _request_stop(sig: signal.Signals) -> None:
            self._stopping = True
            # 让位给默认行为：下一次同样的信号直接中断进程
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(sig)
            log.warning("shutdown_requested", signal=sig.name,
                        hint="正在等待在途请求完成并刷写缓冲，再按一次 Ctrl-C 强制退出")

        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, _request_stop, sig)
                self._prev_handlers.append(sig)

    def _restore_signal_handlers(self) -> None:
        """交还信号控制权。

        引擎可能只是宿主程序里的一段流程，跑完之后不该继续霸占 SIGINT。
        """
        loop = asyncio.get_running_loop()
        for sig in getattr(self, "_prev_handlers", []):
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(sig)
        self._prev_handlers = []


async def crawl(
    spider: BaseSpider,
    settings: Settings | None = None,
    **kwargs: Any,
) -> Stats:
    """便捷入口：跑完一个 spider 并返回统计。"""
    return await Engine(spider, settings, **kwargs).run()
