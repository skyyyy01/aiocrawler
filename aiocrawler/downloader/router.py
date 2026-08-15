"""混合下载器：按 request.renderer 把请求分发给 HTTP 或浏览器下载器。

默认全部走 HTTP；spider 只需给需要 JS 渲染的请求标上 `renderer="browser"`：

    yield Request(url, callback="parse_detail", renderer="browser")

浏览器下载器是**惰性启动**的：如果整轮抓取里没有任何一个 browser 请求，
Chromium 进程根本不会被拉起来。这样即使把 Router 设成默认下载器，纯 HTTP
的爬虫也不会白白付出启动浏览器的代价。
"""

from __future__ import annotations

import asyncio

import structlog

from aiocrawler.downloader.base import BaseDownloader
from aiocrawler.models import Request, Response

log = structlog.get_logger(__name__)


class DownloaderRouter:
    def __init__(
        self,
        http: BaseDownloader,
        browser: BaseDownloader | None = None,
        *,
        lazy_browser: bool = True,
    ) -> None:
        self._http = http
        self._browser = browser
        self._lazy = lazy_browser
        self._browser_started = False
        # 并发下可能有多个请求同时触发首次启动，用锁保证只启动一次
        self._start_lock = asyncio.Lock()

    async def start(self) -> None:
        await self._http.start()
        if self._browser is not None and not self._lazy:
            await self._browser.start()
            self._browser_started = True

    async def fetch(self, request: Request) -> Response:
        if request.renderer != "browser":
            return await self._http.fetch(request)

        if self._browser is None:
            raise RuntimeError(
                f"请求要求浏览器渲染但未配置浏览器下载器：{request.url}\n"
                '请安装 pip install -e ".[browser]" 并在构造 Router 时传入 BrowserDownloader'
            )

        await self._ensure_browser()
        return await self._browser.fetch(request)

    async def _ensure_browser(self) -> None:
        if self._browser_started:
            return
        async with self._start_lock:
            if self._browser_started:  # 双检：等锁期间可能已被其他协程启动
                return
            log.info("browser_lazy_start", reason="首次遇到 renderer=browser 的请求")
            await self._browser.start()
            self._browser_started = True

    async def close(self) -> None:
        await self._http.close()
        if self._browser is not None and self._browser_started:
            await self._browser.close()
            self._browser_started = False
