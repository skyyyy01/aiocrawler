"""基于 Playwright 的浏览器下载器，用于 JS 渲染页面。

## 性能：为什么是 context 池

浏览器渲染本就比 HTTP 慢一到两个数量级，实现方式再出错就完全不可用了。
两个关键决策：

1. **全程只启动一个 Browser 进程**。每个请求都 launch 一次浏览器，光进程启动
   就要 1~2 秒，比页面加载本身还慢。

2. **预建固定数量的 BrowserContext 并复用**，用 asyncio.Queue 当池子。队列为空时
   `get()` 自动等待，天然实现了并发上限，不必再单独加信号量。Context 之间
   cookie 隔离，同一 context 内复用则能保持会话状态。

Page 是每请求新建的——它足够轻，且能避免上一个页面的 JS 定时器、监听器污染下一个。

## 资源拦截

默认拦截图片、字体、媒体。爬虫只要 DOM，下载这些纯属浪费带宽和时间，
实测能显著缩短渲染耗时。需要截图时把 block_resources 设为空即可。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import structlog

from aiocrawler.exceptions import NotConfigured
from aiocrawler.models import Request, Response

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext

log = structlog.get_logger(__name__)

#: 对爬虫无用、默认拦截的资源类型
DEFAULT_BLOCKED_RESOURCES = frozenset({"image", "media", "font"})

#: 等待策略：
#:   commit            —— 收到响应头就返回，最快，适合只要初始 HTML
#:   domcontentloaded  —— DOM 构建完成，多数 SPA 首屏够用（默认）
#:   load              —— 含子资源加载完成
#:   networkidle       —— 网络空闲 500ms，最慢但最保险，适合懒加载页面
WaitUntil = str


class BrowserDownloader:
    def __init__(
        self,
        *,
        headless: bool = True,
        contexts: int = 4,
        timeout: float = 30.0,
        wait_until: WaitUntil = "domcontentloaded",
        user_agent: str | None = None,
        viewport: dict[str, int] | None = None,
        block_resources: frozenset[str] | set[str] = DEFAULT_BLOCKED_RESOURCES,
        proxy: str | None = None,
        browser_type: str = "chromium",
    ) -> None:
        self._headless = headless
        self._n_contexts = max(1, contexts)
        self._timeout_ms = int(timeout * 1000)
        self._wait_until = wait_until
        self._user_agent = user_agent
        self._viewport = viewport or {"width": 1366, "height": 900}
        self._blocked = frozenset(block_resources)
        self._proxy = proxy
        self._browser_type = browser_type

        self._pw: Any = None
        self._browser: Any = None
        self._pool: asyncio.Queue[BrowserContext] | None = None
        self._contexts: list[BrowserContext] = []

    async def start(self) -> None:
        if self._browser is not None:
            return

        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:  # pragma: no cover - 取决于安装情况
            raise NotConfigured(
                "浏览器下载器需要 playwright，请执行：\n"
                '  pip install -e ".[browser]" && playwright install chromium'
            ) from exc

        self._pw = await async_playwright().start()
        launcher = getattr(self._pw, self._browser_type)
        self._browser = await launcher.launch(
            headless=self._headless,
            proxy={"server": self._proxy} if self._proxy else None,
        )

        self._pool = asyncio.Queue()
        for _ in range(self._n_contexts):
            ctx = await self._new_context()
            self._contexts.append(ctx)
            self._pool.put_nowait(ctx)

        log.info(
            "browser_started",
            type=self._browser_type,
            headless=self._headless,
            contexts=self._n_contexts,
        )

    async def _new_context(self) -> BrowserContext:
        ctx = await self._browser.new_context(
            user_agent=self._user_agent,
            viewport=self._viewport,
            ignore_https_errors=True,
        )
        ctx.set_default_timeout(self._timeout_ms)
        if self._blocked:
            await ctx.route("**/*", self._route_filter)
        return ctx

    async def _route_filter(self, route: Any) -> None:
        if route.request.resource_type in self._blocked:
            await route.abort()
        else:
            await route.continue_()

    async def fetch(self, request: Request) -> Response:
        if self._pool is None:
            raise RuntimeError("浏览器下载器未启动，请先 await start()")

        # 池为空时在此排队，等同于并发上限
        ctx = await self._pool.get()
        try:
            return await self._fetch_with(ctx, request)
        finally:
            self._pool.put_nowait(ctx)

    async def _fetch_with(self, ctx: BrowserContext, request: Request) -> Response:
        page = await ctx.new_page()
        try:
            if request.headers:
                await page.set_extra_http_headers(request.headers)

            timeout_ms = (
                int(request.timeout * 1000) if request.timeout is not None else self._timeout_ms
            )
            resp = await page.goto(
                request.url,
                wait_until=request.meta.get("wait_until", self._wait_until),
                timeout=timeout_ms,
            )

            # spider 可通过 meta 要求等待某个元素出现，应对异步渲染的内容
            selector = request.meta.get("wait_for")
            if selector:
                await page.wait_for_selector(selector, timeout=timeout_ms)

            # 额外静置时间，用于等待动画或轮询式加载
            extra_wait = request.meta.get("wait_time")
            if extra_wait:
                await page.wait_for_timeout(float(extra_wait) * 1000)

            html = await page.content()
            headers = dict(resp.headers) if resp is not None else {}
            status = resp.status if resp is not None else 200

            return Response(
                # 用 page.url：JS 跳转后这里才是真实地址
                url=page.url,
                status=status,
                headers=headers,
                body=html.encode("utf-8"),
                request=request,
                encoding="utf-8",
                rendered=True,
            )
        finally:
            await page.close()

    async def close(self) -> None:
        for ctx in self._contexts:
            try:
                await ctx.close()
            except Exception:  # pragma: no cover - 关闭期异常无需中断流程
                pass
        self._contexts.clear()
        self._pool = None

        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._pw is not None:
            await self._pw.stop()
            self._pw = None
