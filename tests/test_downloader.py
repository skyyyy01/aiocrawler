"""下载器路由与浏览器渲染。

Router 的分发逻辑用假下载器验证（快）；BrowserDownloader 的渲染能力则必须用
真实 Chromium 验证——渲染是它存在的唯一理由，打桩验证等于什么都没验证。
浏览器相关用例统一打上 browser 标记，可用 `-m "not browser"` 跳过。
"""

from __future__ import annotations

import asyncio

import pytest

from aiocrawler.downloader.browser import BrowserDownloader
from aiocrawler.downloader.router import DownloaderRouter
from aiocrawler.models import Request, Response


class TrackingDownloader:
    """记录自身被启动、调用、关闭的次数。"""

    def __init__(self, tag: str) -> None:
        self.tag = tag
        self.starts = 0
        self.closes = 0
        self.fetched: list[str] = []

    async def start(self) -> None:
        self.starts += 1

    async def fetch(self, request: Request) -> Response:
        self.fetched.append(request.url)
        return Response(
            url=request.url, status=200, headers={}, body=self.tag.encode(), request=request
        )

    async def close(self) -> None:
        self.closes += 1


class TestRouter:
    async def test_http_request_goes_to_http(self):
        http, browser = TrackingDownloader("http"), TrackingDownloader("browser")
        router = DownloaderRouter(http, browser)
        await router.start()

        resp = await router.fetch(Request("http://a.com/", renderer="http"))
        assert resp.body == b"http"
        assert browser.fetched == []

    async def test_browser_request_goes_to_browser(self):
        http, browser = TrackingDownloader("http"), TrackingDownloader("browser")
        router = DownloaderRouter(http, browser)
        await router.start()

        resp = await router.fetch(Request("http://a.com/", renderer="browser"))
        assert resp.body == b"browser"
        assert http.fetched == []

    async def test_browser_not_started_without_browser_requests(self):
        """纯 HTTP 的爬虫不应付出启动浏览器的代价。"""
        http, browser = TrackingDownloader("http"), TrackingDownloader("browser")
        router = DownloaderRouter(http, browser, lazy_browser=True)
        await router.start()
        for i in range(5):
            await router.fetch(Request(f"http://a.com/{i}"))

        assert browser.starts == 0
        await router.close()
        assert browser.closes == 0  # 没启动过就不该去关

    async def test_browser_started_lazily_once(self):
        http, browser = TrackingDownloader("http"), TrackingDownloader("browser")
        router = DownloaderRouter(http, browser, lazy_browser=True)
        await router.start()

        # 并发触发首次启动，双检锁应保证只启动一次
        await asyncio.gather(*[
            router.fetch(Request(f"http://a.com/{i}", renderer="browser")) for i in range(8)
        ])
        assert browser.starts == 1
        assert len(browser.fetched) == 8

    async def test_eager_mode_starts_browser_upfront(self):
        http, browser = TrackingDownloader("http"), TrackingDownloader("browser")
        router = DownloaderRouter(http, browser, lazy_browser=False)
        await router.start()
        assert browser.starts == 1

    async def test_browser_request_without_browser_downloader_errors(self):
        router = DownloaderRouter(TrackingDownloader("http"), None)
        await router.start()
        with pytest.raises(RuntimeError, match="未配置浏览器下载器"):
            await router.fetch(Request("http://a.com/", renderer="browser"))

    async def test_close_propagates(self):
        http, browser = TrackingDownloader("http"), TrackingDownloader("browser")
        router = DownloaderRouter(http, browser, lazy_browser=False)
        await router.start()
        await router.close()
        assert http.closes == 1 and browser.closes == 1


# --------------------------------------------------------------------------
# 以下用真实 Chromium，跑在本地 HTTP 服务器上，不触达公网
# --------------------------------------------------------------------------

JS_PAGE = """<!doctype html><html><head><title>t</title></head><body>
<div id="app"></div>
<script>
  var data = [{"t": "\\u7b2c\\u4e00\\u6761"}, {"t": "\\u7b2c\\u4e8c\\u6761"}];
  var app = document.getElementById('app');
  data.forEach(function (d) {
    var el = document.createElement('div');
    el.className = 'row';
    el.textContent = d.t;
    app.appendChild(el);
  });
</script>
</body></html>"""


class StaticServer:
    """只返回一个固定 JS 页面的最小服务器。"""

    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/"

    async def _handle(self, reader, writer) -> None:
        try:
            await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        except Exception:
            writer.close()
            return
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
            b"Content-Length: " + str(len(self._body)).encode() + b"\r\n"
            b"Connection: close\r\n\r\n" + self._body
        )
        await writer.drain()
        writer.close()


@pytest.fixture
async def js_server():
    srv = StaticServer(JS_PAGE)
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()


@pytest.mark.browser
class TestBrowserDownloader:
    async def test_renders_javascript_content(self, js_server):
        """核心能力：JS 生成的 DOM 必须出现在返回的 HTML 里。"""
        d = BrowserDownloader(contexts=1, timeout=20)
        await d.start()
        try:
            resp = await d.fetch(Request(js_server.url, renderer="browser"))
        finally:
            await d.close()

        assert resp.status == 200
        assert resp.rendered is True
        rows = resp.css("div.row")
        assert len(rows) == 2
        assert rows[0].text(strip=True) == "第一条"

    async def test_http_downloader_cannot_see_js_content(self, js_server):
        """对照组：同一页面走 HTTP 抓不到任何渲染结果。"""
        from aiocrawler.downloader.http import HttpDownloader

        d = HttpDownloader(timeout=10, http2=False)
        await d.start()
        try:
            resp = await d.fetch(Request(js_server.url))
        finally:
            await d.close()

        assert resp.status == 200
        assert len(resp.css("div.row")) == 0   # JS 未执行，DOM 里没有内容
        assert resp.rendered is False

    async def test_wait_for_selector(self, js_server):
        d = BrowserDownloader(contexts=1, timeout=20)
        await d.start()
        try:
            req = Request(js_server.url, renderer="browser", meta={"wait_for": "div.row"})
            resp = await d.fetch(req)
        finally:
            await d.close()
        assert len(resp.css("div.row")) == 2

    async def test_context_pool_limits_concurrency(self, js_server):
        """池大小为 2 时，8 个并发请求应全部完成且不出错。"""
        d = BrowserDownloader(contexts=2, timeout=20)
        await d.start()
        try:
            results = await asyncio.gather(*[
                d.fetch(Request(js_server.url, renderer="browser")) for _ in range(8)
            ])
        finally:
            await d.close()

        assert len(results) == 8
        assert all(len(r.css("div.row")) == 2 for r in results)

    async def test_fetch_before_start_raises(self):
        d = BrowserDownloader()
        with pytest.raises(RuntimeError, match="未启动"):
            await d.fetch(Request("http://a.com/", renderer="browser"))
