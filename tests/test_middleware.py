"""中间件链与各内置中间件。"""

from __future__ import annotations

import asyncio

import httpx
import pytest
import respx

from aiocrawler.exceptions import IgnoreRequest
from aiocrawler.middleware import build_default_middlewares
from aiocrawler.middleware.base import Middleware, MiddlewareManager
from aiocrawler.middleware.proxy import ProxyMiddleware
from aiocrawler.middleware.retry import RetryMiddleware
from aiocrawler.middleware.robots import RobotsMiddleware
from aiocrawler.middleware.throttle import ThrottleMiddleware, domain_key
from aiocrawler.middleware.useragent import UserAgentMiddleware
from aiocrawler.models import Request, Response
from aiocrawler.settings import Settings


class FakeDownloader:
    """可编程下载器：按需返回响应或抛异常，并记录收到的请求。"""

    def __init__(self, status: int = 200, exc: Exception | None = None) -> None:
        self.status = status
        self.exc = exc
        self.seen: list[Request] = []

    async def start(self) -> None:
        ...

    async def fetch(self, request: Request) -> Response:
        self.seen.append(request)
        if self.exc is not None:
            raise self.exc
        return Response(
            url=request.url, status=self.status, headers={}, body=b"<html></html>", request=request
        )

    async def close(self) -> None:
        ...


class RecordingMiddleware(Middleware):
    """把自己的调用记进共享列表，用于验证链的执行顺序。"""

    def __init__(self, tag: str, sink: list[str]) -> None:
        self.tag = tag
        self.sink = sink

    async def process_request(self, request):
        self.sink.append(f"req:{self.tag}")
        return None

    async def process_response(self, request, response):
        self.sink.append(f"resp:{self.tag}")
        return response

    async def process_exception(self, request, exc):
        self.sink.append(f"exc:{self.tag}")
        return None


class TestMiddlewareOrder:
    async def test_request_forward_response_reverse(self):
        """请求侧正序、响应侧逆序，构成对称的洋葱结构。"""
        sink: list[str] = []
        mgr = MiddlewareManager(
            FakeDownloader(),
            [RecordingMiddleware("A", sink), RecordingMiddleware("B", sink)],
        )
        await mgr.download(Request("http://a.com/"))
        assert sink == ["req:A", "req:B", "resp:B", "resp:A"]

    async def test_exception_chain_is_reverse(self):
        sink: list[str] = []
        mgr = MiddlewareManager(
            FakeDownloader(exc=httpx.ConnectError("x")),
            [RecordingMiddleware("A", sink), RecordingMiddleware("B", sink)],
        )
        with pytest.raises(httpx.ConnectError):
            await mgr.download(Request("http://a.com/"))
        assert sink == ["req:A", "req:B", "exc:B", "exc:A"]

    async def test_request_returning_response_short_circuits(self):
        """process_request 返回 Response 时应跳过下载，直接进响应链。"""

        class CacheHit(Middleware):
            async def process_request(self, request):
                return Response(url=request.url, status=200, headers={}, body=b"cached", request=request)

        downloader = FakeDownloader()
        mgr = MiddlewareManager(downloader, [CacheHit()])
        result = await mgr.download(Request("http://a.com/"))

        assert isinstance(result, Response) and result.body == b"cached"
        assert downloader.seen == []  # 下载器根本没被调用

    async def test_ignore_request_propagates(self):
        class Blocker(Middleware):
            async def process_request(self, request):
                raise IgnoreRequest("blocked")

        mgr = MiddlewareManager(FakeDownloader(), [Blocker()])
        with pytest.raises(IgnoreRequest):
            await mgr.download(Request("http://a.com/"))


class TestRetryMiddleware:
    def _mw(self, **kw):
        kw.setdefault("backoff_base", 0.001)  # 让测试跑得快
        return RetryMiddleware(**kw)

    async def test_retryable_status_returns_new_request(self):
        mw = self._mw(max_retries=2)
        req = Request("http://a.com/")
        resp = Response(url=req.url, status=503, headers={}, body=b"", request=req)

        result = await mw.process_response(req, resp)
        assert isinstance(result, Request)
        assert result.retries == 1

    async def test_retry_request_must_bypass_dedup(self):
        """重试请求若不带 dont_filter，会被去重器静默丢弃——重试将完全失效。"""
        mw = self._mw()
        req = Request("http://a.com/")
        resp = Response(url=req.url, status=500, headers={}, body=b"", request=req)

        result = await mw.process_response(req, resp)
        assert result.dont_filter is True

    async def test_gives_up_after_max_retries(self):
        mw = self._mw(max_retries=2)
        req = Request("http://a.com/", retries=2)
        resp = Response(url=req.url, status=500, headers={}, body=b"", request=req)

        result = await mw.process_response(req, resp)
        assert result is resp  # 放弃重试，原样返回响应

    async def test_non_retryable_status_passes_through(self):
        mw = self._mw()
        req = Request("http://a.com/")
        resp = Response(url=req.url, status=404, headers={}, body=b"", request=req)
        assert await mw.process_response(req, resp) is resp

    async def test_retryable_exception_triggers_retry(self):
        mw = self._mw()
        result = await mw.process_exception(Request("http://a.com/"), httpx.ConnectError("x"))
        assert isinstance(result, Request) and result.retries == 1

    async def test_non_retryable_exception_ignored(self):
        mw = self._mw()
        assert await mw.process_exception(Request("http://a.com/"), ValueError("x")) is None

    async def test_retry_after_seconds_respected(self):
        mw = self._mw(max_retries=3)
        req = Request("http://a.com/")
        resp = Response(url=req.url, status=429, headers={"retry-after": "0"}, request=req, body=b"")

        loop = asyncio.get_running_loop()
        start = loop.time()
        result = await mw.process_response(req, resp)
        # Retry-After: 0 应立即重试，而不是走指数退避
        assert loop.time() - start < 0.5
        assert isinstance(result, Request)

    def test_parse_retry_after_formats(self):
        assert RetryMiddleware._parse_retry_after("120") == 120.0
        assert RetryMiddleware._parse_retry_after(None) is None
        assert RetryMiddleware._parse_retry_after("garbage") is None
        # HTTP 日期格式：过去的时间应被压到 0 而非负数
        assert RetryMiddleware._parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0


class TestThrottleMiddleware:
    async def test_same_domain_is_spaced_out(self):
        mw = ThrottleMiddleware(delay=0.15, jitter=0)
        loop = asyncio.get_running_loop()
        start = loop.time()
        await mw.process_request(Request("http://a.com/1"))
        await mw.process_request(Request("http://a.com/2"))
        assert loop.time() - start >= 0.15

    async def test_different_domains_do_not_block_each_other(self):
        """跨域名必须并行——全局共享速率是常见的性能陷阱。"""
        mw = ThrottleMiddleware(delay=0.3, jitter=0)
        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(
            mw.process_request(Request("http://a.com/")),
            mw.process_request(Request("http://b.com/")),
            mw.process_request(Request("http://c.com/")),
        )
        # 三个不同域名的首次请求都无需等待
        assert loop.time() - start < 0.2

    async def test_zero_delay_is_free(self):
        mw = ThrottleMiddleware(delay=0)
        loop = asyncio.get_running_loop()
        start = loop.time()
        for i in range(5):
            await mw.process_request(Request(f"http://a.com/{i}"))
        assert loop.time() - start < 0.05

    async def test_crawl_delay_from_robots_takes_priority(self):
        mw = ThrottleMiddleware(delay=0, jitter=0)
        req1 = Request("http://a.com/1", meta={"_crawl_delay": 0.15})
        req2 = Request("http://a.com/2", meta={"_crawl_delay": 0.15})
        loop = asyncio.get_running_loop()
        start = loop.time()
        await mw.process_request(req1)
        await mw.process_request(req2)
        # 即便自身 delay=0，也应遵守站点声明的 Crawl-delay
        assert loop.time() - start >= 0.15


class TestUserAgentMiddleware:
    async def test_sets_user_agent(self):
        mw = UserAgentMiddleware("MyBot/1.0")
        req = Request("http://a.com/")
        await mw.process_request(req)
        assert req.headers["User-Agent"] == "MyBot/1.0"

    async def test_does_not_override_explicit_ua(self):
        mw = UserAgentMiddleware("MyBot/1.0")
        req = Request("http://a.com/", headers={"User-Agent": "Custom"})
        await mw.process_request(req)
        assert req.headers["User-Agent"] == "Custom"

    async def test_picks_from_pool(self):
        pool = ["UA-1", "UA-2", "UA-3"]
        mw = UserAgentMiddleware(pool)
        seen = set()
        for i in range(60):
            req = Request(f"http://a.com/{i}")
            await mw.process_request(req)
            seen.add(req.headers["User-Agent"])
        assert seen <= set(pool) and len(seen) > 1

    async def test_default_pool_used_when_none(self):
        mw = UserAgentMiddleware(None)
        req = Request("http://a.com/")
        await mw.process_request(req)
        assert "Mozilla/5.0" in req.headers["User-Agent"]

    def test_empty_pool_rejected(self):
        with pytest.raises(ValueError):
            UserAgentMiddleware([])


class TestRobotsMiddleware:
    @respx.mock
    async def test_disallowed_path_raises(self):
        respx.get("https://a.com/robots.txt").mock(
            return_value=httpx.Response(200, text="User-agent: *\nDisallow: /private/")
        )
        mw = RobotsMiddleware()
        await mw.open_spider(None)
        try:
            with pytest.raises(IgnoreRequest):
                await mw.process_request(Request("https://a.com/private/x"))
            # 未被禁止的路径照常放行
            assert await mw.process_request(Request("https://a.com/public/x")) is None
        finally:
            await mw.close_spider(None)

    @respx.mock
    async def test_missing_robots_allows_everything(self):
        respx.get("https://a.com/robots.txt").mock(return_value=httpx.Response(404))
        mw = RobotsMiddleware()
        await mw.open_spider(None)
        try:
            assert await mw.process_request(Request("https://a.com/anything")) is None
        finally:
            await mw.close_spider(None)

    @respx.mock
    async def test_network_failure_allows_everything(self):
        """robots.txt 取不到时不应让整站瘫痪。"""
        respx.get("https://a.com/robots.txt").mock(side_effect=httpx.ConnectError("x"))
        mw = RobotsMiddleware()
        await mw.open_spider(None)
        try:
            assert await mw.process_request(Request("https://a.com/x")) is None
        finally:
            await mw.close_spider(None)

    @respx.mock
    async def test_fetched_once_under_concurrency(self):
        """并发请求同一站点时，robots.txt 只应下载一次。"""
        route = respx.get("https://a.com/robots.txt").mock(
            return_value=httpx.Response(200, text="User-agent: *\nAllow: /")
        )
        mw = RobotsMiddleware()
        await mw.open_spider(None)
        try:
            await asyncio.gather(*[
                mw.process_request(Request(f"https://a.com/{i}")) for i in range(10)
            ])
            assert route.call_count == 1
        finally:
            await mw.close_spider(None)

    @respx.mock
    async def test_crawl_delay_written_to_meta(self):
        respx.get("https://a.com/robots.txt").mock(
            return_value=httpx.Response(200, text="User-agent: *\nCrawl-delay: 2.5")
        )
        mw = RobotsMiddleware()
        await mw.open_spider(None)
        try:
            req = Request("https://a.com/x")
            await mw.process_request(req)
            assert req.meta["_crawl_delay"] == 2.5
        finally:
            await mw.close_spider(None)


class TestCrawlDelayParsing:
    """标准库只认整数 Crawl-delay，这段自研解析补上了小数与分组规则。"""

    parse = staticmethod(RobotsMiddleware._extract_crawl_delay)

    def test_decimal_value(self):
        assert self.parse("User-agent: *\nCrawl-delay: 2.5", "*") == 2.5

    def test_integer_value(self):
        assert self.parse("User-agent: *\nCrawl-delay: 3", "*") == 3.0

    def test_absent_returns_none(self):
        assert self.parse("User-agent: *\nDisallow: /x", "*") is None

    def test_invalid_value_ignored(self):
        assert self.parse("User-agent: *\nCrawl-delay: soon", "*") is None

    def test_comments_stripped(self):
        assert self.parse("User-agent: *\nCrawl-delay: 1.5  # 慢一点", "*") == 1.5

    def test_specific_agent_wins_over_wildcard(self):
        text = (
            "User-agent: *\n"
            "Crawl-delay: 10\n"
            "\n"
            "User-agent: mybot\n"
            "Crawl-delay: 0.5\n"
        )
        assert self.parse(text, "mybot") == 0.5
        # 不匹配的 UA 落到通配组
        assert self.parse(text, "otherbot") == 10.0

    def test_grouped_agents_share_directives(self):
        """连续的 User-agent 行属于同一组，共享其后的指令。"""
        text = "User-agent: a\nUser-agent: b\nCrawl-delay: 4\n"
        assert self.parse(text, "b") == 4.0

    def test_delay_belongs_to_preceding_group_only(self):
        text = (
            "User-agent: slowbot\n"
            "Crawl-delay: 9\n"
            "User-agent: fastbot\n"
            "Disallow:\n"
        )
        assert self.parse(text, "fastbot") is None
        assert self.parse(text, "slowbot") == 9.0


class TestProxyMiddleware:
    async def test_assigns_proxy(self):
        mw = ProxyMiddleware(["http://p1:8080"])
        req = Request("http://a.com/")
        await mw.process_request(req)
        # 自动挑选的代理放在下划线键里，不会被序列化进队列
        assert req.meta["_proxy"] == "http://p1:8080"

    async def test_auto_proxy_credentials_never_serialized(self):
        """自动分配的代理常带账号密码，绝不能跟着请求写进队列存储。"""
        mw = ProxyMiddleware(["http://user:secret@p1:8080"])
        req = Request("http://a.com/")
        await mw.process_request(req)
        assert "secret" not in req.to_json()
        assert "_proxy" not in req.to_dict()["meta"]

    async def test_manual_proxy_respected(self):
        mw = ProxyMiddleware(["http://p1:8080"])
        req = Request("http://a.com/", meta={"proxy": "http://manual:1"})
        await mw.process_request(req)
        assert req.meta["proxy"] == "http://manual:1"

    async def test_failed_proxy_enters_cooldown(self):
        mw = ProxyMiddleware(["http://p1:8080", "http://p2:8080"], cooldown=30)
        req = Request("http://a.com/")
        await mw.process_request(req)
        failed = req.meta["_proxy"]

        await mw.process_exception(req, httpx.ConnectError("x"))

        # 后续请求应避开处于冷却中的那个代理
        for i in range(10):
            r = Request(f"http://a.com/{i}")
            await mw.process_request(r)
            assert r.meta.get("_proxy") != failed

    async def test_falls_back_to_direct_when_all_cooling(self):
        mw = ProxyMiddleware(["http://p1:8080"], cooldown=30, fallback_direct=True)
        req = Request("http://a.com/")
        await mw.process_request(req)
        await mw.process_exception(req, httpx.ConnectError("x"))

        nxt = Request("http://a.com/2")
        await mw.process_request(nxt)
        assert "_proxy" not in nxt.meta  # 退回直连而不是卡死

    def test_empty_pool_rejected(self):
        with pytest.raises(ValueError):
            ProxyMiddleware([])


class TestDefaultChain:
    def test_retry_is_first(self):
        """Retry 必须排在最前，异常侧逆序时它才会最后执行——
        这样 Proxy 才有机会先标记失效代理。"""
        chain = build_default_middlewares(Settings())
        assert isinstance(chain[0], RetryMiddleware)

    def test_throttle_is_last(self):
        chain = build_default_middlewares(Settings())
        assert isinstance(chain[-1], ThrottleMiddleware)

    def test_robots_included_by_default(self):
        chain = build_default_middlewares(Settings())
        assert any(isinstance(m, RobotsMiddleware) for m in chain)

    def test_robots_can_be_disabled(self):
        chain = build_default_middlewares(Settings(respect_robots=False))
        assert not any(isinstance(m, RobotsMiddleware) for m in chain)

    def test_proxy_only_when_configured(self):
        assert not any(isinstance(m, ProxyMiddleware) for m in build_default_middlewares(Settings()))
        chain = build_default_middlewares(Settings(proxies=["http://p:1"]))
        assert any(isinstance(m, ProxyMiddleware) for m in chain)

    def test_proxy_before_throttle(self):
        """Proxy 需排在 Throttle 之前，异常侧才能先于 Retry 拿到标记机会。"""
        chain = build_default_middlewares(Settings(proxies=["http://p:1"]))
        idx = {type(m).__name__: i for i, m in enumerate(chain)}
        assert idx["ProxyMiddleware"] < idx["ThrottleMiddleware"]


class TestRetryAfterIsBounded:
    """回归：Retry-After 完全由服务端控制，不设上限等于把 worker 的生死
    交给对方——一个 `Retry-After: 86400` 就能让它睡满一天。"""

    async def test_huge_retry_after_is_capped(self):
        mw = RetryMiddleware(max_retries=3, backoff_base=1.0, max_backoff=0.05)
        req = Request("https://evil.example/x")
        resp = Response(
            url=req.url, status=429,
            headers={"retry-after": "86400"}, body=b"", request=req,
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        result = await mw.process_response(req, resp)
        elapsed = loop.time() - started

        assert isinstance(result, Request)
        # 实际等待被夹到 max_backoff，而不是服务端要的 86400 秒
        assert elapsed < 1.0, elapsed

    async def test_reasonable_retry_after_still_respected(self):
        mw = RetryMiddleware(max_retries=3, max_backoff=60.0)
        assert mw._parse_retry_after("2") == 2.0


class TestResponseChainContract:
    """回归：process_response 忘了 return，None 会一路传到引擎被当成
    「请求已放弃」，页面就这么无声无息地少了一个。"""

    async def test_none_from_process_response_raises(self):
        class Forgetful(Middleware):
            async def process_response(self, request, response):
                pass   # 忘了 return

        class Dummy:
            async def start(self): ...
            async def close(self): ...
            async def fetch(self, request):
                return Response(
                    url=request.url, status=200, headers={}, body=b"", request=request
                )

        mgr = MiddlewareManager(Dummy(), [Forgetful()])
        with pytest.raises(TypeError, match="返回了 None"):
            await mgr.download(Request("https://x.com/"))


class TestThrottleDomainKey:
    """回归：直接用 netloc 当键，大小写和默认端口会各占一个桶，限速被绕过。"""

    def test_variants_share_one_bucket(self):
        keys = {
            domain_key("https://Example.com/a"),
            domain_key("https://example.com/b"),
            domain_key("https://EXAMPLE.com:443/c"),
        }
        assert len(keys) == 1

    def test_non_default_port_is_distinct(self):
        assert domain_key("https://example.com:8443/") != domain_key("https://example.com/")

    async def test_same_site_requests_are_serialized(self):
        mw = ThrottleMiddleware(delay=0.05, jitter=0)
        loop = asyncio.get_running_loop()
        started = loop.time()
        await asyncio.gather(*[
            mw.process_request(Request(f"https://Example.com/{i}")) for i in range(3)
        ])
        # 三个大小写不同的写法必须落进同一个桶，共走两次间隔
        assert loop.time() - started >= 0.09
