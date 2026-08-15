"""集成测试：跑在真实的 TCP + HTTP 协议栈上，不用 respx 打桩。

单元测试验证的是各组件的逻辑，这里验证它们组装起来之后在真实 socket 上
确实能协同工作——尤其是重试，它牵涉引擎、中间件链、下载器连接池三方。

服务器用 asyncio.start_server 手写，不引入外部依赖，也不触达公网。
"""

from __future__ import annotations

import asyncio
from collections import Counter

import pytest

from aiocrawler import BaseSpider, Item, Response
from aiocrawler.engine import Engine
from aiocrawler.middleware.retry import RetryMiddleware
from aiocrawler.middleware.throttle import ThrottleMiddleware
from aiocrawler.settings import Settings
from tests.conftest import CollectPipeline


class FlakyServer:
    """一个可编程的最小 HTTP 服务器。

    /flaky  前 fail_times 次返回 503，之后返回 200
    /ok     总是 200
    /gone   总是 404
    """

    def __init__(self, fail_times: int = 2) -> None:
        self.fail_times = fail_times
        self.hits: Counter[str] = Counter()
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError):
            writer.close()
            return

        path = head.split(b" ")[1].decode()
        self.hits[path] += 1

        if path.startswith("/flaky"):
            status, body = (
                (503, b"<h1>busy</h1>")
                if self.hits[path] <= self.fail_times
                else (200, b"<html><h1>recovered</h1></html>")
            )
        elif path.startswith("/gone"):
            status, body = 404, b"<h1>missing</h1>"
        else:
            status, body = 200, b"<html><h1>ok</h1></html>"

        writer.write(
            f"HTTP/1.1 {status} S\r\n"
            f"Content-Type: text/html; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n".encode() + body
        )
        await writer.drain()
        writer.close()


@pytest.fixture
async def server():
    srv = FlakyServer()
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()


class PageItem(Item):
    url: str
    title: str


def make_spider(name: str, urls: list[str]) -> BaseSpider:
    class _Spider(BaseSpider):
        pass

    _Spider.name = name
    _Spider.start_urls = urls
    _Spider.custom_settings = {"concurrency": 2, "stats_interval": 3600.0}

    async def parse(self, response: Response):
        yield PageItem(url=response.url, title=response.text_of("h1"))

    _Spider.parse = parse
    return _Spider()


def fast_chain() -> list:
    """只保留重试与零延迟限速，去掉 robots（本地服务器没有 robots.txt）。"""
    return [
        RetryMiddleware(max_retries=3, backoff_base=0.01),
        ThrottleMiddleware(delay=0),
    ]


async def test_retry_recovers_from_transient_failure(server):
    """503 两次后转 200：应当重试成功，最终拿到恢复后的内容。"""
    collector = CollectPipeline()
    engine = Engine(
        make_spider("flaky", [f"{server.base}/flaky"]),
        Settings(stats_interval=3600.0),
        pipelines=[collector],
        middlewares=fast_chain(),
    )
    stats = await asyncio.wait_for(engine.run(), timeout=30)

    assert len(collector.items) == 1
    assert collector.items[0].title == "recovered"
    # 1 次原始请求 + 2 次重试
    assert server.hits["/flaky"] == 3
    assert stats.get("request/retried") == 2


async def test_retry_gives_up_and_yields_last_response(server):
    """失败次数超过上限时应放弃重试，把最后一个响应交给回调，而不是无限重试。"""
    server.fail_times = 99
    collector = CollectPipeline()
    engine = Engine(
        make_spider("always_fail", [f"{server.base}/flaky"]),
        Settings(stats_interval=3600.0),
        pipelines=[collector],
        middlewares=[RetryMiddleware(max_retries=2, backoff_base=0.01), ThrottleMiddleware(delay=0)],
    )
    stats = await asyncio.wait_for(engine.run(), timeout=30)

    # 1 次原始 + 2 次重试后放弃
    assert server.hits["/flaky"] == 3
    assert stats.get("request/retried") == 2
    # 放弃后最后那个 503 响应仍会进入回调
    assert len(collector.items) == 1
    assert collector.items[0].title == "busy"


async def test_non_retryable_status_not_retried(server):
    """404 不在重试列表里，只应请求一次。"""
    collector = CollectPipeline()
    engine = Engine(
        make_spider("gone", [f"{server.base}/gone"]),
        Settings(stats_interval=3600.0),
        pipelines=[collector],
        middlewares=fast_chain(),
    )
    stats = await asyncio.wait_for(engine.run(), timeout=30)

    assert server.hits["/gone"] == 1
    assert stats.get("request/retried") == 0


async def test_multiple_pages_over_real_socket(server):
    """多个页面并发抓取，验证连接池复用下不串数据。"""
    urls = [f"{server.base}/ok/{i}" for i in range(12)]
    collector = CollectPipeline()
    engine = Engine(
        make_spider("many", urls),
        Settings(concurrency=4, stats_interval=3600.0),
        pipelines=[collector],
        middlewares=fast_chain(),
    )
    await asyncio.wait_for(engine.run(), timeout=30)

    assert len(collector.items) == 12
    assert {i.url for i in collector.items} == set(urls)
    assert all(i.title == "ok" for i in collector.items)


async def test_throttle_applies_over_real_requests(server):
    """限速在真实请求上生效：4 个请求、间隔 0.1s，总耗时应不少于 3 个间隔。"""
    urls = [f"{server.base}/ok/{i}" for i in range(4)]
    engine = Engine(
        make_spider("throttled", urls),
        Settings(concurrency=4, stats_interval=3600.0),
        pipelines=[],
        middlewares=[ThrottleMiddleware(delay=0.1, jitter=0)],
    )
    loop = asyncio.get_running_loop()
    start = loop.time()
    await asyncio.wait_for(engine.run(), timeout=30)
    assert loop.time() - start >= 0.3
