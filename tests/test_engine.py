"""引擎端到端行为，全部用 respx 拦截 HTTP，不触达真实网络。

这里一律传 `middlewares=[]`，把引擎从中间件链里剥离出来单独验证——
中间件自身的行为由 test_middleware.py 覆盖。

重点守住结束判定：既不能在还有在途请求时提前收工（丢数据），
也不能在队列真的空了之后挂死（卡住不退出）。
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
import respx

from aiocrawler import BaseSpider, Item, Response
from aiocrawler.engine import Engine
from aiocrawler.settings import Settings
from tests.conftest import CollectPipeline

BASE = "https://demo.test"

INDEX_HTML = """
<html><body>
  <a class="item" href="/p/1">one</a>
  <a class="item" href="/p/2">two</a>
  <a class="item" href="/p/3">three</a>
  <a class="next" href="/page/2">next</a>
</body></html>
"""

PAGE2_HTML = """
<html><body>
  <a class="item" href="/p/4">four</a>
</body></html>
"""


def detail_html(n: str) -> str:
    return f"<html><body><h1>标题 {n}</h1></body></html>"


class DemoItem(Item):
    url: str
    title: str


class DemoSpider(BaseSpider):
    name = "demo"
    start_urls = [f"{BASE}/"]
    # stats_interval 调大，避免测试期间输出进度日志
    custom_settings = {"concurrency": 4, "stats_interval": 3600.0}

    async def parse(self, response: Response):
        for a in response.css("a.item"):
            yield response.follow(a.attributes["href"], callback="parse_item", priority=1)
        nxt = response.css_first("a.next")
        if nxt is not None:
            yield response.follow(nxt.attributes["href"], callback="parse")

    async def parse_item(self, response: Response):
        yield DemoItem(url=response.url, title=response.text_of("h1"))


def mock_site() -> None:
    respx.get(f"{BASE}/").mock(return_value=httpx.Response(200, html=INDEX_HTML))
    respx.get(f"{BASE}/page/2").mock(return_value=httpx.Response(200, html=PAGE2_HTML))
    for i in range(1, 5):
        respx.get(f"{BASE}/p/{i}").mock(return_value=httpx.Response(200, html=detail_html(str(i))))


def build(spider=None, settings=None, pipelines=None) -> Engine:
    """构造一个不带任何中间件的引擎。"""
    return Engine(
        spider or DemoSpider(),
        settings,
        pipelines=pipelines or [],
        middlewares=[],
    )


@respx.mock
async def test_end_to_end_collects_every_item():
    mock_site()
    collector = CollectPipeline()
    stats = await build(pipelines=[collector]).run()

    # 4 个详情页全部抓到，一个都不能少——验证结束判定没有提前收工
    assert len(collector.items) == 4
    assert {i.title for i in collector.items} == {f"标题 {n}" for n in "1234"}
    assert stats.get("item/stored") == 4
    assert stats.get("request/failed") == 0
    # 2 个列表页 + 4 个详情页
    assert stats.get("response/received") == 6


@respx.mock
async def test_duplicate_requests_filtered():
    """同一 URL 重复产出时只应抓取一次。"""
    respx.get(f"{BASE}/").mock(return_value=httpx.Response(
        200, html='<a class="item" href="/p/1">a</a><a class="item" href="/p/1">a again</a>'
    ))
    respx.get(f"{BASE}/p/1").mock(return_value=httpx.Response(200, html=detail_html("1")))

    collector = CollectPipeline()
    stats = await build(pipelines=[collector]).run()

    assert len(collector.items) == 1
    assert stats.get("request/duplicated") == 1


@respx.mock
async def test_error_status_still_reaches_parse_without_middleware():
    """没有重试中间件时，500 是一个普通响应，照常交给回调处理。"""
    mock_site()
    respx.get(f"{BASE}/p/2").mock(return_value=httpx.Response(500))

    collector = CollectPipeline()
    stats = await build(pipelines=[collector]).run()

    # 页面无 h1，title 为空串，但其余 3 个页面不受影响
    assert len(collector.items) == 4
    assert stats.get("response/status/500") == 1


@respx.mock
async def test_network_error_is_isolated():
    """连接异常只影响单个请求，不应拖垮 worker。"""
    mock_site()
    respx.get(f"{BASE}/p/2").mock(side_effect=httpx.ConnectError("boom"))

    collector = CollectPipeline()
    stats = await build(pipelines=[collector]).run()

    assert len(collector.items) == 3
    assert stats.get("request/failed") == 1


@respx.mock
async def test_max_items_stops_early():
    mock_site()
    collector = CollectPipeline()
    settings = Settings(max_items=2, concurrency=1, stats_interval=3600.0)
    await build(settings=settings, pipelines=[collector]).run()

    # 并发为 1 时应精确停在上限
    assert len(collector.items) == 2


@respx.mock
async def test_terminates_when_no_start_requests():
    """没有种子请求时应立即干净退出，而不是空转挂死。"""

    class EmptySpider(DemoSpider):
        name = "empty"
        start_urls = []

    stats = await asyncio.wait_for(build(EmptySpider()).run(), timeout=5)
    assert stats.get("response/received") == 0


async def test_unknown_callback_raises_helpful_error():
    spider = DemoSpider()
    with pytest.raises(AttributeError, match="找不到回调方法"):
        spider.get_callback("no_such_method")


async def test_custom_settings_merged_into_engine():
    """spider.custom_settings 应覆盖全局默认值。"""
    engine = build()
    assert engine.settings.concurrency == 4      # 来自 custom_settings
    assert engine.settings.timeout == 20.0       # 未覆盖，保持默认
