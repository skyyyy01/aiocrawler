"""调度器契约测试：同一组断言跑在全部三种实现上。

这是检验 BaseScheduler 抽象是否真正成立的最直接方式——如果某个实现需要
特殊对待才能通过，说明抽象没做对。三种实现分别是：

    MemoryScheduler   内存，单进程
    SqliteScheduler   本地文件，支持断点续爬
    RedisScheduler    分布式，多进程/多机共享

Redis 未配置 AIOCRAWLER_TEST_REDIS 时自动跳过其对应的参数。
"""

from __future__ import annotations

import asyncio
import os

import pytest

from aiocrawler.models import Request
from aiocrawler.scheduler.memory import MemoryScheduler
from aiocrawler.scheduler.sqlite import SqliteScheduler

REDIS_URL = os.getenv("AIOCRAWLER_TEST_REDIS")


@pytest.fixture(
    params=[
        "memory",
        "sqlite",
        pytest.param(
            "redis",
            marks=pytest.mark.skipif(not REDIS_URL, reason="未设置 AIOCRAWLER_TEST_REDIS"),
        ),
    ]
)
async def scheduler(request, tmp_path):
    kind = request.param
    if kind == "memory":
        s = MemoryScheduler()
    elif kind == "sqlite":
        s = SqliteScheduler(tmp_path / "state.db")
    else:
        from aiocrawler.scheduler.redis_backend import RedisScheduler

        # 每个用例用独立前缀，避免相互干扰
        s = RedisScheduler(REDIS_URL, prefix=f"test:{request.node.name[:40]}", resume=False)

    await s.open()
    try:
        yield s
    finally:
        await s.close()


class TestSchedulerContract:
    async def test_empty_pop_returns_none(self, scheduler):
        assert await scheduler.pop() is None

    async def test_push_then_pop(self, scheduler):
        assert await scheduler.push(Request("http://a.com/")) is True
        got = await scheduler.pop()
        assert got is not None and got.url == "http://a.com/"

    async def test_request_fields_survive_roundtrip(self, scheduler):
        """所有实现都必须完整保留请求内容，包括 meta 与 renderer。"""
        await scheduler.push(
            Request(
                "http://a.com/x",
                callback="parse_detail",
                meta={"page": 7, "名称": "中文"},
                priority=2,
                renderer="browser",
                headers={"X-A": "1"},
            )
        )
        got = await scheduler.pop()
        assert got.callback == "parse_detail"
        assert got.meta["page"] == 7
        assert got.meta["名称"] == "中文"
        assert got.priority == 2
        assert got.renderer == "browser"
        assert got.headers == {"X-A": "1"}

    async def test_duplicate_filtered(self, scheduler):
        assert await scheduler.push(Request("http://a.com/")) is True
        assert await scheduler.push(Request("http://a.com/")) is False

    async def test_equivalent_urls_deduped(self, scheduler):
        """查询串顺序与跟踪参数不应影响去重判定。"""
        assert await scheduler.push(Request("http://a.com/p?b=2&a=1")) is True
        assert await scheduler.push(Request("http://a.com/p?a=1&b=2&utm_source=x")) is False

    async def test_dont_filter_allows_duplicates(self, scheduler):
        await scheduler.push(Request("http://a.com/", dont_filter=True))
        await scheduler.push(Request("http://a.com/", dont_filter=True))
        assert await scheduler.size() == 2

    async def test_priority_order(self, scheduler):
        await scheduler.push(Request("http://a.com/low", priority=0))
        await scheduler.push(Request("http://a.com/high", priority=10))
        await scheduler.push(Request("http://a.com/mid", priority=5))
        urls = [(await scheduler.pop()).url for _ in range(3)]
        assert urls == ["http://a.com/high", "http://a.com/mid", "http://a.com/low"]

    async def test_fifo_within_same_priority(self, scheduler):
        for i in range(6):
            await scheduler.push(Request(f"http://a.com/{i}"))
        urls = [(await scheduler.pop()).url for _ in range(6)]
        assert urls == [f"http://a.com/{i}" for i in range(6)]

    async def test_size_reflects_pending_only(self, scheduler):
        await scheduler.push(Request("http://a.com/1"))
        await scheduler.push(Request("http://a.com/2"))
        assert await scheduler.size() == 2
        await scheduler.pop()
        assert await scheduler.size() == 1

    async def test_ack_is_always_callable(self, scheduler):
        """内存实现的 ack 是空操作，但必须存在且可调用。"""
        await scheduler.push(Request("http://a.com/"))
        req = await scheduler.pop()
        await scheduler.ack(req)
        assert await scheduler.size() == 0

    async def test_concurrent_pop_no_duplicate_delivery(self, scheduler):
        """并发消费时同一请求不能被派发两次。"""
        for i in range(30):
            await scheduler.push(Request(f"http://a.com/{i}"))
        got = await asyncio.gather(*[scheduler.pop() for _ in range(30)])
        urls = [r.url for r in got if r is not None]
        assert len(urls) == 30 and len(set(urls)) == 30

    async def test_drain_to_empty(self, scheduler):
        for i in range(5):
            await scheduler.push(Request(f"http://a.com/{i}"))
        while (req := await scheduler.pop()) is not None:
            await scheduler.ack(req)
        assert await scheduler.size() == 0
        assert await scheduler.pop() is None
