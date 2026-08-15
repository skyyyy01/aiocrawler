"""调度器：优先级、去重、FIFO 稳定性。"""

from __future__ import annotations

from aiocrawler.models import Request
from aiocrawler.scheduler.memory import MemoryDupeFilter, MemoryScheduler


class TestDupeFilter:
    async def test_first_sight_returns_false(self):
        f = MemoryDupeFilter()
        assert await f.seen("abc") is False

    async def test_second_sight_returns_true(self):
        f = MemoryDupeFilter()
        await f.seen("abc")
        assert await f.seen("abc") is True

    async def test_count_tracks_unique_only(self):
        f = MemoryDupeFilter()
        await f.seen("a")
        await f.seen("a")
        await f.seen("b")
        assert await f.count() == 2


class TestScheduler:
    async def test_pop_empty_returns_none(self):
        assert await MemoryScheduler().pop() is None

    async def test_duplicate_rejected(self):
        s = MemoryScheduler()
        assert await s.push(Request("http://a.com/")) is True
        assert await s.push(Request("http://a.com/")) is False
        assert await s.size() == 1

    async def test_dont_filter_bypasses_dedup(self):
        s = MemoryScheduler()
        await s.push(Request("http://a.com/", dont_filter=True))
        await s.push(Request("http://a.com/", dont_filter=True))
        assert await s.size() == 2

    async def test_higher_priority_pops_first(self):
        s = MemoryScheduler()
        await s.push(Request("http://a.com/low", priority=0))
        await s.push(Request("http://a.com/high", priority=10))
        await s.push(Request("http://a.com/mid", priority=5))
        order = [(await s.pop()).url for _ in range(3)]
        assert order == ["http://a.com/high", "http://a.com/mid", "http://a.com/low"]

    async def test_same_priority_is_fifo(self):
        """同优先级下保持插入顺序，让抓取行为可预测、易调试。"""
        s = MemoryScheduler()
        for i in range(5):
            await s.push(Request(f"http://a.com/{i}"))
        assert [(await s.pop()).url for _ in range(5)] == [f"http://a.com/{i}" for i in range(5)]

    async def test_equal_priority_never_compares_requests(self):
        """Request 未定义 __lt__，堆比较若触及它会 TypeError。

        自增序号作为第二排序键正是为了避免这一点，这里明确守住。
        """
        s = MemoryScheduler()
        for i in range(50):
            await s.push(Request(f"http://a.com/{i}", priority=1))
        for _ in range(50):
            assert await s.pop() is not None

    async def test_size_shrinks_on_pop(self):
        s = MemoryScheduler()
        await s.push(Request("http://a.com/"))
        await s.pop()
        assert await s.size() == 0
