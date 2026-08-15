"""SQLite 持久化调度器与断点续爬。

核心要守住的性质：**取出但未确认的请求不能丢**。因为它的指纹已经写进去重表，
一旦丢失就再也不会被重新抓取，表现为没有任何报错的数据缺失。
"""

from __future__ import annotations

from aiocrawler.models import Request
from aiocrawler.scheduler.sqlite import SqliteScheduler


async def open_scheduler(path, *, resume: bool = True) -> SqliteScheduler:
    s = SqliteScheduler(path, resume=resume)
    await s.open()
    return s


class TestBasicQueue:
    async def test_push_pop_roundtrip(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            req = Request("http://a.com/", callback="parse_x", meta={"page": 2}, priority=3)
            assert await s.push(req) is True

            got = await s.pop()
            assert got.url == "http://a.com/"
            assert got.callback == "parse_x"
            assert got.meta["page"] == 2      # meta 完整往返
            assert got.priority == 3
        finally:
            await s.close()

    async def test_pop_empty_returns_none(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            assert await s.pop() is None
        finally:
            await s.close()

    async def test_duplicate_rejected(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            assert await s.push(Request("http://a.com/")) is True
            assert await s.push(Request("http://a.com/")) is False
            assert await s.size() == 1
        finally:
            await s.close()

    async def test_dont_filter_bypasses_dedup(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            await s.push(Request("http://a.com/", dont_filter=True))
            await s.push(Request("http://a.com/", dont_filter=True))
            assert await s.size() == 2
        finally:
            await s.close()

    async def test_priority_order(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            await s.push(Request("http://a.com/low", priority=0))
            await s.push(Request("http://a.com/high", priority=10))
            await s.push(Request("http://a.com/mid", priority=5))
            urls = [(await s.pop()).url for _ in range(3)]
            assert urls == ["http://a.com/high", "http://a.com/mid", "http://a.com/low"]
        finally:
            await s.close()

    async def test_same_priority_fifo(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            for i in range(5):
                await s.push(Request(f"http://a.com/{i}"))
            assert [(await s.pop()).url for _ in range(5)] == [
                f"http://a.com/{i}" for i in range(5)
            ]
        finally:
            await s.close()


class TestAckSemantics:
    async def test_pop_moves_to_inflight_not_deleted(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            await s.push(Request("http://a.com/"))
            await s.pop()
            assert await s.size() == 0        # 不在待处理里
            assert await s.inflight() == 1    # 但仍被记账
        finally:
            await s.close()

    async def test_ack_removes_permanently(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            await s.push(Request("http://a.com/"))
            req = await s.pop()
            await s.ack(req)
            assert await s.inflight() == 0
            assert await s.size() == 0
        finally:
            await s.close()

    async def test_ack_is_idempotent(self, tmp_path):
        s = await open_scheduler(tmp_path / "s.db")
        try:
            await s.push(Request("http://a.com/"))
            req = await s.pop()
            await s.ack(req)
            await s.ack(req)   # 重复 ack 不应报错
        finally:
            await s.close()

    async def test_concurrent_pops_get_distinct_requests(self, tmp_path):
        """pop 的取出与占位必须原子，否则并发 worker 会拿到同一条。"""
        import asyncio

        s = await open_scheduler(tmp_path / "s.db")
        try:
            for i in range(20):
                await s.push(Request(f"http://a.com/{i}"))
            got = await asyncio.gather(*[s.pop() for _ in range(20)])
            urls = [r.url for r in got if r is not None]
            assert len(urls) == 20
            assert len(set(urls)) == 20     # 无重复派发
        finally:
            await s.close()


class TestCrashRecovery:
    async def test_unacked_request_returns_to_pending(self, tmp_path):
        """本组最重要的一条：崩溃时正在处理的请求必须能重新入队。

        它的指纹已经记在去重表里，若不恢复就永远不会被再次抓取。
        """
        path = tmp_path / "s.db"
        s = await open_scheduler(path)
        await s.push(Request("http://a.com/lost"))
        await s.pop()                 # 取出但不 ack，模拟处理到一半崩溃
        await s.close()

        s2 = await open_scheduler(path)   # 重启
        try:
            assert await s2.size() == 1
            assert await s2.inflight() == 0
            assert (await s2.pop()).url == "http://a.com/lost"
        finally:
            await s2.close()

    async def test_pending_queue_survives_restart(self, tmp_path):
        path = tmp_path / "s.db"
        s = await open_scheduler(path)
        for i in range(5):
            await s.push(Request(f"http://a.com/{i}"))
        await s.close()

        s2 = await open_scheduler(path)
        try:
            assert await s2.size() == 5
        finally:
            await s2.close()

    async def test_fingerprints_survive_restart(self, tmp_path):
        """续爬时已抓过的 URL 不应重复抓取。"""
        path = tmp_path / "s.db"
        s = await open_scheduler(path)
        await s.push(Request("http://a.com/done"))
        req = await s.pop()
        await s.ack(req)
        await s.close()

        s2 = await open_scheduler(path)
        try:
            assert await s2.push(Request("http://a.com/done")) is False
            assert await s2.count() == 1
        finally:
            await s2.close()

    async def test_fresh_start_clears_everything(self, tmp_path):
        path = tmp_path / "s.db"
        s = await open_scheduler(path)
        await s.push(Request("http://a.com/1"))
        await s.push(Request("http://a.com/2"))
        await s.close()

        s2 = await open_scheduler(path, resume=False)
        try:
            assert await s2.size() == 0
            assert await s2.count() == 0
            # 清空后同一 URL 可以重新入队
            assert await s2.push(Request("http://a.com/1")) is True
        finally:
            await s2.close()
