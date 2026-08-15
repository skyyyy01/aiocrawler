"""Redis 调度器的分布式语义。

契约测试（test_scheduler_contract.py）已覆盖三种实现的共同行为，这里只测
Redis 特有的部分：多节点共享队列时的可见性超时与回收边界。
"""

from __future__ import annotations

import asyncio
import os

import pytest

from aiocrawler.models import Request

REDIS_URL = os.getenv("AIOCRAWLER_TEST_REDIS")

pytestmark = pytest.mark.skipif(not REDIS_URL, reason="未设置 AIOCRAWLER_TEST_REDIS")


def make(prefix: str, **kw):
    from aiocrawler.scheduler.redis_backend import RedisScheduler

    return RedisScheduler(REDIS_URL, prefix=f"test:{prefix}", **kw)


async def opened(prefix: str, **kw):
    s = make(prefix, **kw)
    await s.open()
    return s


class TestVisibilityTimeout:
    async def test_new_node_does_not_steal_active_inflight(self):
        """回归测试：新节点加入不得抢走其他节点正在处理的请求。

        这正是首次分布式验证中出现 7 条重复抓取的根因——当时的恢复逻辑把所有
        inflight 一律当作崩溃残留回收，而它们其实归活跃节点所有。
        """
        node_a = await opened("steal", resume=False)
        try:
            await node_a.push(Request("http://a.com/1"))
            await node_a.push(Request("http://a.com/2"))
            taken = await node_a.pop()          # A 取走一条，正在处理，尚未 ack
            assert taken is not None
            assert await node_a.inflight() == 1

            # B 此刻加入（默认 visibility_timeout=300s，A 的条目远未超时）
            node_b = await opened("steal", resume=True)
            try:
                assert await node_b.size() == 1        # 只剩另一条，没被多塞回来
                assert await node_b.inflight() == 1    # A 的条目仍归 A

                got = await node_b.pop()
                assert got.url != taken.url            # B 拿到的不是 A 手里那条
                assert await node_b.pop() is None      # 队列已空
            finally:
                await node_b.close()
        finally:
            await node_a.close()

    async def test_stale_inflight_is_reclaimed(self):
        """持有者真的死了（条目滞留超时）时，请求必须能被回收重发。"""
        dead = await opened("stale", resume=False)
        try:
            await dead.push(Request("http://a.com/orphan"))
            await dead.pop()          # 取走后「崩溃」，永不 ack
            assert await dead.inflight() == 1
        finally:
            await dead.close()

        await asyncio.sleep(1.1)

        # 新节点用 1 秒的可见性超时接管，应能回收这条孤儿请求
        rescuer = await opened("stale", resume=True, visibility_timeout=1.0)
        try:
            assert await rescuer.size() == 1
            assert await rescuer.inflight() == 0
            assert (await rescuer.pop()).url == "http://a.com/orphan"
        finally:
            await rescuer.close()

    async def test_acked_request_never_reclaimed(self):
        s = await opened("acked", resume=False, visibility_timeout=0.0)
        try:
            await s.push(Request("http://a.com/done"))
            req = await s.pop()
            await s.ack(req)
            assert await s.inflight() == 0

            s2 = await opened("acked", resume=True, visibility_timeout=0.0)
            try:
                assert await s2.size() == 0   # 已确认的请求不会复活
            finally:
                await s2.close()
        finally:
            await s.close()


class TestSharedQueue:
    async def test_two_nodes_split_work_without_overlap(self):
        """两个调度器实例共享队列时，每条请求只应被派发给其中一个。"""
        a = await opened("split", resume=False)
        b = await opened("split", resume=True)
        try:
            for i in range(40):
                await a.push(Request(f"http://a.com/{i}"))

            got_a, got_b = [], []
            while True:
                ra = await a.pop()
                rb = await b.pop()
                if ra is None and rb is None:
                    break
                if ra is not None:
                    got_a.append(ra.url)
                    await a.ack(ra)
                if rb is not None:
                    got_b.append(rb.url)
                    await b.ack(rb)

            assert len(got_a) + len(got_b) == 40
            assert set(got_a) & set(got_b) == set()      # 零重叠
            assert len(got_a) > 0 and len(got_b) > 0     # 两边都分到了活
        finally:
            await a.close()
            await b.close()

    async def test_dedup_shared_across_nodes(self):
        a = await opened("dedup", resume=False)
        b = await opened("dedup", resume=True)
        try:
            assert await a.push(Request("http://a.com/x")) is True
            # 另一个节点看到同一 URL 时应识别为重复
            assert await b.push(Request("http://a.com/x")) is False
        finally:
            await a.close()
            await b.close()

    async def test_fresh_clears_shared_state(self):
        a = await opened("fresh", resume=False)
        await a.push(Request("http://a.com/1"))
        await a.close()

        b = await opened("fresh", resume=False)   # fresh 启动
        try:
            assert await b.size() == 0
            assert await b.count() == 0
        finally:
            await b.close()
