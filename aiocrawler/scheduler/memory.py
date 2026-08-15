"""单机内存实现：heapq 优先级队列 + set 去重。

适用于单进程、队列规模在百万级以内的场景。超出这个规模，或需要多机协作时，
换用 RedisScheduler（阶段 6），接口完全一致。
"""

from __future__ import annotations

import heapq
import itertools

from aiocrawler.models import Request


class MemoryDupeFilter:
    """基于 set 的精确去重。

    内存占用约为 指纹数 × 100 字节（sha256 十六进制串 64 字符 + set 开销），
    一百万条约 100 MB。需要更省内存时换布隆过滤器（有极低误判率）。
    """

    __slots__ = ("_seen",)

    def __init__(self) -> None:
        self._seen: set[str] = set()

    async def seen(self, fingerprint: str) -> bool:
        # 用 len 变化判断是否为新元素，一次操作完成「检查并置位」。
        # 单线程 asyncio 下这里不会被抢占，天然原子。
        before = len(self._seen)
        self._seen.add(fingerprint)
        return len(self._seen) == before

    async def count(self) -> int:
        return len(self._seen)

    async def close(self) -> None:
        self._seen.clear()


class MemoryScheduler:
    """内存优先级队列。

    heapq 是小顶堆，而我们希望 priority 数值大的先出队，因此入堆时取负值。
    第二个排序键是自增序号，用于：
      1. 保证同优先级下的 FIFO 顺序（抓取顺序可预测，便于调试）；
      2. 避免堆比较时触及第三个元素 Request（dataclass 未定义 __lt__，会抛错）
    """

    __slots__ = ("_heap", "_counter", "_dupefilter")

    def __init__(self, dupefilter: MemoryDupeFilter | None = None) -> None:
        self._heap: list[tuple[int, int, Request]] = []
        self._counter = itertools.count()
        self._dupefilter = dupefilter if dupefilter is not None else MemoryDupeFilter()

    async def open(self) -> None:
        """内存队列无需初始化。"""

    async def push(self, request: Request) -> bool:
        if not request.dont_filter:
            if await self._dupefilter.seen(request.fingerprint()):
                return False
        heapq.heappush(self._heap, (-request.priority, next(self._counter), request))
        return True

    async def pop(self) -> Request | None:
        if not self._heap:
            return None
        return heapq.heappop(self._heap)[2]

    async def ack(self, request: Request) -> None:
        """内存队列没有崩溃恢复的诉求，无需记账。"""

    async def size(self) -> int:
        return len(self._heap)

    async def close(self) -> None:
        self._heap.clear()
        await self._dupefilter.close()
