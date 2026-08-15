"""调度器与去重器的接口定义。

**这是整个框架最重要的抽象边界。** 引擎只依赖这两个 Protocol，不依赖任何
具体实现。阶段 1 用内存实现，未来换成 Redis 实现时，引擎代码一行都不用改。

实现新调度器时请严格遵守 push() 的语义：它必须**原子地**完成「去重判断 +
入队」，返回值表示是否真正入队。把去重和入队拆成两步调用会在并发下产生竞态。
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from aiocrawler.models import Request


@runtime_checkable
class BaseDupeFilter(Protocol):
    """去重器：判断指纹是否已出现过。"""

    async def seen(self, fingerprint: str) -> bool:
        """检查并标记指纹。返回 True 表示**此前已见过**（应当丢弃）。

        必须是原子的「检查并置位」，不能拆成 contains + add。
        """
        ...

    async def count(self) -> int:
        """已记录的指纹数量。"""
        ...

    async def close(self) -> None:
        ...


@runtime_checkable
class BaseScheduler(Protocol):
    """调度器：管理待抓取请求队列。"""

    async def open(self) -> None:
        """初始化（建表、连接、恢复上次状态等）。内存实现可为空。"""
        ...

    async def push(self, request: Request) -> bool:
        """入队一个请求。返回 True 表示真正入队，False 表示被去重丢弃。"""
        ...

    async def pop(self) -> Request | None:
        """取出优先级最高的请求；队列为空时返回 None（不阻塞）。"""
        ...

    async def ack(self, request: Request) -> None:
        """确认一个请求已处理完毕。

        持久化/分布式调度器需要它来实现可靠队列：pop() 只是把请求标记为
        「处理中」，只有 ack() 之后才真正移除。这样进程崩溃时，处理到一半的
        请求能在重启后回到待处理状态，而不会因为指纹已记录而被永久跳过。

        纯内存实现不需要这个保证，空实现即可。
        """
        ...

    async def size(self) -> int:
        """当前待处理队列长度（不含处理中的请求）。"""
        ...

    async def close(self) -> None:
        ...
