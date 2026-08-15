"""存储后端接口。

统一 open/write/close 三个方法，让 JSONL、SQLite、PostgreSQL、MongoDB 等
后端可以互换。注意 write() 接收的是**一批** item，不是单条——批量写入是
存储层性能的分水岭，逐条写数据库会让吞吐量下降一到两个数量级。

缓冲攒批的逻辑统一由 StoragePipeline 负责，后端只需老实地写入收到的这一批。
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BaseStorage(Protocol):
    async def open(self) -> None:
        """建立连接 / 打开文件 / 建表。"""
        ...

    async def write(self, rows: list[dict[str, Any]]) -> None:
        """写入一批记录。实现应保证可重入（重复运行不产生重复数据）。"""
        ...

    async def close(self) -> None:
        """刷盘并释放资源。必须可重复调用。"""
        ...
