"""MongoDB 存储后端。

**这里用 PyMongo 内置的 AsyncMongoClient，而不是 motor。** Motor 已被 MongoDB
官方废弃，其异步能力自 PyMongo 4.13 起并入主库。网上大量教程仍在教 motor，
新项目不应再采用。

相比 SQL 后端，这里不需要推断 schema、也不需要把嵌套结构序列化成 JSON 字符串
——文档型数据库原生支持嵌套和不定字段，这正是它适合爬虫数据的原因。
"""

from __future__ import annotations

from typing import Any

import structlog

log = structlog.get_logger(__name__)


class MongoStorage:
    def __init__(
        self,
        uri: str,
        *,
        database: str,
        collection: str = "items",
        unique_key: str | list[str] | None = None,
    ) -> None:
        """
        :param uri: 形如 mongodb://localhost:27017
        :param unique_key: 唯一键字段（或字段列表）。给定后写入走 upsert，
                           重复运行爬虫不会产生重复文档。
        """
        self._uri = uri
        self._db_name = database
        self._coll_name = collection
        self._keys = [unique_key] if isinstance(unique_key, str) else list(unique_key or [])
        self._client: Any = None
        self._coll: Any = None

    async def open(self) -> None:
        from pymongo import AsyncMongoClient

        self._client = AsyncMongoClient(self._uri)
        self._coll = self._client[self._db_name][self._coll_name]

        if self._keys:
            await self._coll.create_index(
                [(k, 1) for k in self._keys], unique=True, name="ux_items"
            )
        log.debug(
            "mongo_ready",
            database=self._db_name,
            collection=self._coll_name,
            unique_key=self._keys or None,
        )

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows or self._coll is None:
            return

        if not self._keys:
            # ordered=False：单条失败不影响这一批的其余文档
            await self._coll.insert_many(rows, ordered=False)
            return

        from pymongo import ReplaceOne

        ops = [
            ReplaceOne({k: doc.get(k) for k in self._keys}, doc, upsert=True)
            for doc in rows
        ]
        await self._coll.bulk_write(ops, ordered=False)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._coll = None
