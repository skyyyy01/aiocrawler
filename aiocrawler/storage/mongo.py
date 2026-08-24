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
            # 索引名带上键名：写死一个名字的话，改了 unique_key 再跑就会撞上
            # IndexOptionsConflict（同名索引、不同键），且报错跟 unique_key
            # 毫无字面关联，很难看出是怎么回事
            await self._coll.create_index(
                [(k, 1) for k in self._keys],
                unique=True,
                name="ux_" + "_".join(self._keys),
            )
        log.debug(
            "mongo_ready",
            database=self._db_name,
            collection=self._coll_name,
            unique_key=self._keys or None,
        )

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if self._coll is None:
            # 静默 return 会让 open() 失败这类问题在很久以后才暴露：
            # 爬虫一路跑完、日志一片正常，最后发现一条数据都没落地
            raise RuntimeError(
                f"{type(self).__name__} 未打开，请先 await open()"
            )

        if not self._keys:
            # ordered=False：单条失败不影响这一批的其余文档
            await self._coll.insert_many(rows, ordered=False)
            return

        from pymongo import ReplaceOne

        # filter 的取值来自抓取内容。若某个键的值是 dict，它会被 MongoDB 当作
        # 查询表达式解释（`{"$ne": null}` 之类），upsert 就可能命中并覆盖掉
        # 一条毫不相干的文档。唯一键只接受标量。
        ops = [ReplaceOne(self._key_filter(doc), doc, upsert=True) for doc in rows]
        await self._coll.bulk_write(ops, ordered=False)

    def _key_filter(self, doc: dict[str, Any]) -> dict[str, Any]:
        """用唯一键的取值构造 upsert 条件，拒绝非标量。"""
        keys: dict[str, Any] = {}
        for k in self._keys:
            value = doc.get(k)
            if isinstance(value, (dict, list)):
                raise ValueError(
                    f"唯一键 {k!r} 的取值是 {type(value).__name__}，"
                    "会被 MongoDB 当成查询表达式解释——请改用标量字段作唯一键"
                )
            keys[k] = value
        return keys

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._coll = None
