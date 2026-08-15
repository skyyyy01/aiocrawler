"""针对真实数据库服务的连通性与语义验证。

DSN 通过环境变量提供，未设置的后端自动跳过——没有数据库的环境同样能跑完
整个测试套件：

    AIOCRAWLER_TEST_PG=postgresql://postgres:testpass@127.0.0.1:15432/crawl
    AIOCRAWLER_TEST_MYSQL=mysql://root:testpass@127.0.0.1:13306/crawl
    AIOCRAWLER_TEST_MONGO=mongodb://127.0.0.1:27018

每个用例使用独立的表名/集合名并在结束时清理，可重复运行。
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

PG_DSN = os.getenv("AIOCRAWLER_TEST_PG")
MYSQL_DSN = os.getenv("AIOCRAWLER_TEST_MYSQL")
MONGO_URI = os.getenv("AIOCRAWLER_TEST_MONGO")

pytestmark = pytest.mark.live


def unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


SAMPLE = [
    {"url": "https://a.com/1", "title": "第一本", "price": 10.5, "tags": ["x", "y"], "sold": True},
    {"url": "https://a.com/2", "title": "第二本", "price": 20.0, "tags": [], "sold": False},
]


# ---------------------------------------------------------------- PostgreSQL

@pytest.mark.skipif(not PG_DSN, reason="未设置 AIOCRAWLER_TEST_PG")
class TestPostgresLive:
    async def _fetch_all(self, table: str):
        import asyncpg

        conn = await asyncpg.connect(PG_DSN)
        try:
            return await conn.fetch(f'SELECT * FROM "{table}" ORDER BY url')
        finally:
            await conn.close()

    async def _drop(self, table: str):
        import asyncpg

        conn = await asyncpg.connect(PG_DSN)
        try:
            await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        finally:
            await conn.close()

    async def test_create_write_and_types(self):
        from aiocrawler.storage.sql import PostgresStorage

        table = unique_name("pg_items")
        s = PostgresStorage(PG_DSN, table=table)
        await s.open()
        try:
            await s.write(SAMPLE)
        finally:
            await s.close()

        try:
            rows = await self._fetch_all(table)
            assert len(rows) == 2
            assert rows[0]["title"] == "第一本"
            assert rows[0]["price"] == 10.5
            assert rows[0]["sold"] is True          # BOOLEAN 而非 0/1
            assert json.loads(rows[0]["tags"]) == ["x", "y"]   # JSONB
        finally:
            await self._drop(table)

    async def test_upsert(self):
        from aiocrawler.storage.sql import PostgresStorage

        table = unique_name("pg_upsert")
        s = PostgresStorage(PG_DSN, table=table, unique_key="url")
        await s.open()
        try:
            await s.write([{"url": "u1", "price": 1.0}])
            await s.write([{"url": "u1", "price": 9.9}, {"url": "u2", "price": 2.0}])
        finally:
            await s.close()

        try:
            rows = await self._fetch_all(table)
            assert len(rows) == 2
            assert {r["url"]: r["price"] for r in rows} == {"u1": 9.9, "u2": 2.0}
        finally:
            await self._drop(table)

    async def test_ragged_rows(self):
        from aiocrawler.storage.sql import PostgresStorage

        table = unique_name("pg_ragged")
        s = PostgresStorage(PG_DSN, table=table)
        await s.open()
        try:
            await s.write([{"url": "u1", "title": "有标题"}, {"url": "u2"}])
        finally:
            await s.close()

        try:
            rows = await self._fetch_all(table)
            assert rows[1]["title"] is None
        finally:
            await self._drop(table)


# ---------------------------------------------------------------------- MySQL

@pytest.mark.skipif(not MYSQL_DSN, reason="未设置 AIOCRAWLER_TEST_MYSQL")
class TestMysqlLive:
    async def _query(self, sql: str):
        import aiomysql

        from aiocrawler.storage.sql import MysqlStorage

        cfg = MysqlStorage._parse_dsn(MYSQL_DSN)
        conn = await aiomysql.connect(charset="utf8mb4", **cfg)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()
        finally:
            conn.close()

    async def _drop(self, table: str):
        import aiomysql

        from aiocrawler.storage.sql import MysqlStorage

        cfg = MysqlStorage._parse_dsn(MYSQL_DSN)
        conn = await aiomysql.connect(charset="utf8mb4", autocommit=True, **cfg)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        finally:
            conn.close()

    async def test_create_write_and_types(self):
        from aiocrawler.storage.sql import MysqlStorage

        table = unique_name("my_items")
        s = MysqlStorage(MYSQL_DSN, table=table)
        await s.open()
        try:
            await s.write(SAMPLE)
        finally:
            await s.close()

        try:
            rows = await self._query(f"SELECT * FROM `{table}` ORDER BY url")
            assert len(rows) == 2
            assert rows[0]["title"] == "第一本"      # utf8mb4 中文无乱码
            assert float(rows[0]["price"]) == 10.5
            assert json.loads(rows[0]["tags"]) == ["x", "y"]
        finally:
            await self._drop(table)

    async def test_upsert_with_varchar_key(self):
        """唯一键被强制为 VARCHAR(255)，否则 MySQL 无法在 TEXT 上建唯一索引。"""
        from aiocrawler.storage.sql import MysqlStorage

        table = unique_name("my_upsert")
        s = MysqlStorage(MYSQL_DSN, table=table, unique_key="url")
        await s.open()
        try:
            await s.write([{"url": "u1", "price": 1.0}])
            await s.write([{"url": "u1", "price": 9.9}, {"url": "u2", "price": 2.0}])
        finally:
            await s.close()

        try:
            rows = await self._query(f"SELECT url, price FROM `{table}` ORDER BY url")
            assert {r["url"]: float(r["price"]) for r in rows} == {"u1": 9.9, "u2": 2.0}

            cols = await self._query(f"SHOW COLUMNS FROM `{table}` LIKE 'url'")
            assert "varchar(255)" in cols[0]["Type"].lower()
        finally:
            await self._drop(table)

    async def test_ragged_rows(self):
        from aiocrawler.storage.sql import MysqlStorage

        table = unique_name("my_ragged")
        s = MysqlStorage(MYSQL_DSN, table=table)
        await s.open()
        try:
            await s.write([{"url": "u1", "title": "有标题"}, {"url": "u2"}])
        finally:
            await s.close()

        try:
            rows = await self._query(f"SELECT url, title FROM `{table}` ORDER BY url")
            assert rows[1]["title"] is None
        finally:
            await self._drop(table)


# -------------------------------------------------------------------- MongoDB

@pytest.mark.skipif(not MONGO_URI, reason="未设置 AIOCRAWLER_TEST_MONGO")
class TestMongoLive:
    async def _docs(self, coll_name: str):
        from pymongo import AsyncMongoClient

        client = AsyncMongoClient(MONGO_URI)
        try:
            coll = client["crawl_test"][coll_name]
            return [d async for d in coll.find({}, {"_id": 0}).sort("url", 1)]
        finally:
            await client.close()

    async def _drop(self, coll_name: str):
        from pymongo import AsyncMongoClient

        client = AsyncMongoClient(MONGO_URI)
        try:
            await client["crawl_test"][coll_name].drop()
        finally:
            await client.close()

    async def test_insert_keeps_nested_structure(self):
        """文档库的价值所在：嵌套结构原样保留，不必序列化成字符串。"""
        from aiocrawler.storage.mongo import MongoStorage

        coll = unique_name("mg_items")
        s = MongoStorage(MONGO_URI, database="crawl_test", collection=coll)
        await s.open()
        try:
            await s.write(SAMPLE)
        finally:
            await s.close()

        try:
            docs = await self._docs(coll)
            assert len(docs) == 2
            assert docs[0]["title"] == "第一本"
            assert docs[0]["tags"] == ["x", "y"]    # 真正的数组，不是 JSON 字符串
            assert docs[0]["sold"] is True
        finally:
            await self._drop(coll)

    async def test_upsert(self):
        from aiocrawler.storage.mongo import MongoStorage

        coll = unique_name("mg_upsert")
        s = MongoStorage(MONGO_URI, database="crawl_test", collection=coll, unique_key="url")
        await s.open()
        try:
            await s.write([{"url": "u1", "price": 1.0}])
            await s.write([{"url": "u1", "price": 9.9}, {"url": "u2", "price": 2.0}])
        finally:
            await s.close()

        try:
            docs = await self._docs(coll)
            assert len(docs) == 2
            assert {d["url"]: d["price"] for d in docs} == {"u1": 9.9, "u2": 2.0}
        finally:
            await self._drop(coll)

    async def test_ragged_documents(self):
        from aiocrawler.storage.mongo import MongoStorage

        coll = unique_name("mg_ragged")
        s = MongoStorage(MONGO_URI, database="crawl_test", collection=coll)
        await s.open()
        try:
            await s.write([{"url": "u1", "extra": "只有这条有"}, {"url": "u2"}])
        finally:
            await s.close()

        try:
            docs = await self._docs(coll)
            # 文档库不要求字段齐整，缺失字段就是不存在
            assert "extra" in docs[0] and "extra" not in docs[1]
        finally:
            await self._drop(coll)
