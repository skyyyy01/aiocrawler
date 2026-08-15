"""数据库存储后端。

SQLite 无需外部服务，可以完整验证建表、类型推断与 UPSERT 语义。
PostgreSQL / MySQL / MongoDB 的真实连通性验证见 test_storage_live.py。
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from aiocrawler.storage import storage_from_uri
from aiocrawler.storage._common import (
    ColumnType,
    infer_column_type,
    infer_schema,
    json_safe,
    normalize_rows,
)
from aiocrawler.storage.files import CsvStorage, JsonlStorage
from aiocrawler.storage.sqlite import SqliteStorage


class TestTypeInference:
    def test_bool_detected_before_int(self):
        """Python 里 bool 是 int 的子类，判断顺序错了会把 True 存成整数。"""
        assert infer_column_type(True) is ColumnType.BOOL
        assert infer_column_type(1) is ColumnType.INT

    def test_basic_types(self):
        assert infer_column_type(1.5) is ColumnType.FLOAT
        assert infer_column_type("x") is ColumnType.TEXT
        assert infer_column_type({"a": 1}) is ColumnType.JSON
        assert infer_column_type([1, 2]) is ColumnType.JSON
        assert infer_column_type(None) is None

    def test_widens_int_to_float(self):
        schema = infer_schema([{"v": 1}, {"v": 2.5}])
        assert schema["v"] is ColumnType.FLOAT

    def test_widens_to_text_on_mixed(self):
        schema = infer_schema([{"v": 1}, {"v": "abc"}])
        assert schema["v"] is ColumnType.TEXT

    def test_all_none_column_defaults_to_text(self):
        assert infer_schema([{"v": None}])["v"] is ColumnType.TEXT

    def test_column_order_preserved(self):
        schema = infer_schema([{"b": 1, "a": 2}, {"c": 3}])
        assert list(schema) == ["b", "a", "c"]

    def test_json_safe_serializes_containers(self):
        assert json.loads(json_safe({"a": 1})) == {"a": 1}
        assert json.loads(json_safe([1, 2])) == [1, 2]
        assert json_safe("plain") == "plain"
        assert json_safe(5) == 5

    def test_normalize_aligns_ragged_rows(self):
        """字段参差的记录必须先对齐，否则拼 SQL 时列数不匹配。"""
        out = normalize_rows([{"a": 1}, {"a": 2, "b": 3}], ["a", "b"])
        assert out == [{"a": 1, "b": None}, {"a": 2, "b": 3}]


class TestSqliteStorage:
    async def test_creates_table_and_writes(self, tmp_path):
        db = tmp_path / "t.db"
        s = SqliteStorage(db, table="books")
        await s.open()
        await s.write([{"title": "A", "price": 1.5}, {"title": "B", "price": 2.0}])
        await s.close()

        con = sqlite3.connect(db)
        rows = con.execute("SELECT title, price FROM books ORDER BY title").fetchall()
        con.close()
        assert rows == [("A", 1.5), ("B", 2.0)]

    async def test_inferred_column_types(self, tmp_path):
        db = tmp_path / "t.db"
        s = SqliteStorage(db)
        await s.open()
        await s.write([{"n": 1, "f": 1.5, "t": "x", "j": {"k": 1}}])
        await s.close()

        con = sqlite3.connect(db)
        types = {r[1]: r[2] for r in con.execute("PRAGMA table_info(items)")}
        con.close()
        assert types == {"n": "INTEGER", "f": "REAL", "t": "TEXT", "j": "TEXT"}

    async def test_nested_value_stored_as_json(self, tmp_path):
        db = tmp_path / "t.db"
        s = SqliteStorage(db)
        await s.open()
        await s.write([{"tags": ["a", "b"], "meta": {"x": 1}}])
        await s.close()

        con = sqlite3.connect(db)
        tags, meta = con.execute("SELECT tags, meta FROM items").fetchone()
        con.close()
        assert json.loads(tags) == ["a", "b"]
        assert json.loads(meta) == {"x": 1}

    async def test_upsert_replaces_existing_row(self, tmp_path):
        """同一唯一键重复写入应更新而非插入新行——增量抓取依赖这个性质。"""
        db = tmp_path / "t.db"
        s = SqliteStorage(db, unique_key="url")
        await s.open()
        await s.write([{"url": "u1", "price": 10.0}])
        await s.write([{"url": "u1", "price": 20.0}])
        await s.close()

        con = sqlite3.connect(db)
        rows = con.execute("SELECT url, price FROM items").fetchall()
        con.close()
        assert rows == [("u1", 20.0)]

    async def test_upsert_within_single_batch(self, tmp_path):
        db = tmp_path / "t.db"
        s = SqliteStorage(db, unique_key="url")
        await s.open()
        await s.write([{"url": "u1", "n": 1}, {"url": "u1", "n": 2}, {"url": "u2", "n": 3}])
        await s.close()

        con = sqlite3.connect(db)
        rows = dict(con.execute("SELECT url, n FROM items").fetchall())
        con.close()
        assert rows == {"u1": 2, "u2": 3}

    async def test_composite_unique_key(self, tmp_path):
        db = tmp_path / "t.db"
        s = SqliteStorage(db, unique_key=["site", "sku"])
        await s.open()
        await s.write([{"site": "a", "sku": "1", "v": 1}])
        await s.write([{"site": "a", "sku": "1", "v": 2}, {"site": "b", "sku": "1", "v": 3}])
        await s.close()

        con = sqlite3.connect(db)
        rows = con.execute("SELECT site, sku, v FROM items ORDER BY site").fetchall()
        con.close()
        assert rows == [("a", "1", 2), ("b", "1", 3)]

    async def test_explicit_schema_respected(self, tmp_path):
        db = tmp_path / "t.db"
        s = SqliteStorage(db, schema={"url": "TEXT", "n": "INTEGER"})
        await s.open()
        await s.write([{"url": "u", "n": 5, "ignored": "x"}])
        await s.close()

        con = sqlite3.connect(db)
        cols = [r[1] for r in con.execute("PRAGMA table_info(items)")]
        con.close()
        assert cols == ["url", "n"]   # 未声明的字段被丢弃

    async def test_ragged_rows_do_not_break_insert(self, tmp_path):
        db = tmp_path / "t.db"
        s = SqliteStorage(db)
        await s.open()
        await s.write([{"a": 1, "b": 2}, {"a": 3}])   # 第二条缺 b
        await s.close()

        con = sqlite3.connect(db)
        rows = con.execute("SELECT a, b FROM items ORDER BY a").fetchall()
        con.close()
        assert rows == [(1, 2), (3, None)]

    async def test_empty_write_is_noop(self, tmp_path):
        s = SqliteStorage(tmp_path / "t.db")
        await s.open()
        await s.write([])
        await s.close()   # 不应抛异常


class TestStorageFactory:
    def test_jsonl_by_default(self, tmp_path):
        assert isinstance(storage_from_uri(str(tmp_path / "x.jsonl")), JsonlStorage)
        assert isinstance(storage_from_uri(str(tmp_path / "noext")), JsonlStorage)

    def test_csv_by_suffix(self, tmp_path):
        assert isinstance(storage_from_uri(str(tmp_path / "x.csv")), CsvStorage)

    def test_sqlite_by_suffix(self, tmp_path):
        for name in ("x.db", "x.sqlite", "x.sqlite3"):
            assert isinstance(storage_from_uri(str(tmp_path / name)), SqliteStorage)

    def test_sqlite_by_scheme(self):
        assert isinstance(storage_from_uri("sqlite:///out/a.db"), SqliteStorage)

    def test_postgres_uri(self):
        from aiocrawler.storage.sql import PostgresStorage

        s = storage_from_uri("postgresql://u:p@h:5432/db", unique_key="url")
        assert isinstance(s, PostgresStorage)

    def test_mysql_uri(self):
        from aiocrawler.storage.sql import MysqlStorage

        assert isinstance(storage_from_uri("mysql://u:p@h:3306/db"), MysqlStorage)

    def test_mongo_uri_takes_db_from_path(self):
        from aiocrawler.storage.mongo import MongoStorage

        s = storage_from_uri("mongodb://localhost:27017/crawl")
        assert isinstance(s, MongoStorage)
        assert s._db_name == "crawl"

    def test_kwargs_forwarded(self, tmp_path):
        s = storage_from_uri(str(tmp_path / "x.db"), table="custom", unique_key="url")
        assert s._table == "custom" and s._keys == ["url"]


class TestMysqlDsnParsing:
    def test_parses_full_dsn(self):
        from aiocrawler.storage.sql import MysqlStorage

        cfg = MysqlStorage._parse_dsn("mysql://alice:s3cret@db.host:3307/shop")
        assert cfg == {
            "host": "db.host",
            "port": 3307,
            "user": "alice",
            "password": "s3cret",
            "db": "shop",
        }

    def test_defaults_and_url_encoding(self):
        from aiocrawler.storage.sql import MysqlStorage

        cfg = MysqlStorage._parse_dsn("mysql://root:p%40ss@localhost/test")
        assert cfg["port"] == 3306
        assert cfg["password"] == "p@ss"   # %40 应被还原成 @


class TestMysqlKeyTypeRule:
    def test_unique_key_forced_to_varchar(self):
        """MySQL 不允许在 TEXT 列上建唯一索引，唯一键必须是定长类型。"""
        from aiocrawler.storage.sql import MysqlStorage

        s = MysqlStorage("mysql://u@h/db", unique_key="url")
        defs = s._column_defs([{"url": "http://a", "title": "x"}])
        assert defs["url"] == "VARCHAR(255)"
        assert defs["title"] == "TEXT"

    def test_postgres_keeps_text_key(self):
        from aiocrawler.storage.sql import PostgresStorage

        s = PostgresStorage("postgresql://u@h/db", unique_key="url")
        defs = s._column_defs([{"url": "http://a"}])
        assert defs["url"] == "TEXT"   # PG 无此限制
