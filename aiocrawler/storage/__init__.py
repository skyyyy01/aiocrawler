"""存储后端集合，以及按 URI 自动选择后端的工厂。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from aiocrawler.storage.base import BaseStorage
from aiocrawler.storage.files import CsvStorage, JsonlStorage

__all__ = [
    "BaseStorage",
    "CsvStorage",
    "JsonlStorage",
    "storage_from_uri",
]


def storage_from_uri(uri: str, **kwargs: Any) -> BaseStorage:
    """按 URI 或文件后缀挑选存储后端。

        out/books.jsonl                          → JsonlStorage
        out/books.csv                            → CsvStorage
        out/books.db  /  sqlite:///out/books.db  → SqliteStorage
        postgresql://user:pass@host/db           → PostgresStorage
        mysql://user:pass@host/db                → MysqlStorage
        mongodb://host:27017/dbname              → MongoStorage

    数据库后端的驱动是按需导入的，因此没装 asyncpg 也不影响使用 JSONL。
    """
    lowered = uri.lower()

    if lowered.startswith(("postgresql://", "postgres://")):
        from aiocrawler.storage.sql import PostgresStorage

        return PostgresStorage(uri, **kwargs)

    if lowered.startswith(("mysql://", "mariadb://")):
        from aiocrawler.storage.sql import MysqlStorage

        return MysqlStorage(uri, **kwargs)

    if lowered.startswith(("mongodb://", "mongodb+srv://")):
        from urllib.parse import urlsplit

        from aiocrawler.storage.mongo import MongoStorage

        # 数据库名取自 URI 路径，未给出时由调用方通过 database= 指定
        db = kwargs.pop("database", None) or urlsplit(uri).path.lstrip("/") or "aiocrawler"
        return MongoStorage(uri, database=db, **kwargs)

    if lowered.startswith("sqlite://"):
        from aiocrawler.storage.sqlite import SqliteStorage

        # sqlite:///relative/path.db 与 sqlite:////abs/path.db 都要能用
        path = uri[len("sqlite://") :]
        return SqliteStorage(path.lstrip("/") if not path.startswith("//") else path, **kwargs)

    suffix = Path(uri).suffix.lower()
    if suffix == ".csv":
        return CsvStorage(uri, **kwargs)
    if suffix in (".db", ".sqlite", ".sqlite3"):
        from aiocrawler.storage.sqlite import SqliteStorage

        return SqliteStorage(uri, **kwargs)
    return JsonlStorage(uri, **kwargs)
