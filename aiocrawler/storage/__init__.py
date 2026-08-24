"""存储后端集合，以及按 URI 自动选择后端的工厂。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog

from aiocrawler.storage.base import BaseStorage
from aiocrawler.storage.files import CsvStorage, JsonlStorage

log = structlog.get_logger(__name__)

__all__ = [
    "BaseStorage",
    "CsvStorage",
    "JsonlStorage",
    "storage_from_uri",
]


def storage_from_uri(uri: str, *, append: bool | None = None, **kwargs: Any) -> BaseStorage:
    """按 URI 或文件后缀挑选存储后端。

        out/books.jsonl                          → JsonlStorage
        out/books.csv                            → CsvStorage
        out/books.db  /  sqlite:///out/books.db  → SqliteStorage
        postgresql://user:pass@host/db           → PostgresStorage
        mysql://user:pass@host/db                → MysqlStorage
        mongodb://host:27017/dbname              → MongoStorage

    数据库后端的驱动是按需导入的，因此没装 asyncpg 也不影响使用 JSONL。

    :param append: 追加而非覆盖。仅对文件类后端有意义，数据库后端会忽略它
        （它们靠 unique_key 的 UPSERT 保证幂等）。断点续爬必须传 True，
        否则上一轮的结果会被清空，而指纹库又拦着不让重抓，数据就永久没了。
    """
    lowered = uri.lower()

    def file_backend(factory: Any) -> BaseStorage:
        """文件后端才接受 append 参数。"""
        if append is not None:
            kwargs.setdefault("append", append)
        return factory(uri, **kwargs)

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
        return file_backend(CsvStorage)
    if suffix in (".db", ".sqlite", ".sqlite3"):
        from aiocrawler.storage.sqlite import SqliteStorage

        return SqliteStorage(uri, **kwargs)
    # 兜底：任何认不出来的路径都按 JSONL 落地。注意这也意味着
    # `sqlit://`（少个 e）这类拼写错误不会报错，而是写出一个奇怪文件名——
    # 因此这里补一条日志，让拼错至少能被看见
    if "://" in uri:
        log.warning(
            "unrecognized_storage_uri",
            uri=uri,
            fallback="JsonlStorage",
            hint="URI scheme 无法识别，已按普通文件路径处理；请检查拼写",
        )
    return file_backend(JsonlStorage)
