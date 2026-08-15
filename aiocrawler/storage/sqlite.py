"""SQLite 存储后端。

单文件数据库，无需部署服务，却支持索引、去重查询和增量更新——
是爬虫落地的默认好选择，比 JSONL 多了可查询性，比 MySQL 少了运维成本。

指定 unique_key 后写入变为 UPSERT，重复运行爬虫不会产生重复行，
断点续爬和增量更新都依赖这个性质。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog

from aiocrawler.storage._common import ColumnType, infer_schema, normalize_rows

log = structlog.get_logger(__name__)

SQLITE_TYPES = {
    ColumnType.BOOL: "INTEGER",
    ColumnType.INT: "INTEGER",
    ColumnType.FLOAT: "REAL",
    ColumnType.TEXT: "TEXT",
    ColumnType.JSON: "TEXT",
}


class SqliteStorage:
    def __init__(
        self,
        path: str | Path,
        *,
        table: str = "items",
        unique_key: str | list[str] | None = None,
        schema: dict[str, str] | None = None,
    ) -> None:
        """
        :param unique_key: 唯一键列名（或列名列表）。给定后写入走 UPSERT。
        :param schema: 显式列定义 {列名: SQL 类型}，不给则从首批数据推断。
        """
        self._path = Path(path)
        self._table = table
        self._keys = [unique_key] if isinstance(unique_key, str) else list(unique_key or [])
        self._schema = schema
        self._columns: list[str] = []
        self._conn: Any = None
        self._ready = False

    async def open(self) -> None:
        import aiosqlite

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        # WAL 让写入与读取互不阻塞，长时间抓取时可以另开连接查看进度
        await self._conn.execute("PRAGMA journal_mode=WAL")
        # 批量写入场景下放宽同步级别，速度提升明显，代价是断电可能丢最后几批
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.commit()

        if self._schema:
            await self._create_table({k: v for k, v in self._schema.items()})

    async def _create_table(self, columns: dict[str, str]) -> None:
        self._columns = list(columns)
        cols_sql = ", ".join(f'"{name}" {sql_type}' for name, sql_type in columns.items())
        await self._conn.execute(f'CREATE TABLE IF NOT EXISTS "{self._table}" ({cols_sql})')

        if self._keys:
            key_sql = ", ".join(f'"{k}"' for k in self._keys)
            # UPSERT 依赖唯一索引，没有它 ON CONFLICT 无从判断冲突
            await self._conn.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{self._table}" '
                f'ON "{self._table}" ({key_sql})'
            )
        await self._conn.commit()
        self._ready = True
        log.debug("sqlite_table_ready", table=self._table, columns=len(columns))

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows or self._conn is None:
            return

        if not self._ready:
            inferred = infer_schema(rows)
            await self._create_table({k: SQLITE_TYPES[v] for k, v in inferred.items()})

        prepared = normalize_rows(rows, self._columns)
        placeholders = ", ".join("?" for _ in self._columns)
        cols_sql = ", ".join(f'"{c}"' for c in self._columns)
        sql = f'INSERT INTO "{self._table}" ({cols_sql}) VALUES ({placeholders})'

        if self._keys:
            updates = ", ".join(
                f'"{c}"=excluded."{c}"' for c in self._columns if c not in self._keys
            )
            conflict = ", ".join(f'"{k}"' for k in self._keys)
            sql += (
                f" ON CONFLICT({conflict}) DO UPDATE SET {updates}"
                if updates
                else f" ON CONFLICT({conflict}) DO NOTHING"
            )

        await self._conn.executemany(sql, [tuple(r.values()) for r in prepared])
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
