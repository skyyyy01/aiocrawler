"""PostgreSQL 与 MySQL 存储后端。

两者的 UPSERT 语法和类型系统差异不小，因此分成两个类而不是硬套一层抽象：

    PostgreSQL   INSERT ... ON CONFLICT (key) DO UPDATE SET   占位符 $1, $2
    MySQL        INSERT ... ON DUPLICATE KEY UPDATE            占位符 %s

一个容易踩的坑：**MySQL 不允许在 TEXT 列上直接建唯一索引**（必须给前缀长度）。
因此这里把唯一键列强制声明为 VARCHAR(255)。PostgreSQL 无此限制。
"""

from __future__ import annotations

from typing import Any

import structlog

from aiocrawler.storage._common import ColumnType, infer_schema, normalize_rows

log = structlog.get_logger(__name__)


class _SqlStorageBase:
    """共用的 schema 推断与列对齐逻辑。"""

    TYPES: dict[ColumnType, str] = {}
    #: 作为唯一键时必须替换成的类型（MySQL 的 TEXT 不能建唯一索引）
    KEY_TYPE: str | None = None

    def __init__(
        self,
        dsn: str,
        *,
        table: str = "items",
        unique_key: str | list[str] | None = None,
        schema: dict[str, str] | None = None,
    ) -> None:
        self._dsn = dsn
        self._table = table
        self._keys = [unique_key] if isinstance(unique_key, str) else list(unique_key or [])
        self._schema = schema
        self._columns: list[str] = []
        self._ready = False

    def _column_defs(self, rows: list[dict[str, Any]]) -> dict[str, str]:
        if self._schema:
            return dict(self._schema)
        inferred = infer_schema(rows)
        defs = {name: self.TYPES[t] for name, t in inferred.items()}
        if self.KEY_TYPE:
            for key in self._keys:
                defs[key] = self.KEY_TYPE
        return defs

    def _prepare(self, rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
        return [tuple(r.values()) for r in normalize_rows(rows, self._columns)]


class PostgresStorage(_SqlStorageBase):
    """PostgreSQL 后端（asyncpg）。

    DSN 形如 postgresql://user:pass@host:5432/dbname
    """

    TYPES = {
        ColumnType.BOOL: "BOOLEAN",
        ColumnType.INT: "BIGINT",
        ColumnType.FLOAT: "DOUBLE PRECISION",
        ColumnType.TEXT: "TEXT",
        ColumnType.JSON: "JSONB",
    }

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 5, **kw: Any) -> None:
        super().__init__(dsn, **kw)
        self._min_size = min_size
        self._max_size = max_size
        self._pool: Any = None

    async def open(self) -> None:
        import asyncpg

        async def init_conn(conn: Any) -> None:
            # normalize_rows 已把嵌套结构转成 JSON 字符串，这里用恒等 codec
            # 让字符串能直接写入 JSONB 列，省去再解析回 dict 的开销
            await conn.set_type_codec(
                "jsonb",
                encoder=lambda v: v,
                decoder=lambda v: v,
                schema="pg_catalog",
                format="text",
            )

        self._pool = await asyncpg.create_pool(
            self._dsn, min_size=self._min_size, max_size=self._max_size, init=init_conn
        )

    async def _ensure_table(self, rows: list[dict[str, Any]]) -> None:
        defs = self._column_defs(rows)
        self._columns = list(defs)
        cols_sql = ", ".join(f'"{n}" {t}' for n, t in defs.items())
        async with self._pool.acquire() as conn:
            await conn.execute(f'CREATE TABLE IF NOT EXISTS "{self._table}" ({cols_sql})')
            if self._keys:
                keys = ", ".join(f'"{k}"' for k in self._keys)
                await conn.execute(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{self._table}" '
                    f'ON "{self._table}" ({keys})'
                )
        self._ready = True
        log.debug("postgres_table_ready", table=self._table, columns=len(defs))

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows or self._pool is None:
            return
        if not self._ready:
            await self._ensure_table(rows)

        cols_sql = ", ".join(f'"{c}"' for c in self._columns)
        placeholders = ", ".join(f"${i}" for i in range(1, len(self._columns) + 1))
        sql = f'INSERT INTO "{self._table}" ({cols_sql}) VALUES ({placeholders})'

        if self._keys:
            updates = ", ".join(
                f'"{c}"=EXCLUDED."{c}"' for c in self._columns if c not in self._keys
            )
            conflict = ", ".join(f'"{k}"' for k in self._keys)
            sql += (
                f" ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
                if updates
                else f" ON CONFLICT ({conflict}) DO NOTHING"
            )

        async with self._pool.acquire() as conn:
            await conn.executemany(sql, self._prepare(rows))

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None


class MysqlStorage(_SqlStorageBase):
    """MySQL / MariaDB 后端（aiomysql）。

    DSN 形如 mysql://user:pass@host:3306/dbname
    """

    TYPES = {
        ColumnType.BOOL: "TINYINT(1)",
        ColumnType.INT: "BIGINT",
        ColumnType.FLOAT: "DOUBLE",
        ColumnType.TEXT: "TEXT",
        ColumnType.JSON: "JSON",
    }
    # TEXT 列无法直接建唯一索引，唯一键一律用定长 VARCHAR
    KEY_TYPE = "VARCHAR(255)"

    def __init__(self, dsn: str, *, pool_size: int = 5, charset: str = "utf8mb4", **kw: Any) -> None:
        super().__init__(dsn, **kw)
        self._pool_size = pool_size
        self._charset = charset
        self._pool: Any = None

    @staticmethod
    def _parse_dsn(dsn: str) -> dict[str, Any]:
        from urllib.parse import unquote, urlsplit

        parts = urlsplit(dsn)
        return {
            "host": parts.hostname or "127.0.0.1",
            "port": parts.port or 3306,
            "user": unquote(parts.username) if parts.username else None,
            "password": unquote(parts.password) if parts.password else "",
            "db": parts.path.lstrip("/") or None,
        }

    async def open(self) -> None:
        import aiomysql

        cfg = self._parse_dsn(self._dsn)
        self._pool = await aiomysql.create_pool(
            minsize=1,
            maxsize=self._pool_size,
            charset=self._charset,
            autocommit=True,
            **cfg,
        )

    async def _ensure_table(self, rows: list[dict[str, Any]]) -> None:
        defs = self._column_defs(rows)
        self._columns = list(defs)
        cols_sql = ", ".join(f"`{n}` {t}" for n, t in defs.items())
        if self._keys:
            keys = ", ".join(f"`{k}`" for k in self._keys)
            cols_sql += f", UNIQUE KEY `ux_{self._table}` ({keys})"

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"CREATE TABLE IF NOT EXISTS `{self._table}` ({cols_sql}) "
                    f"ENGINE=InnoDB DEFAULT CHARSET={self._charset}"
                )
        self._ready = True
        log.debug("mysql_table_ready", table=self._table, columns=len(defs))

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows or self._pool is None:
            return
        if not self._ready:
            await self._ensure_table(rows)

        cols_sql = ", ".join(f"`{c}`" for c in self._columns)
        placeholders = ", ".join(["%s"] * len(self._columns))
        sql = f"INSERT INTO `{self._table}` ({cols_sql}) VALUES ({placeholders})"

        if self._keys:
            updates = ", ".join(
                f"`{c}`=VALUES(`{c}`)" for c in self._columns if c not in self._keys
            )
            if updates:
                sql += f" ON DUPLICATE KEY UPDATE {updates}"
            else:
                # 没有非键列可更新时，用自赋值实现「存在即跳过」
                sql += f" ON DUPLICATE KEY UPDATE `{self._keys[0]}`=`{self._keys[0]}`"

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, self._prepare(rows))

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
