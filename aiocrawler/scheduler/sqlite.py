"""SQLite 持久化调度器——断点续爬的实现。

## 为什么需要 ack

崩溃恢复的正确性全在这里。如果 pop() 直接把请求从队列删掉，那么进程在处理
它的过程中崩溃时，这个请求就永久消失了——而它的指纹已经记在去重表里，重启后
会被当成「已抓过」而跳过，最终表现为**静默丢数据**。

因此采用可靠队列：pop() 只把状态从 pending 改为 inflight，处理完成后由引擎
调用 ack() 才真正删除。启动时把所有残留的 inflight 重置回 pending，上次中断
时正在处理的请求就能自动重来。

## 与 Redis 调度器的关系

这套 pending/inflight/ack 的结构与 Redis 可靠队列完全同构，阶段 6 的
RedisScheduler 直接沿用同一套语义，接口无需任何改动。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog

from aiocrawler.models import Request

log = structlog.get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fingerprints (
    fp TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS queue (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    priority INTEGER NOT NULL DEFAULT 0,
    status   INTEGER NOT NULL DEFAULT 0,   -- 0=待处理 1=处理中
    payload  TEXT NOT NULL
);
-- pop() 的取数条件，没有这个索引队列一长就会全表扫描
CREATE INDEX IF NOT EXISTS ix_queue_pick ON queue (status, priority DESC, id ASC);
"""


class SqliteScheduler:
    """把队列与去重指纹落在 SQLite 里，支持中断后继续。"""

    def __init__(self, path: str | Path = "out/crawl_state.db", *, resume: bool = True) -> None:
        """
        :param resume: True 表示复用已有状态继续抓；False 表示清空重来。
        """
        self._path = Path(path)
        self._resume = resume
        self._conn: Any = None

    async def open(self) -> None:
        import aiosqlite

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.executescript(_SCHEMA)

        if not self._resume:
            await self._conn.execute("DELETE FROM queue")
            await self._conn.execute("DELETE FROM fingerprints")
            await self._conn.commit()
            log.info("scheduler_state_cleared", path=str(self._path))
            return

        # 上次中断时处于「处理中」的请求，重新放回待处理
        cur = await self._conn.execute("UPDATE queue SET status=0 WHERE status=1")
        requeued = cur.rowcount
        await self._conn.commit()

        pending = await self.size()
        seen = await self.count()
        if pending or seen:
            log.info(
                "scheduler_state_restored",
                pending=pending,
                requeued=requeued,
                known_fingerprints=seen,
            )

    # -------------------------------------------------------------- 去重

    async def seen(self, fingerprint: str) -> bool:
        cur = await self._conn.execute(
            "INSERT OR IGNORE INTO fingerprints (fp) VALUES (?)", (fingerprint,)
        )
        # rowcount==0 说明这条指纹已经存在，即此前见过
        return cur.rowcount == 0

    async def count(self) -> int:
        cur = await self._conn.execute("SELECT COUNT(*) FROM fingerprints")
        row = await cur.fetchone()
        return int(row[0])

    # -------------------------------------------------------------- 队列

    async def push(self, request: Request) -> bool:
        # 指纹登记与入队必须同进同退。sqlite3 在 DML 上隐式开启事务，若两条语句
        # 之间抛错而不回滚，那条已登记的指纹会被**下一次** push 的 commit 顺带
        # 提交——留下一个「已标记抓过但从未入队」的空洞，且毫无迹象
        try:
            if not request.dont_filter:
                if await self.seen(request.fingerprint()):
                    await self._conn.commit()
                    return False

            await self._conn.execute(
                "INSERT INTO queue (priority, status, payload) VALUES (?, 0, ?)",
                (request.priority, request.to_json()),
            )
        except BaseException:
            await self._conn.rollback()
            raise
        await self._conn.commit()
        return True

    async def pop(self) -> Request | None:
        # 取出并占位必须是一次原子操作，否则并发 worker 会拿到同一条
        cur = await self._conn.execute(
            "UPDATE queue SET status=1 "
            "WHERE id = (SELECT id FROM queue WHERE status=0 "
            "            ORDER BY priority DESC, id ASC LIMIT 1) "
            "RETURNING id, payload"
        )
        row = await cur.fetchone()
        await self._conn.commit()
        if row is None:
            return None

        request = Request.from_json(row[1])
        # 把队列行号记在 meta 里，ack 时据此删除
        request.meta["_queue_id"] = row[0]
        return request

    async def ack(self, request: Request) -> None:
        queue_id = request.meta.pop("_queue_id", None)
        if queue_id is None:
            return
        await self._conn.execute("DELETE FROM queue WHERE id=?", (queue_id,))
        await self._conn.commit()

    async def size(self) -> int:
        cur = await self._conn.execute("SELECT COUNT(*) FROM queue WHERE status=0")
        row = await cur.fetchone()
        return int(row[0])

    async def inflight(self) -> int:
        cur = await self._conn.execute("SELECT COUNT(*) FROM queue WHERE status=1")
        row = await cur.fetchone()
        return int(row[0])

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
