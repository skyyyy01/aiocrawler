"""把 item 攒批后交给存储后端。

两个触发条件满足其一就 flush：
  1. 缓冲达到 batch_size（默认 200）
  2. 距上次 flush 超过 flush_interval 秒（默认 5s）

第二个条件不能省：慢站点可能几分钟才凑够一批，没有定时 flush 的话，
中途崩溃会丢掉这批数据，实时性也无从谈起。

## 写入失败的处理

存储后端抖一下（数据库瞬断、磁盘满）不能让数据凭空消失，因此：

* 取出待写批次后若 write() 抛错，整批**放回缓冲头部**等下次重试，而不是
  连同异常一起丢掉；
* 写失败不向上抛。数据已经回到缓冲里等重试，把存储故障升级成抓取故障没有
  好处——引擎会把这一条记成「请求失败」，还会打断 parse() 里后续 item 的产出，
  把一次可恢复的抖动放大成真正的丢数据；
* 定时 flush 任务捕获所有异常继续运行——只 catch CancelledError 的话，
  一次瞬时故障就会让定时器永久停摆，之后只能靠攒满 batch_size 才排空；
* 收尾时无论前面发生什么，最终 flush 与 storage.close() 都必须执行。

积压有上限（batch_size × _BACKLOG_FACTOR）：后端长时间不可用时，丢弃最旧的
记录并明确告警，总比把内存吃光让整个进程崩掉要好。
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

import structlog

from aiocrawler.models import Item
from aiocrawler.storage.base import BaseStorage

if TYPE_CHECKING:
    from aiocrawler.spider import BaseSpider

log = structlog.get_logger(__name__)

#: 积压上限相对 batch_size 的倍数
_BACKLOG_FACTOR = 10


class StoragePipeline:
    def __init__(
        self,
        storage: BaseStorage,
        *,
        batch_size: int = 200,
        flush_interval: float = 5.0,
    ) -> None:
        self._storage = storage
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_backlog = batch_size * _BACKLOG_FACTOR
        self._buffer: list[dict[str, Any]] = []
        # 保护 _buffer：多个 worker 会并发调用 process_item
        self._lock = asyncio.Lock()
        self._ticker: asyncio.Task[None] | None = None
        self._closed = False

    async def open_spider(self, spider: BaseSpider) -> None:
        await self._storage.open()
        self._closed = False
        self._ticker = asyncio.create_task(self._periodic_flush())

    async def process_item(self, item: Item, spider: BaseSpider) -> Item:
        async with self._lock:
            self._buffer.append(item.model_dump(mode="json"))
            ready = self._buffer if len(self._buffer) >= self._batch_size else None
            if ready is not None:
                self._buffer = []
        # 在锁外写盘，避免 IO 期间阻塞其他 worker 投递 item
        if ready:
            await self._write(ready)
        return item

    async def _periodic_flush(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._flush_interval)
                # _flush 内部已经兜住写入失败；这里再兜一层，是为了保证
                # 任何意料之外的异常都不会让定时器退出——它一旦退出就再也
                # 不会回来，数据之后只能靠攒满 batch_size 才落盘
                try:
                    await self._flush()
                except Exception:
                    log.exception("periodic_flush_failed")
        except asyncio.CancelledError:
            pass

    async def _flush(self) -> bool:
        async with self._lock:
            if not self._buffer:
                return True
            ready, self._buffer = self._buffer, []
        if not await self._write(ready):
            return False
        log.debug("storage_flushed", count=len(ready))
        return True

    async def _write(self, rows: list[dict[str, Any]]) -> bool:
        """写入一批。失败则把数据放回缓冲等待重试，返回 False。"""
        try:
            await self._storage.write(rows)
        except Exception:
            await self._requeue(rows)
            return False
        return True

    async def _requeue(self, rows: list[dict[str, Any]]) -> None:
        async with self._lock:
            # 放回头部，保持原有先后顺序
            self._buffer = rows + self._buffer
            overflow = len(self._buffer) - self._max_backlog
            if overflow > 0:
                # 后端长期不可用。丢最旧的并大声报出来，总比 OOM 强
                del self._buffer[:overflow]
                log.error(
                    "storage_backlog_overflow",
                    dropped=overflow,
                    limit=self._max_backlog,
                    hint="存储后端持续写入失败，最旧的记录已被丢弃",
                )
        log.warning(
            "storage_write_failed",
            requeued=len(rows),
            backlog=len(self._buffer),
            exc_info=True,
        )

    async def close_spider(self, spider: BaseSpider) -> None:
        self._closed = True
        if self._ticker is not None:
            self._ticker.cancel()
            # 定时任务可能存着一个未取回的异常，这里必须吞掉：让它冒出来会
            # 直接跳过下面的收尾 flush，把整个残留缓冲赔进去
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await self._ticker
            self._ticker = None

        try:
            # 关闭前必须清空残留缓冲，否则最后不足一批的数据会丢失
            await self._flush()
        finally:
            # flush 失败也要关掉后端，否则连接/文件句柄会漏
            await self._storage.close()

        if self._buffer:
            # 走到这里说明数据是真的没了，必须显式报出来——否则爬虫会以
            # 「正常结束」的姿态收工，只是结果里静静地少了一块
            log.error(
                "storage_data_lost",
                count=len(self._buffer),
                hint="收尾 flush 未能写出这些记录，请检查存储后端",
            )
