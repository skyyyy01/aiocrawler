"""把 item 攒批后交给存储后端。

两个触发条件满足其一就 flush：
  1. 缓冲达到 batch_size（默认 200）
  2. 距上次 flush 超过 flush_interval 秒（默认 5s）

第二个条件不能省：慢站点可能几分钟才凑够一批，没有定时 flush 的话，
中途崩溃会丢掉这批数据，实时性也无从谈起。
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
            await self._storage.write(ready)
        return item

    async def _periodic_flush(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._flush_interval)
                await self._flush()
        except asyncio.CancelledError:
            pass

    async def _flush(self) -> None:
        async with self._lock:
            if not self._buffer:
                return
            ready, self._buffer = self._buffer, []
        await self._storage.write(ready)
        log.debug("storage_flushed", count=len(ready))

    async def close_spider(self, spider: BaseSpider) -> None:
        self._closed = True
        if self._ticker is not None:
            self._ticker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ticker
            self._ticker = None
        # 关闭前必须清空残留缓冲，否则最后不足一批的数据会丢失
        await self._flush()
        await self._storage.close()
