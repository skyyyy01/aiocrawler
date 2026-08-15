"""管道接口与链执行器。

Item 依次流过每个管道；任一环节返回 None 或抛出 DropItem，该 item 即被丢弃，
不再进入后续管道。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

import structlog

from aiocrawler.exceptions import DropItem
from aiocrawler.models import Item

if TYPE_CHECKING:
    from aiocrawler.spider import BaseSpider

log = structlog.get_logger(__name__)


@runtime_checkable
class BasePipeline(Protocol):
    async def open_spider(self, spider: BaseSpider) -> None:
        ...

    async def process_item(self, item: Item, spider: BaseSpider) -> Item | None:
        """返回 item 继续传递，返回 None 表示丢弃。"""
        ...

    async def close_spider(self, spider: BaseSpider) -> None:
        ...


class PipelineManager:
    """按顺序串联多个管道。"""

    def __init__(self, pipelines: list[BasePipeline]) -> None:
        self._pipelines = pipelines

    async def open(self, spider: BaseSpider) -> None:
        for p in self._pipelines:
            if hasattr(p, "open_spider"):
                await p.open_spider(spider)

    async def process(self, item: Item, spider: BaseSpider) -> bool:
        """让 item 流过整条管道链。返回 True 表示被完整处理，False 表示中途丢弃。"""
        current: Item | None = item
        for p in self._pipelines:
            try:
                current = await p.process_item(current, spider)
            except DropItem as exc:
                log.debug("item_dropped", pipeline=type(p).__name__, reason=str(exc))
                return False
            if current is None:
                log.debug("item_dropped", pipeline=type(p).__name__, reason="返回 None")
                return False
        return True

    async def close(self, spider: BaseSpider) -> None:
        # 逆序关闭，且逐个捕获异常——否则前面的管道抛错会导致后面的
        # 缓冲区来不及 flush，造成数据丢失
        for p in reversed(self._pipelines):
            if not hasattr(p, "close_spider"):
                continue
            try:
                await p.close_spider(spider)
            except Exception:
                log.exception("pipeline_close_failed", pipeline=type(p).__name__)
