"""测试共用组件。"""

from __future__ import annotations

from typing import Any

from aiocrawler.models import Item
from aiocrawler.spider import BaseSpider


class CollectPipeline:
    """把流经的 item 收集到内存列表，供断言使用。"""

    def __init__(self) -> None:
        self.items: list[Item] = []

    async def open_spider(self, spider: BaseSpider) -> None:
        self.items.clear()

    async def process_item(self, item: Item, spider: BaseSpider) -> Item:
        self.items.append(item)
        return item

    async def close_spider(self, spider: BaseSpider) -> None:
        pass


class MemoryStorage:
    """内存存储后端，用于验证批量写入行为。"""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.batches: list[int] = []   # 记录每批的大小，用来验证攒批逻辑
        self.opened = False
        self.closed = False

    async def open(self) -> None:
        self.opened = True

    async def write(self, rows: list[dict[str, Any]]) -> None:
        self.rows.extend(rows)
        self.batches.append(len(rows))

    async def close(self) -> None:
        self.closed = True
