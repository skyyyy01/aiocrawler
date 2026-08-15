"""aiocrawler —— 自研 Python 异步爬虫框架。

最小用法：

    from aiocrawler import BaseSpider, Item, Request

    class MySpider(BaseSpider):
        name = "my"
        start_urls = ["https://example.com"]

        async def parse(self, response):
            yield MyItem(title=response.text_of("h1"))
"""

from aiocrawler.engine import Engine, crawl
from aiocrawler.exceptions import DropItem, IgnoreRequest, NotConfigured
from aiocrawler.models import Item, Request, Response
from aiocrawler.settings import Settings
from aiocrawler.spider import BaseSpider

__version__ = "0.1.0"

__all__ = [
    "BaseSpider",
    "DropItem",
    "Engine",
    "IgnoreRequest",
    "Item",
    "NotConfigured",
    "Request",
    "Response",
    "Settings",
    "crawl",
]
