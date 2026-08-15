"""抓取 books.toscrape.com 的全部图书（该站点专为爬虫练习搭建，共 1000 本）。

演示框架的典型用法：列表页翻页 + 详情页提取，两个回调分工。
"""

from __future__ import annotations

import re
from typing import AsyncIterator

from aiocrawler import BaseSpider, Item, Request, Response

RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}

_NUM_RE = re.compile(r"[\d.]+")
_STOCK_RE = re.compile(r"\((\d+)\s+available\)")


class BookItem(Item):
    url: str
    title: str
    price: float
    currency: str
    stock: int
    rating: int
    category: str
    upc: str
    reviews: int
    description: str


class BooksSpider(BaseSpider):
    """books.toscrape.com 全站图书"""

    name = "books"
    start_urls = ["https://books.toscrape.com/"]

    custom_settings = {
        # 练习站点，8 并发足够快且不至于造成压力
        "concurrency": 8,
        # 全局默认 1.0 秒对 1050 个页面就是 17 分钟。该站专为爬虫练习搭建，
        # 且无 robots.txt 限制，这里放宽到 0.1 秒（约 10 页/秒）。
        # 抓取生产站点时应保留更保守的默认值。
        "download_delay": 0.1,
        "output": "out/books.jsonl",
    }

    async def parse(self, response: Response) -> AsyncIterator[Item | Request]:
        """列表页：产出 20 个详情页请求，并跟进下一页。"""
        for link in response.css("article.product_pod h3 a"):
            href = link.attributes.get("href")
            if href:
                # priority=1 让详情页优先于列表页出队，
                # 否则 50 个列表页会先跑完，队列里堆积 1000 个详情请求
                yield response.follow(href, callback="parse_book", priority=1)

        next_link = response.css_first("li.next a")
        if next_link is not None:
            href = next_link.attributes.get("href")
            if href:
                yield response.follow(href, callback="parse")

    async def parse_book(self, response: Response) -> AsyncIterator[Item | Request]:
        """详情页：提取一本书的完整字段。"""
        info = self._product_table(response)
        price_text = response.text_of("p.price_color")

        yield BookItem(
            url=response.url,
            title=response.text_of("h1"),
            price=self._to_float(price_text),
            currency=price_text[:1] if price_text else "",
            stock=self._to_stock(response.text_of("p.availability")),
            rating=self._to_rating(response),
            category=self._category(response),
            upc=info.get("UPC", ""),
            reviews=int(info.get("Number of reviews", 0) or 0),
            description=self._description(response),
        )

    # ------------------------------------------------------------ 解析辅助

    @staticmethod
    def _to_float(text: str) -> float:
        """'£51.77' -> 51.77"""
        m = _NUM_RE.search(text)
        return float(m.group()) if m else 0.0

    @staticmethod
    def _to_stock(text: str) -> int:
        """'In stock (22 available)' -> 22"""
        m = _STOCK_RE.search(text)
        return int(m.group(1)) if m else 0

    @staticmethod
    def _to_rating(response: Response) -> int:
        """星级写在 class 里：<p class="star-rating Three">"""
        node = response.css_first("p.star-rating")
        if node is None:
            return 0
        for cls in (node.attributes.get("class") or "").split():
            if cls in RATING_MAP:
                return RATING_MAP[cls]
        return 0

    @staticmethod
    def _category(response: Response) -> str:
        """面包屑形如 [Home, Books, Poetry, 书名]，倒数第二项是分类。"""
        crumbs = response.css("ul.breadcrumb li")
        return crumbs[-2].text(strip=True) if len(crumbs) >= 2 else ""

    @staticmethod
    def _product_table(response: Response) -> dict[str, str]:
        """把 Product Information 表转成字典。"""
        result: dict[str, str] = {}
        for row in response.css("table.table-striped tr"):
            key, value = row.css_first("th"), row.css_first("td")
            if key is not None and value is not None:
                result[key.text(strip=True)] = value.text(strip=True)
        return result

    @staticmethod
    def _description(response: Response) -> str:
        """描述是 #product_description 之后的第一个 <p>。

        部分图书没有描述，此时该锚点不存在，返回空串。

        注意：该站点原始 HTML 里的描述本身就是「截断版 + 完整版」拼接的，
        提取结果中出现重复文本属于源数据特征，不是解析错误。
        """
        anchor = response.css_first("#product_description")
        if anchor is None:
            return ""
        node = anchor.next
        while node is not None and node.tag != "p":
            node = node.next
        return node.text(strip=True) if node is not None else ""
