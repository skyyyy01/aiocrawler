"""抓取 quotes.toscrape.com/js —— 内容完全由 JavaScript 生成的页面。

这个站点是验证浏览器下载器的理想对照：它的 HTML 里没有任何名言内容，
只有一段 `var data = [...]` 的脚本，由前端 JS 渲染成 DOM。因此：

    纯 HTTP 抓取  → 0 条
    浏览器渲染后  → 100 条（10 页 × 10 条）

只要设 `renderer = "browser"`，框架就会把请求路由给 Playwright，
其余写法与普通爬虫完全一致。
"""

from __future__ import annotations

from typing import AsyncIterator

from aiocrawler import BaseSpider, Item, Request, Response


class QuoteItem(Item):
    text: str
    author: str
    tags: list[str]
    page_url: str


class QuotesJsSpider(BaseSpider):
    """quotes.toscrape.com JS 渲染版"""

    name = "quotes_js"
    start_urls = ["https://quotes.toscrape.com/js/"]

    # 整站走浏览器渲染；response.follow() 会自动继承这个设置
    renderer = "browser"

    custom_settings = {
        # 渲染比 HTTP 重得多，并发不宜高。这里与 browser_contexts 保持一致，
        # 避免 worker 数超过可用 context 数而在池上空等
        "concurrency": 4,
        "browser_contexts": 4,
        "download_delay": 0.2,
        # 等到名言节点真正出现再取 DOM，避免拿到 JS 执行前的空壳
        "browser_wait_until": "domcontentloaded",
        "output": "out/quotes_js.jsonl",
    }

    async def parse(self, response: Response) -> AsyncIterator[Item | Request]:
        for node in response.css("div.quote"):
            text = node.css_first("span.text")
            author = node.css_first("small.author")
            yield QuoteItem(
                text=text.text(strip=True) if text is not None else "",
                author=author.text(strip=True) if author is not None else "",
                tags=[a.text(strip=True) for a in node.css("div.tags a.tag")],
                page_url=response.url,
            )

        next_link = response.css_first("li.next a")
        if next_link is not None:
            href = next_link.attributes.get("href")
            if href:
                # renderer 自动继承为 "browser"
                yield response.follow(href, callback="parse")
