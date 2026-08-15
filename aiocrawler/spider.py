"""Spider 基类——使用者唯一需要接触的类。

写一个新爬虫只需三步：
  1. 继承 BaseSpider，起个 name
  2. 给出 start_urls（或重写 start_requests 做更复杂的初始请求）
  3. 实现 parse()，用 yield 产出 Item 或新的 Request
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Iterable

import structlog

from aiocrawler.models import Item, Request, Response


class BaseSpider:
    #: 爬虫标识，CLI 用它来定位（`aiocrawler run <name>`）
    name: str = ""

    #: 起始 URL 列表
    start_urls: list[str] = []

    #: 本爬虫默认的渲染方式。设为 "browser" 则整站走 Playwright 渲染；
    #: 也可以保持 "http"，只对个别请求显式指定 renderer="browser"。
    renderer: str = "http"

    #: 覆盖全局配置，仅对本爬虫生效
    custom_settings: dict[str, Any] = {}

    def __init__(self) -> None:
        if not self.name:
            raise ValueError(f"{type(self).__name__} 必须定义 name 属性")
        self.log = structlog.get_logger(self.name)

    def start_requests(self) -> Iterable[Request]:
        """生成初始请求。默认把 start_urls 交给 parse 处理。"""
        for url in self.start_urls:
            yield Request(url=url, callback="parse", renderer=self.renderer)

    async def parse(self, response: Response) -> AsyncIterator[Item | Request]:
        """解析响应，yield 出 Item（数据）或 Request（继续抓）。

        这是抽象方法，子类必须实现。注意它是 async generator——
        即使不产出任何东西，也要保证函数体内至少有一个 yield。
        """
        raise NotImplementedError(f"{type(self).__name__} 必须实现 parse()")
        yield  # pragma: no cover  —— 使本方法成为 async generator

    def get_callback(self, name: str):
        """按名字取回解析方法。

        Request 存的是方法名字符串而非函数引用（为了可序列化），
        引擎通过这里换回真正的可调用对象。
        """
        fn = getattr(self, name, None)
        if fn is None or not callable(fn):
            raise AttributeError(
                f"爬虫 {self.name} 上找不到回调方法 {name!r}，"
                f"请检查 Request(callback=...) 是否拼写正确"
            )
        return fn

    async def on_start(self) -> None:
        """爬虫启动时的钩子（建连接、读配置等）。"""

    async def on_close(self) -> None:
        """爬虫结束时的钩子。无论正常结束还是异常中断都会被调用。"""
