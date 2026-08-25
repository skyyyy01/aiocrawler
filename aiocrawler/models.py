"""框架的三个核心数据结构：Request / Response / Item。

设计要点：Request 必须是**纯数据、可 JSON 序列化**的，因此 callback 存的是
方法名字符串而非函数引用。这样未来把队列换成 Redis 时，Request 可以直接
序列化进队列，引擎代码无需改动。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace as _dc_replace
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from pydantic import BaseModel
from selectolax.parser import HTMLParser, Node

Renderer = Literal["http", "browser"]

# 规范化 URL 时剥离的跟踪参数：它们不影响页面内容，保留会导致同一页面重复抓取
TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "msclkid", "spm", "ref", "referrer",
})


def canonicalize_url(url: str) -> str:
    """规范化 URL，用于计算去重指纹。

    做三件事：小写 scheme/host、排序 query 参数、剥离跟踪参数。
    目的是让 ?a=1&b=2 与 ?b=2&a=1&utm_source=x 得到同一个指纹。
    """
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if k.lower() not in TRACKING_PARAMS]
    query.sort()
    return urlunsplit((
        parts.scheme.lower(),
        parts.netloc.lower(),
        parts.path or "/",
        urlencode(query),
        "",  # 丢弃 fragment：#anchor 不会产生不同的响应
    ))


#: scheme 的默认端口，规范化域名时剥掉
_DEFAULT_PORTS = {"http": 80, "https": 443}


def domain_key(url: str) -> str:
    """把 URL 归一成「站点」标识，用于按域名限速与限并发。

    小写化，并剥掉与 scheme 对应的默认端口——`Example.com`、`example.com`、
    `example.com:443` 指向同一台服务器，必须落进同一个桶，否则限制会被绕开
    （每种写法各自占一份额度，对同一台机器的实际压力成倍上去）。
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    port = parts.port
    if port is not None and port != _DEFAULT_PORTS.get(parts.scheme.lower()):
        return f"{host}:{port}"
    return host


@dataclass(slots=True)
class Request:
    """一个待抓取的请求。

    callback 是 Spider 上的方法名（字符串），引擎用 getattr 取回对应方法。
    不要改成函数引用——那会让 Request 无法序列化，堵死分布式升级的路。
    """

    url: str
    callback: str = "parse"
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None
    # meta 用于在请求链路上透传上下文（如分页页码、父页面提取的字段）
    meta: dict[str, Any] = field(default_factory=dict)
    # 数值越大越优先。详情页通常给正数，让其先于列表页消费，避免队列无限膨胀
    priority: int = 0
    renderer: Renderer = "http"
    # 跳过去重（如登录页、需要重复访问的翻页接口）
    dont_filter: bool = False
    # 由 RetryMiddleware 维护，阶段 2 启用
    retries: int = 0
    timeout: float | None = None

    def fingerprint(self) -> str:
        """去重指纹：method + 规范化 URL + body 的 sha256。"""
        h = hashlib.sha256()
        h.update(self.method.upper().encode())
        h.update(b"\x00")
        h.update(canonicalize_url(self.url).encode())
        if self.body:
            h.update(b"\x00")
            h.update(self.body)
        return h.hexdigest()

    def replace(self, **changes: Any) -> Request:
        """派生一个修改过部分字段的新 Request（中间件常用）。

        meta 与 headers 会被复制一份。dataclasses.replace 只做浅拷贝，派生出的
        请求会和原请求共享同一个 dict——重试请求改一下 header，原请求跟着变；
        原请求被 ack 时从 meta 里摘掉记账字段，重试请求也跟着少一块。这类
        「改了 A 结果 B 变了」的问题在并发下极难排查。
        """
        changes.setdefault("meta", dict(self.meta))
        changes.setdefault("headers", dict(self.headers))
        return _dc_replace(self, **changes)

    # ---- 序列化：分布式调度器的接口基础 ----

    def public_meta(self) -> dict[str, Any]:
        """剔除框架内部记账字段后的 meta。

        下划线开头的键是各组件的运行期便签（调度器行号、Redis member、
        自动分配的代理……），只在本次处理内有效。把它们一起序列化进队列会带来
        实打实的麻烦：Redis 的 member 里存着整条 payload，跟着重试再序列化一次
        就层层嵌套，请求体每重试一轮膨胀一截；自动分配的代理则会把账号密码
        原样写进队列存储。
        """
        return {k: v for k, v in self.meta.items() if not k.startswith("_")}

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "callback": self.callback,
            "method": self.method,
            "headers": self.headers,
            "body": self.body.decode("latin-1") if self.body else None,
            "meta": self.public_meta(),
            "priority": self.priority,
            "renderer": self.renderer,
            "dont_filter": self.dont_filter,
            "retries": self.retries,
            "timeout": self.timeout,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Request:
        data = dict(data)
        if data.get("body") is not None:
            data["body"] = data["body"].encode("latin-1")
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> Request:
        return cls.from_dict(json.loads(raw))


@dataclass(slots=True)
class Response:
    """一次下载的结果，并附带解析便利方法。"""

    url: str  # 最终 URL（可能经过重定向），urljoin 以它为基准
    status: int
    headers: dict[str, str]
    body: bytes
    request: Request
    encoding: str = "utf-8"
    # 由 BrowserDownloader 填充，标识该响应经过了 JS 渲染
    rendered: bool = False

    _tree: HTMLParser | None = field(default=None, repr=False, compare=False)

    @property
    def text(self) -> str:
        return self.body.decode(self.encoding, errors="replace")

    @property
    def meta(self) -> dict[str, Any]:
        """透传自 request.meta，让 parse 里少写一层 .request。"""
        return self.request.meta

    @property
    def tree(self) -> HTMLParser:
        """惰性构建 DOM 树，只在真正解析时才付出代价。"""
        if self._tree is None:
            self._tree = HTMLParser(self.text)
        return self._tree

    def css(self, selector: str) -> list[Node]:
        return self.tree.css(selector)

    def css_first(self, selector: str) -> Node | None:
        return self.tree.css_first(selector)

    def text_of(self, selector: str, default: str = "") -> str:
        """取首个匹配节点的文本并 strip，取不到返回 default。高频操作，值得封装。"""
        node = self.css_first(selector)
        return node.text(strip=True) if node is not None else default

    def attr_of(self, selector: str, attr: str, default: str = "") -> str:
        """取首个匹配节点的属性值，取不到返回 default。"""
        node = self.css_first(selector)
        if node is None:
            return default
        return node.attributes.get(attr) or default

    def json(self) -> Any:
        return json.loads(self.body)

    def urljoin(self, href: str) -> str:
        return urljoin(self.url, href)

    def follow(self, href: str, callback: str = "parse", **kwargs: Any) -> Request:
        """基于当前页面构造下一个请求，自动补全相对链接。

        默认继承当前请求的 renderer——从一个需要 JS 渲染的页面跟进的链接，
        通常同样需要渲染。需要时可显式传 renderer 覆盖。
        """
        kwargs.setdefault("renderer", self.request.renderer)
        return Request(url=self.urljoin(href), callback=callback, **kwargs)


class Item(BaseModel):
    """所有抓取结果的基类。

    继承 pydantic BaseModel，字段校验由 ValidationPipeline 在管道里统一执行。
    """

    model_config = {"extra": "forbid", "str_strip_whitespace": True}
