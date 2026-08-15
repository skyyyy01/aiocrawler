"""下载器接口。

引擎只依赖这个 Protocol。阶段 3 加入 BrowserDownloader 和 DownloaderRouter 时，
只要它们同样满足此接口，引擎无需任何改动。
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from aiocrawler.models import Request, Response


@runtime_checkable
class BaseDownloader(Protocol):
    async def start(self) -> None:
        """初始化资源（连接池、浏览器进程等）。"""
        ...

    async def fetch(self, request: Request) -> Response:
        """执行下载。网络层异常应直接抛出，由中间件链决定是否重试。"""
        ...

    async def close(self) -> None:
        """释放资源。必须可重复调用。"""
        ...
