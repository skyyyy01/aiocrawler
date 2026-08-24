"""基于 httpx 的 HTTP 下载器。

两个性能要点：

1. **复用 AsyncClient**：连接池挂在 client 实例上，每次请求新建 client 会让
   TCP/TLS 握手无法复用，吞吐量下降一个数量级。

2. **按代理分池**：httpx 的代理绑定在 client 上，无法逐请求切换。因此这里维护
   `{代理地址: client}` 的映射，直连用 None 作键。ProxyMiddleware 把选中的代理
   写进 request.meta，这里据此取对应的 client——每个代理各自保有连接池。

## 响应体上限

对端返回多大的 body 完全由它说了算，一次性读进内存就等于把爬虫的生死交到了
被抓站点手上——几个并发拉几 GB 就能把进程撑爆。因此这里流式读取并计数，
超过 max_response_bytes 即中断连接并抛错，交给中间件链按普通下载失败处理。
"""

from __future__ import annotations

import httpx

from aiocrawler.exceptions import ResponseTooLarge
from aiocrawler.models import Request, Response

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


class HttpDownloader:
    def __init__(
        self,
        *,
        timeout: float = 20.0,
        max_connections: int = 100,
        max_keepalive: int = 20,
        follow_redirects: bool = True,
        http2: bool = True,
        verify_ssl: bool = True,
        default_headers: dict[str, str] | None = None,
        proxy: str | None = None,
        max_response_bytes: int | None = 64 * 1024 * 1024,
    ) -> None:
        self._timeout = timeout
        self._limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive,
        )
        self._follow_redirects = follow_redirects
        self._http2 = http2
        self._verify_ssl = verify_ssl
        self._default_proxy = proxy
        self._max_bytes = max_response_bytes
        self._headers = {"User-Agent": DEFAULT_UA, **(default_headers or {})}
        # None 键代表直连
        self._clients: dict[str | None, httpx.AsyncClient] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._client_for(self._default_proxy)  # 预建默认 client

    def _client_for(self, proxy: str | None) -> httpx.AsyncClient:
        client = self._clients.get(proxy)
        if client is None:
            client = httpx.AsyncClient(
                timeout=self._timeout,
                limits=self._limits,
                follow_redirects=self._follow_redirects,
                http2=self._http2,
                verify=self._verify_ssl,
                headers=self._headers,
                proxy=proxy,
            )
            self._clients[proxy] = client
        return client

    async def fetch(self, request: Request) -> Response:
        if not self._started:
            raise RuntimeError("下载器未启动，请先 await start()")

        # meta["proxy"] 是 spider 手工指定的，meta["_proxy"] 是 ProxyMiddleware
        # 自动挑的（下划线开头，不会被序列化进队列，详见 middleware/proxy.py）
        proxy = request.meta.get("proxy") or request.meta.get("_proxy") or self._default_proxy
        client = self._client_for(proxy)

        async with client.stream(
            request.method,
            request.url,
            headers=request.headers or None,
            content=request.body,
            timeout=request.timeout if request.timeout is not None else self._timeout,
        ) as resp:
            body = await self._read_capped(resp, request.url)
            return Response(
                # 用 str(resp.url)：重定向后这里是最终地址，urljoin 才能算对相对链接
                url=str(resp.url),
                status=resp.status_code,
                headers=dict(resp.headers),
                body=body,
                request=request,
                # httpx 已综合 HTTP 头与 <meta charset> 推断编码，比硬编码 utf-8 可靠
                encoding=resp.encoding or "utf-8",
            )

    async def _read_capped(self, resp: httpx.Response, url: str) -> bytes:
        """读取响应体，超过上限即中断。"""
        if self._max_bytes is None:
            await resp.aread()
            return resp.content

        chunks: list[bytes] = []
        size = 0
        async for chunk in resp.aiter_bytes():
            size += len(chunk)
            if size > self._max_bytes:
                raise ResponseTooLarge(
                    f"响应体超过上限 {self._max_bytes} 字节，已中断：{url}"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    async def close(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
        self._started = False
