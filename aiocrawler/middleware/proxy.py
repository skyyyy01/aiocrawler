"""代理池。

httpx 的代理是绑定在 AsyncClient 上的，无法逐请求切换，因此这里只把选中的
代理写入 `request.meta["proxy"]`，由 HttpDownloader 按代理维护多个 client。

代理失败后会进入冷却期，冷却期内不再被选中。全部代理都在冷却时退回直连，
而不是让抓取彻底停摆。
"""

from __future__ import annotations

import asyncio
import random

import structlog

from aiocrawler.middleware.base import Middleware
from aiocrawler.models import Request, Response

log = structlog.get_logger(__name__)


class ProxyMiddleware(Middleware):
    def __init__(
        self,
        proxies: list[str] | tuple[str, ...],
        *,
        cooldown: float = 60.0,
        fallback_direct: bool = True,
    ) -> None:
        """
        :param proxies: 形如 ["http://127.0.0.1:7890", "socks5://..."]
        :param cooldown: 代理失败后的冷却秒数
        :param fallback_direct: 全部代理都在冷却时是否退回直连
        """
        self._proxies = list(proxies)
        if not self._proxies:
            raise ValueError("proxies 不能为空")
        self._cooldown = cooldown
        self._fallback_direct = fallback_direct
        self._blocked_until: dict[str, float] = {}

    async def process_request(self, request: Request) -> None:
        # spider 手工指定的代理始终尊重；自动分配的则在重试时重新挑选
        if request.meta.get("proxy") and not request.meta.get("_proxy_auto"):
            return None

        chosen = self._pick()
        if chosen is None:
            request.meta.pop("proxy", None)
            request.meta.pop("_proxy_auto", None)
            return None

        request.meta["proxy"] = chosen
        request.meta["_proxy_auto"] = True
        return None

    async def process_exception(self, request: Request, exc: Exception) -> None:
        """代理相关的失败让该代理进入冷却，但不消化异常——交给 RetryMiddleware。"""
        proxy = request.meta.get("proxy")
        if proxy and request.meta.get("_proxy_auto"):
            self._block(proxy, reason=type(exc).__name__)
        return None

    async def process_response(self, request: Request, response: Response) -> Response:
        # 407 说明代理鉴权有问题，这个代理暂时不可用
        if response.status == 407:
            proxy = request.meta.get("proxy")
            if proxy:
                self._block(proxy, reason="HTTP 407")
        return response

    # ------------------------------------------------------------------

    def _pick(self) -> str | None:
        now = asyncio.get_running_loop().time()
        available = [p for p in self._proxies if self._blocked_until.get(p, 0.0) <= now]
        if available:
            return random.choice(available)

        if self._fallback_direct:
            log.warning("all_proxies_cooling_down", count=len(self._proxies), action="退回直连")
            return None
        # 不允许直连时，只能挑一个最早解禁的硬用
        return min(self._proxies, key=lambda p: self._blocked_until.get(p, 0.0))

    def _block(self, proxy: str, *, reason: str) -> None:
        try:
            now = asyncio.get_running_loop().time()
        except RuntimeError:
            return
        self._blocked_until[proxy] = now + self._cooldown
        log.debug("proxy_cooldown", proxy=proxy, reason=reason, seconds=self._cooldown)
