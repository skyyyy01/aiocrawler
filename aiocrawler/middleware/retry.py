"""失败重试：指数退避 + 随机抖动。

## 两个必须做对的细节

1. **重试请求必须带 dont_filter=True**。否则它的指纹与原请求相同，会被去重器
   当成重复请求直接丢弃——表现为「重试完全不生效」，且没有任何报错，极难排查。

2. **退避要加随机抖动**。大量请求同时失败（如目标站点短暂 503）时，若退避间隔
   完全一致，重试会在同一时刻再次涌向服务器，形成惊群。抖动把它们摊开。

429 / 503 若带 Retry-After 头，优先采用服务端给的时间——这是对方明确告知的
恢复时间，比我们自己算的退避更准确。但**必须夹在 max_backoff 以内**：这个值
完全由服务端控制，一个 `Retry-After: 86400` 就能让 worker 睡满一天。并发 N 的
爬虫被这么摆一道，整轮抓取就静默挂死了，日志上还看不出任何异常。
"""

from __future__ import annotations

import asyncio
import email.utils
import random
import time

import httpx
import structlog

from aiocrawler.middleware.base import Middleware
from aiocrawler.models import Request, Response

log = structlog.get_logger(__name__)

#: 这些异常属于瞬时故障，重试有意义；其余（如 SSL 证书错误）重试也没用
RETRYABLE_EXCEPTIONS = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.RemoteProtocolError,
    httpx.PoolTimeout,
)

DEFAULT_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class RetryMiddleware(Middleware):
    def __init__(
        self,
        *,
        max_retries: int = 3,
        statuses: set[int] | frozenset[int] = DEFAULT_RETRY_STATUSES,
        backoff_base: float = 1.0,
        max_backoff: float = 60.0,
        respect_retry_after: bool = True,
    ) -> None:
        self._max = max_retries
        self._statuses = frozenset(statuses)
        self._base = backoff_base
        self._max_backoff = max_backoff
        self._respect_retry_after = respect_retry_after

    async def process_response(self, request: Request, response: Response) -> Response | Request:
        if response.status not in self._statuses:
            return response

        wait = None
        if self._respect_retry_after:
            wait = self._parse_retry_after(response.headers.get("retry-after"))
            if wait is not None and wait > self._max_backoff:
                log.warning(
                    "retry_after_capped",
                    url=request.url,
                    requested=round(wait, 1),
                    capped_to=self._max_backoff,
                    hint="服务端要求的等待时间超过 max_backoff，已截断",
                )
                wait = self._max_backoff

        retry = await self._schedule_retry(request, f"HTTP {response.status}", override_delay=wait)
        return retry if retry is not None else response

    async def process_exception(self, request: Request, exc: Exception) -> Request | None:
        if not isinstance(exc, RETRYABLE_EXCEPTIONS):
            return None
        return await self._schedule_retry(request, type(exc).__name__)

    # ------------------------------------------------------------------

    async def _schedule_retry(
        self, request: Request, reason: str, *, override_delay: float | None = None
    ) -> Request | None:
        if request.retries >= self._max:
            log.warning(
                "retry_exhausted",
                url=request.url,
                reason=reason,
                attempts=request.retries + 1,
            )
            return None

        delay = override_delay if override_delay is not None else self._backoff(request.retries)
        log.debug(
            "retrying",
            url=request.url,
            reason=reason,
            attempt=request.retries + 1,
            delay=round(delay, 2),
        )
        await asyncio.sleep(delay)

        # dont_filter=True 是关键：不加的话重试请求会被去重器静默丢弃
        return request.replace(retries=request.retries + 1, dont_filter=True)

    def _backoff(self, retries: int) -> float:
        """指数退避，叠加 50%~150% 的随机抖动以打散重试时刻。"""
        raw = min(self._base * (2 ** retries), self._max_backoff)
        return raw * (0.5 + random.random())

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        """Retry-After 有两种合法格式：秒数，或 HTTP 日期。"""
        if not value:
            return None
        value = value.strip()
        if value.isdigit():
            return float(value)
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        delta = when.timestamp() - time.time()
        return max(0.0, delta)
