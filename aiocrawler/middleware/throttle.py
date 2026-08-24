"""按域名限速。

**限速必须按域名独立计算，不能全局共用一个速率。** 这是实战中最常见的设计
失误：一旦全局共享，抓取多个站点时，一个慢站点会拖住所有其他站点的配额，
整体吞吐量被最慢的那个域名锁死。

实现上每个域名各自维护「下一次可发起请求的时刻」，并用一把该域名专属的锁把
等待过程串行化——同域名的并发请求会依次排队，跨域名则互不干扰。

## 两个容易忽略的细节

**域名要先规范化。** 直接拿 netloc 当键的话，`Example.com`、`example.com`、
`example.com:443` 会各自占一个桶，各限各的——对同一台服务器的实际请求速率就
翻了几倍，限速形同虚设。

**桶数要有上限。** 全网漫游型的爬虫会遇到几十万个域名，每个域名留一把锁加一个
时间戳，内存就这么慢慢涨上去了。这里做定期清理：只保留还在冷却期内的条目，
已经过期的桶留着也没有意义。
"""

from __future__ import annotations

import asyncio
import random
from collections import defaultdict
from urllib.parse import urlsplit

import structlog

from aiocrawler.middleware.base import Middleware
from aiocrawler.models import Request

log = structlog.get_logger(__name__)

#: 累积到这么多域名就清一次过期条目
_GC_THRESHOLD = 10_000

#: scheme 的默认端口，规范化时剥掉
_DEFAULT_PORTS = {"http": 80, "https": 443}


def domain_key(url: str) -> str:
    """把 URL 归一成限速用的域名键。

    小写化，并剥掉与 scheme 对应的默认端口——`example.com` 和 `example.com:443`
    指向同一台服务器，必须落进同一个桶。
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    port = parts.port
    if port is not None and port != _DEFAULT_PORTS.get(parts.scheme.lower()):
        return f"{host}:{port}"
    return host


class ThrottleMiddleware(Middleware):
    def __init__(self, *, delay: float = 1.0, jitter: float = 0.3) -> None:
        """
        :param delay: 同一域名两次请求之间的最小间隔（秒）
        :param jitter: 抖动比例，实际间隔在 delay*(1±jitter) 范围内随机波动。
                       固定节奏的请求特征明显，轻微抖动更接近真实访问。
        """
        self._delay = delay
        self._jitter = jitter
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._next_at: dict[str, float] = {}

    async def process_request(self, request: Request) -> None:
        # RobotsMiddleware 会把站点 robots.txt 里声明的 Crawl-delay 放进 meta，
        # 站点自己给的间隔优先于我们的预设值
        delay = float(request.meta.get("_crawl_delay") or self._delay)
        if delay <= 0:
            return None

        domain = domain_key(request.url)
        loop = asyncio.get_running_loop()

        # 持锁期间完成「等待 + 预定下一次时刻」，保证同域名请求严格排队。
        # 锁是按域名分的，因此不同域名可以完全并行。
        async with self._locks[domain]:
            now = loop.time()
            scheduled = self._next_at.get(domain, 0.0)
            if now < scheduled:
                await asyncio.sleep(scheduled - now)
                now = loop.time()
            self._next_at[domain] = now + self._interval(delay)

        if len(self._locks) > _GC_THRESHOLD:
            self._gc(loop.time())
        return None

    def _gc(self, now: float) -> None:
        """丢掉已经过了冷却期、且当前没人持锁的域名条目。"""
        before = len(self._locks)
        for domain in [d for d, at in self._next_at.items() if at <= now]:
            lock = self._locks.get(domain)
            if lock is not None and lock.locked():
                continue  # 还有请求在等，留着
            self._locks.pop(domain, None)
            self._next_at.pop(domain, None)
        log.debug("throttle_gc", before=before, after=len(self._locks))

    def _interval(self, delay: float) -> float:
        if self._jitter <= 0:
            return delay
        return delay * (1.0 + random.uniform(-self._jitter, self._jitter))
