"""按域名限速。

**限速必须按域名独立计算，不能全局共用一个速率。** 这是实战中最常见的设计
失误：一旦全局共享，抓取多个站点时，一个慢站点会拖住所有其他站点的配额，
整体吞吐量被最慢的那个域名锁死。

实现上每个域名各自维护「下一次可发起请求的时刻」，并用一把该域名专属的锁把
等待过程串行化——同域名的并发请求会依次排队，跨域名则互不干扰。
"""

from __future__ import annotations

import asyncio
import random
from collections import defaultdict
from urllib.parse import urlsplit

from aiocrawler.middleware.base import Middleware
from aiocrawler.models import Request


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

        domain = urlsplit(request.url).netloc
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
        return None

    def _interval(self, delay: float) -> float:
        if self._jitter <= 0:
            return delay
        return delay * (1.0 + random.uniform(-self._jitter, self._jitter))
