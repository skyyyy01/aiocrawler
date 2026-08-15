"""User-Agent 设置与轮换。

给定多个 UA 时按请求随机挑选；只给一个就固定使用。
已在 request.headers 里显式设置了 UA 的请求不会被覆盖——
让 spider 有能力对个别请求指定特殊 UA。
"""

from __future__ import annotations

import random

from aiocrawler.middleware.base import Middleware
from aiocrawler.models import Request

#: 一组常见桌面浏览器 UA
DEFAULT_USER_AGENTS = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
)


class UserAgentMiddleware(Middleware):
    def __init__(self, user_agents: list[str] | tuple[str, ...] | str | None = None) -> None:
        if user_agents is None:
            pool = list(DEFAULT_USER_AGENTS)
        elif isinstance(user_agents, str):
            pool = [user_agents]
        else:
            pool = list(user_agents)
        if not pool:
            raise ValueError("user_agents 不能为空")
        self._pool = pool

    async def process_request(self, request: Request) -> None:
        # 已显式指定的不动，尊重 spider 的选择
        if any(k.lower() == "user-agent" for k in request.headers):
            return None
        request.headers["User-Agent"] = (
            self._pool[0] if len(self._pool) == 1 else random.choice(self._pool)
        )
        return None
