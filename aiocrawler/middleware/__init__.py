"""下载中间件集合与默认装配。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiocrawler.middleware.base import Middleware, MiddlewareManager
from aiocrawler.middleware.proxy import ProxyMiddleware
from aiocrawler.middleware.retry import RetryMiddleware
from aiocrawler.middleware.robots import RobotsMiddleware
from aiocrawler.middleware.throttle import ThrottleMiddleware
from aiocrawler.middleware.useragent import UserAgentMiddleware

if TYPE_CHECKING:
    from aiocrawler.settings import Settings

__all__ = [
    "Middleware",
    "MiddlewareManager",
    "ProxyMiddleware",
    "RetryMiddleware",
    "RobotsMiddleware",
    "ThrottleMiddleware",
    "UserAgentMiddleware",
    "build_default_middlewares",
]


def build_default_middlewares(settings: Settings) -> list[Middleware]:
    """按正确的顺序装配默认中间件链。

    ## 顺序为什么是这样

    请求侧按列表正序执行，响应/异常侧按**逆序**执行。RetryMiddleware 放在最前，
    意味着它在异常侧**最后**执行——这是必须的：ProxyMiddleware 要先有机会把失效
    代理标记进冷却，Retry 才去发起重试，重试时才能选到一个健康的代理。
    顺序反过来的话，Retry 会先短路返回，代理永远得不到标记。

    Throttle 放在最后（请求侧最末），保证限速等待发生在所有请求加工完成之后，
    紧挨着真正的下载动作，这样「间隔」才对应真实的发包时刻。
    """
    middlewares: list[Middleware] = [
        RetryMiddleware(
            max_retries=settings.max_retries,
            statuses=settings.retry_statuses,
            backoff_base=settings.retry_backoff_base,
            max_backoff=settings.retry_max_backoff,
        )
    ]

    if settings.respect_robots:
        middlewares.append(RobotsMiddleware())

    middlewares.append(UserAgentMiddleware(settings.user_agents))

    if settings.proxies:
        middlewares.append(
            ProxyMiddleware(settings.proxies, cooldown=settings.proxy_cooldown)
        )

    middlewares.append(
        ThrottleMiddleware(
            delay=settings.download_delay,
            jitter=settings.download_delay_jitter,
        )
    )
    return middlewares
