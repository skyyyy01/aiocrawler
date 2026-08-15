"""structlog 日志配置。

用结构化日志而非 print：每条日志都是带字段的事件，既能在终端彩色可读，
也能一行改成 JSON 输出接入日志系统。
"""

from __future__ import annotations

import logging

import structlog


#: 这些库在 INFO 级别会为每个请求打一行日志，量大时完全淹没框架自己的输出
_NOISY_LOGGERS = ("httpx", "httpcore", "hpack", "asyncio")


def setup_logging(level: str = "INFO", *, json_output: bool = False) -> None:
    logging.basicConfig(format="%(message)s", level=getattr(logging, level.upper(), logging.INFO))

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)

    renderer = (
        structlog.processors.JSONRenderer(ensure_ascii=False)
        if json_output
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        cache_logger_on_first_use=True,
    )
