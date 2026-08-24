"""框架异常。"""

from __future__ import annotations


class AiocrawlerError(Exception):
    """所有框架异常的基类。"""


class DropItem(AiocrawlerError):
    """管道中主动丢弃一条 item（如校验失败、重复数据）。"""


class IgnoreRequest(AiocrawlerError):
    """中间件主动放弃一个请求（如 robots.txt 禁止、超出重试上限）。"""


class NotConfigured(AiocrawlerError):
    """组件缺少必要配置或依赖未安装，应被跳过。"""


class ResponseTooLarge(AiocrawlerError):
    """响应体超过 max_response_bytes，下载已中断。

    重试没有意义（再来一次还是那么大），因此不列入可重试异常。
    """
