"""框架配置与分层加载。

优先级由低到高：

    1. 内置默认值（本文件里的字段定义）
    2. settings.toml 的 [default] 段
    3. spider 类上的 custom_settings
    4. settings.toml 的 [spider.<name>] 段
    5. 命令行参数

第 3 层与第 4 层的先后是刻意这样排的：custom_settings 是爬虫作者写在代码里的
默认值，而 toml 里针对某个爬虫的专属段属于运维侧调整，应当能覆盖代码——
这样调整线上行为不必改代码。
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, PrivateAttr


class Settings(BaseModel):
    model_config = {"extra": "forbid"}

    #: 这份配置是否已在外层（load_settings）叠加过 spider.custom_settings。
    #: Engine 据此避免二次合并——二次合并会让 custom_settings 反超 [spider.x]
    #: 与命令行参数，把文档里定下的优先级整个颠倒过来。
    _custom_applied: bool = PrivateAttr(default=False)

    # ---- 并发 ----
    concurrency: int = Field(default=16, ge=1, description="全局并发 worker 数")
    #: 单个域名同时在途的请求数上限，在 MiddlewareManager 里落地。
    #: 它管的是「并发」，download_delay 管的是「间隔」，两者独立：
    #: download_delay > 0 时同域名请求已被 ThrottleMiddleware 串行化，
    #: 这个上限基本不会触发；真正起作用的是 download_delay = 0 的场景，
    #: 以及同时抓多个站点时——防止某个慢站点把所有 worker 都占住，
    #: 让其他站点完全抓不动。
    #: 取值 >= concurrency 等于不限制（引擎会直接跳过这层）。
    concurrency_per_domain: int = Field(default=8, ge=1, description="单域名并发上限")

    # ---- 下载 ----
    timeout: float = Field(default=20.0, gt=0)
    follow_redirects: bool = True
    http2: bool = True
    verify_ssl: bool = True
    default_headers: dict[str, str] = Field(default_factory=dict)
    proxy: str | None = None
    #: 单个响应体的字节上限，None 表示不限。对端返回多大完全由它决定，
    #: 不设限就等于把进程的内存交给被抓站点支配
    max_response_bytes: int | None = Field(default=64 * 1024 * 1024, gt=0)

    # ---- 礼貌抓取（默认保守，避免给目标站点造成压力）----
    download_delay: float = Field(default=1.0, ge=0, description="同域名两次请求的最小间隔（秒）")
    download_delay_jitter: float = Field(default=0.3, ge=0, le=1, description="间隔抖动比例")
    respect_robots: bool = True

    # ---- 重试 ----
    max_retries: int = Field(default=3, ge=0)
    retry_backoff_base: float = Field(default=1.0, gt=0)
    retry_max_backoff: float = Field(default=60.0, gt=0)
    retry_statuses: set[int] = Field(default_factory=lambda: {429, 500, 502, 503, 504})

    # ---- 身份与代理 ----
    user_agents: list[str] | None = Field(default=None, description="为空则使用内置 UA 池")
    proxies: list[str] = Field(default_factory=list, description="代理池，为空则直连")
    proxy_cooldown: float = Field(default=60.0, gt=0, description="代理失败后的冷却秒数")

    # ---- 浏览器渲染（仅当 spider 产出 renderer="browser" 的请求时才会启动）----
    browser_contexts: int = Field(default=4, ge=1, description="并发渲染的页面数上限")
    browser_headless: bool = True
    browser_timeout: float = Field(default=30.0, gt=0)
    browser_wait_until: str = Field(
        default="domcontentloaded",
        description="commit | domcontentloaded | load | networkidle",
    )
    browser_block_resources: list[str] = Field(
        default_factory=lambda: ["image", "media", "font"],
        description="拦截这些资源类型以加速渲染；需要截图时置空",
    )

    # ---- 存储 ----
    batch_size: int = Field(default=200, ge=1)
    flush_interval: float = Field(default=5.0, gt=0)
    #: 支持文件路径与数据库 URI，具体见 storage.storage_from_uri
    output: str = "out/{spider}.jsonl"
    #: 传给存储后端构造函数的额外参数，例如
    #: {"unique_key": "url", "table": "books"}
    storage_options: dict[str, Any] = Field(default_factory=dict)

    # ---- 运行 ----
    # 软上限：达到后停止派发新请求，但已在途的请求仍会跑完，
    # 因此并发 N 时实际产出可能略微超过该值。需要精确条数请设 concurrency=1。
    max_items: int | None = Field(default=None, description="抓够指定条数后停止，用于调试")
    log_level: str = "INFO"
    stats_interval: float = Field(default=10.0, gt=0, description="进度日志间隔（秒）")
    #: 队列空且无在途请求时，再等待多少秒才判定抓取结束。
    #: 单机默认 0（立即结束）。**分布式必须设为正数**：共享队列时本节点看到的
    #: 队列为空，可能只是因为其他节点正在解析页面、马上就会产出新链接，
    #: 立即退出会导致节点提前离场。
    idle_timeout: float = Field(default=0.0, ge=0)

    @property
    def custom_settings_applied(self) -> bool:
        """custom_settings 这一层是否已经被外层处理过。"""
        return self._custom_applied

    def merged(
        self,
        overrides: dict[str, Any] | None,
        *,
        applies_custom_settings: bool = False,
    ) -> Settings:
        """叠加一层覆盖配置，返回新实例（不修改原对象）。

        :param applies_custom_settings: 本次叠加的就是 spider.custom_settings。
            置位后 Engine 不会再合并一次，从而保证 [spider.x] 与命令行参数
            始终排在 custom_settings 之上。
        """
        if not overrides and not applies_custom_settings:
            return self
        merged = Settings(**{**self.model_dump(), **(overrides or {})})
        merged._custom_applied = self._custom_applied or applies_custom_settings
        return merged


DEFAULT_CONFIG_FILE = "settings.toml"


def read_config_file(path: str | Path = DEFAULT_CONFIG_FILE) -> dict[str, Any]:
    """读取 toml 配置。文件不存在时返回空字典，不报错。"""
    file = Path(path)
    if not file.is_file():
        return {}
    with file.open("rb") as fh:
        return tomllib.load(fh)


def load_settings(
    spider_name: str | None = None,
    *,
    custom_settings: dict[str, Any] | None = None,
    cli_overrides: dict[str, Any] | None = None,
    config_file: str | Path = DEFAULT_CONFIG_FILE,
) -> Settings:
    """按既定优先级把各层配置合并成最终的 Settings。

    toml 结构：

        [default]
        concurrency = 16

        [spider.books]
        concurrency = 8
        download_delay = 0.1
    """
    config = read_config_file(config_file)

    settings = Settings(**config.get("default", {}))
    # 即使 custom_settings 为空也要置位：这一层的归属已经由本函数认领，
    # Engine 不该再插手
    settings = settings.merged(custom_settings, applies_custom_settings=True)

    if spider_name:
        settings = settings.merged(config.get("spider", {}).get(spider_name))

    return settings.merged(cli_overrides)
