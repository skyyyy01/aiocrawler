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

from pydantic import BaseModel, Field


class Settings(BaseModel):
    model_config = {"extra": "forbid"}

    # ---- 并发 ----
    concurrency: int = Field(default=16, ge=1, description="全局并发 worker 数")
    concurrency_per_domain: int = Field(default=8, ge=1, description="单域名并发上限")

    # ---- 下载 ----
    timeout: float = Field(default=20.0, gt=0)
    follow_redirects: bool = True
    http2: bool = True
    verify_ssl: bool = True
    default_headers: dict[str, str] = Field(default_factory=dict)
    proxy: str | None = None

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

    def merged(self, overrides: dict[str, Any] | None) -> Settings:
        """叠加一层覆盖配置，返回新实例（不修改原对象）。"""
        if not overrides:
            return self
        return Settings(**{**self.model_dump(), **overrides})


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
    settings = settings.merged(custom_settings)

    if spider_name:
        settings = settings.merged(config.get("spider", {}).get(spider_name))

    return settings.merged(cli_overrides)
