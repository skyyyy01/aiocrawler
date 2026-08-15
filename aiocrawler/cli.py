"""命令行入口。

    aiocrawler list                       列出所有爬虫
    aiocrawler run books                  运行
    aiocrawler run books -c 8 -n 50       临时覆盖并发与抓取上限
    aiocrawler run books -o data.db       输出到 SQLite（也支持数据库 URI）
    aiocrawler run books --resume         断点续爬：中断后接着上次继续
    aiocrawler run books --resume --fresh 清空上次状态，重新开始
    aiocrawler state books                查看/清理断点续爬状态
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated, Any

import typer

from aiocrawler import loader
from aiocrawler.engine import Engine
from aiocrawler.logconf import setup_logging
from aiocrawler.pipeline.storage import StoragePipeline
from aiocrawler.scheduler.sqlite import SqliteScheduler
from aiocrawler.settings import DEFAULT_CONFIG_FILE, load_settings
from aiocrawler.storage import storage_from_uri

app = typer.Typer(add_completion=False, help="aiocrawler —— 异步爬虫框架")


def _state_path(name: str) -> Path:
    return Path("out") / f"{name}.state.db"


@app.command("list")
def list_spiders(
    spider_dir: Annotated[str, typer.Option("--dir", "-d")] = loader.DEFAULT_SPIDER_DIR,
) -> None:
    """列出 spiders/ 目录下所有可运行的爬虫。"""
    spiders = loader.discover(spider_dir)
    if not spiders:
        typer.echo(f"在 {spider_dir}/ 下没有找到任何爬虫")
        raise typer.Exit(1)
    typer.echo(f"共 {len(spiders)} 个爬虫：")
    for name, cls in sorted(spiders.items()):
        doc = (cls.__doc__ or "").strip().splitlines()
        typer.echo(f"  {name:<16} {doc[0] if doc else ''}")


@app.command("run")
def run_spider(
    name: Annotated[str, typer.Argument(help="爬虫名称")],
    spider_dir: Annotated[str, typer.Option("--dir", "-d")] = loader.DEFAULT_SPIDER_DIR,
    output: Annotated[
        str | None,
        typer.Option("--output", "-o", help="输出目标：.jsonl/.csv/.db 或数据库 URI"),
    ] = None,
    concurrency: Annotated[int | None, typer.Option("--concurrency", "-c")] = None,
    max_items: Annotated[int | None, typer.Option("--max-items", "-n", help="抓够 N 条即停止")] = None,
    delay: Annotated[float | None, typer.Option("--delay", help="同域名请求最小间隔（秒）")] = None,
    resume: Annotated[bool, typer.Option("--resume", help="启用断点续爬")] = False,
    fresh: Annotated[bool, typer.Option("--fresh", help="配合 --resume/--redis：清空状态重新开始")] = False,
    redis: Annotated[
        str | None,
        typer.Option("--redis", help="Redis URL，启用分布式共享队列，可多节点同时运行"),
    ] = None,
    redis_prefix: Annotated[
        str | None, typer.Option("--redis-prefix", help="Redis key 前缀，默认用爬虫名")
    ] = None,
    idle_timeout: Annotated[
        float | None,
        typer.Option("--idle-timeout", help="队列空后再等待多少秒才收工（分布式必须为正数）"),
    ] = None,
    config: Annotated[str, typer.Option("--config", help="配置文件路径")] = DEFAULT_CONFIG_FILE,
    log_level: Annotated[str, typer.Option("--log-level", "-l")] = "INFO",
) -> None:
    """运行指定爬虫。"""
    setup_logging(log_level)

    try:
        spider_cls = loader.load(name, spider_dir)
    except KeyError as exc:
        typer.secho(str(exc), fg=typer.colors.RED)
        raise typer.Exit(1) from exc

    spider = spider_cls()

    cli_overrides: dict[str, Any] = {"log_level": log_level}
    if concurrency is not None:
        cli_overrides["concurrency"] = concurrency
    if max_items is not None:
        cli_overrides["max_items"] = max_items
    if delay is not None:
        cli_overrides["download_delay"] = delay
    if idle_timeout is not None:
        cli_overrides["idle_timeout"] = idle_timeout
    elif redis:
        # 分布式下队列瞬时为空是常态（别的节点正在解析），不能一看到空就收工
        cli_overrides.setdefault("idle_timeout", 10.0)

    settings = load_settings(
        name,
        custom_settings=spider.custom_settings,
        cli_overrides=cli_overrides,
        config_file=config,
    )

    out_path = output or settings.output.format(spider=name)
    storage_kwargs = dict(settings.storage_options)
    if resume and not fresh and out_path.endswith(".jsonl"):
        # JSONL 默认覆盖写，续爬时必须改成追加，否则上一轮的结果会被清掉。
        # 数据库后端靠 unique_key 的 UPSERT 保证幂等，无需特殊处理。
        storage_kwargs.setdefault("append", True)
    storage = storage_from_uri(out_path, **storage_kwargs)

    scheduler = None
    if redis:
        from aiocrawler.scheduler.redis_backend import RedisScheduler

        scheduler = RedisScheduler(
            redis, prefix=redis_prefix or f"aiocrawler:{name}", resume=not fresh
        )
    elif resume:
        scheduler = SqliteScheduler(_state_path(name), resume=not fresh)

    engine = Engine(
        spider,
        settings,
        scheduler=scheduler,
        pipelines=[
            StoragePipeline(
                storage,
                batch_size=settings.batch_size,
                flush_interval=settings.flush_interval,
            )
        ],
    )

    stats = asyncio.run(engine.run())

    typer.echo()
    typer.secho(stats.summary(), fg=typer.colors.GREEN)
    typer.echo(f"输出目标：{out_path}")
    if redis:
        typer.echo(f"共享队列：{redis}（前缀 {redis_prefix or f'aiocrawler:{name}'}）")
    elif resume:
        typer.echo(f"状态文件：{_state_path(name)}")


@app.command("state")
def show_state(
    name: Annotated[str, typer.Argument(help="爬虫名称")],
    clear: Annotated[bool, typer.Option("--clear", help="删除该爬虫的续爬状态")] = False,
) -> None:
    """查看或清理断点续爬状态。"""
    path = _state_path(name)

    if clear:
        removed = 0
        for suffix in ("", "-wal", "-shm"):
            target = Path(f"{path}{suffix}")
            if target.exists():
                target.unlink()
                removed += 1
        typer.echo(f"已清理 {removed} 个状态文件" if removed else "没有可清理的状态文件")
        return

    if not path.exists():
        typer.echo(f"爬虫 {name!r} 没有续爬状态（{path} 不存在）")
        return

    import sqlite3

    con = sqlite3.connect(path)
    try:
        pending = con.execute("SELECT COUNT(*) FROM queue WHERE status=0").fetchone()[0]
        inflight = con.execute("SELECT COUNT(*) FROM queue WHERE status=1").fetchone()[0]
        seen = con.execute("SELECT COUNT(*) FROM fingerprints").fetchone()[0]
    finally:
        con.close()

    typer.echo(f"状态文件：{path}")
    typer.echo(f"  待处理请求：{pending}")
    typer.echo(f"  中断时处理中：{inflight}（下次启动会自动重新入队）")
    typer.echo(f"  已记录指纹：{seen}")


if __name__ == "__main__":
    app()
