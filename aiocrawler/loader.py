"""Spider 发现：扫描 spiders/ 目录，按 name 属性建立索引。"""

from __future__ import annotations

import importlib.util
import inspect
import sys
from pathlib import Path

from aiocrawler.spider import BaseSpider

DEFAULT_SPIDER_DIR = "spiders"


def _load_module(path: Path):
    """从文件路径直接加载模块，无需要求 spiders/ 是可导入的包。"""
    mod_name = f"_aiocrawler_spiders.{path.stem}"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载爬虫模块：{path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


def discover(spider_dir: str | Path = DEFAULT_SPIDER_DIR) -> dict[str, type[BaseSpider]]:
    """返回 {name: SpiderClass}。"""
    directory = Path(spider_dir)
    if not directory.is_dir():
        return {}

    found: dict[str, type[BaseSpider]] = {}
    for file in sorted(directory.glob("*.py")):
        if file.name.startswith("_"):
            continue
        module = _load_module(file)
        for _, obj in inspect.getmembers(module, inspect.isclass):
            # 只收本模块内定义的、有 name 的具体子类，
            # 避免把 import 进来的 BaseSpider 本身也算进去
            if (
                issubclass(obj, BaseSpider)
                and obj is not BaseSpider
                and getattr(obj, "name", "")
                and obj.__module__ == module.__name__
            ):
                found[obj.name] = obj
    return found


def load(name: str, spider_dir: str | Path = DEFAULT_SPIDER_DIR) -> type[BaseSpider]:
    spiders = discover(spider_dir)
    if name not in spiders:
        available = ", ".join(sorted(spiders)) or "（无）"
        raise KeyError(f"找不到名为 {name!r} 的爬虫。可用的爬虫：{available}")
    return spiders[name]
