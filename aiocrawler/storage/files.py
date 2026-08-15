"""文件存储后端：JSONL 与 CSV。

实际写盘放进 asyncio.to_thread，避免阻塞事件循环。因为上层已经攒批
（默认 200 条一次），线程切换的开销被分摊掉，可以忽略。
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
from pathlib import Path
from typing import Any, TextIO


class JsonlStorage:
    """每行一个 JSON 对象。字段可变、嵌套结构友好，是爬虫最通用的落地格式。"""

    def __init__(self, path: str | Path, *, append: bool = False) -> None:
        self._path = Path(path)
        self._mode = "a" if append else "w"
        self._fh: TextIO | None = None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open(self._mode, encoding="utf-8", newline="\n")

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows or self._fh is None:
            return
        # 先在内存拼好整块字符串，只做一次 IO 调用
        buf = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        await asyncio.to_thread(self._sync_write, buf)

    def _sync_write(self, buf: str) -> None:
        assert self._fh is not None
        self._fh.write(buf)
        self._fh.flush()

    async def close(self) -> None:
        if self._fh is not None:
            await asyncio.to_thread(self._fh.close)
            self._fh = None


class CsvStorage:
    """CSV 输出。

    表头由**第一批数据的第一条**决定，后续记录按该表头对齐：缺失字段留空，
    多出的字段丢弃。这是 CSV 的固有限制——字段不固定的数据请用 JSONL。
    """

    def __init__(self, path: str | Path, *, fieldnames: list[str] | None = None) -> None:
        self._path = Path(path)
        self._fieldnames = fieldnames
        self._fh: TextIO | None = None
        self._writer: csv.DictWriter | None = None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open("w", encoding="utf-8-sig", newline="")
        if self._fieldnames:
            self._init_writer(self._fieldnames)

    def _init_writer(self, fieldnames: list[str]) -> None:
        assert self._fh is not None
        self._fieldnames = fieldnames
        self._writer = csv.DictWriter(
            self._fh, fieldnames=fieldnames, extrasaction="ignore"
        )
        self._writer.writeheader()

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows or self._fh is None:
            return
        if self._writer is None:
            self._init_writer(list(rows[0].keys()))
        await asyncio.to_thread(self._sync_write, rows)

    def _sync_write(self, rows: list[dict[str, Any]]) -> None:
        assert self._writer is not None and self._fh is not None
        # 嵌套结构 CSV 表达不了，序列化成 JSON 字符串塞进单元格
        for row in rows:
            self._writer.writerow({
                k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                for k, v in row.items()
            })
        self._fh.flush()

    async def close(self) -> None:
        if self._fh is not None:
            await asyncio.to_thread(self._fh.close)
            self._fh = None
            self._writer = None
