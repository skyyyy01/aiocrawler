"""文件存储后端：JSONL 与 CSV。

实际写盘放进 asyncio.to_thread，避免阻塞事件循环。因为上层已经攒批
（默认 200 条一次），线程切换的开销被分摊掉，可以忽略。

## 为什么 CSV 要转义公式

爬虫写进 CSV 的每个字段都来自被抓取的页面，也就是不可信的远端内容。
Excel / LibreOffice / Google Sheets 会把以 `=` `+` `-` `@` 开头的单元格
当公式执行，于是页面上一段 `=cmd|'/c calc'!A1` 就能在打开表格的人机器上
落地成命令执行（CWE-1236，俗称 CSV 公式注入）。

因此这里给危险前缀补一个单引号——这是表格软件公认的「按纯文本处理」写法，
肉眼可见、可逆，也不影响再次被程序读取。JSONL 没有这个问题，不做处理。
"""

from __future__ import annotations

import asyncio
import csv
import json
from pathlib import Path
from typing import Any, TextIO

#: 表格软件据此判定「这是公式」的起始字符
_FORMULA_PREFIXES = ("=", "+", "-", "@")
#: 前导的制表符/回车同样能让后面的内容被当成公式
_FORMULA_LEAD_CONTROL = ("\t", "\r")


def escape_formula(value: Any) -> Any:
    """给可能被表格软件当成公式的文本加上前导单引号。"""
    if not isinstance(value, str) or not value:
        return value
    if value.startswith(_FORMULA_PREFIXES) or value.startswith(_FORMULA_LEAD_CONTROL):
        return "'" + value
    return value


class JsonlStorage:
    """每行一个 JSON 对象。字段可变、嵌套结构友好，是爬虫最通用的落地格式。"""

    #: 覆盖写的后端，续爬时需要由调用方改成追加
    truncates_on_open = True

    def __init__(self, path: str | Path, *, append: bool = False) -> None:
        self._path = Path(path)
        self._mode = "a" if append else "w"
        self._fh: TextIO | None = None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open(self._mode, encoding="utf-8", newline="\n")

    async def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if self._fh is None:
            raise RuntimeError(
                f"{type(self).__name__} 未打开，请先 await open()——"
                "静默丢弃这批数据会让问题在很久以后才暴露"
            )
        # 先在内存拼好整块字符串，只做一次 IO 调用
        buf = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        await asyncio.to_thread(self._sync_write, buf)

    def _sync_write(self, buf: str) -> None:
        assert self._fh is not None
        self._fh.write(buf)
        self._fh.flush()

    async def close(self) -> None:
        if self._fh is not None:
            fh, self._fh = self._fh, None
            await asyncio.to_thread(fh.close)


class CsvStorage:
    """CSV 输出。

    表头由**第一批数据的第一条**决定，后续记录按该表头对齐：缺失字段留空，
    多出的字段丢弃。这是 CSV 的固有限制——字段不固定的数据请用 JSONL。

    append=True 时从已有文件的首行读回表头并接着写，供断点续爬使用。
    """

    truncates_on_open = True

    def __init__(
        self,
        path: str | Path,
        *,
        fieldnames: list[str] | None = None,
        append: bool = False,
    ) -> None:
        self._path = Path(path)
        self._fieldnames = fieldnames
        self._append = append
        self._fh: TextIO | None = None
        self._writer: csv.DictWriter | None = None

    def _existing_header(self) -> list[str] | None:
        """读回已有文件的表头，供追加写复用。"""
        if not self._path.is_file() or self._path.stat().st_size == 0:
            return None
        with self._path.open("r", encoding="utf-8-sig", newline="") as fh:
            header = next(csv.reader(fh), None)
        return header or None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)

        resumed_header = self._existing_header() if self._append else None
        if resumed_header is not None:
            # 接着上一轮写：沿用原表头，且不能再写一遍表头行
            self._fh = self._path.open("a", encoding="utf-8-sig", newline="")
            self._fieldnames = self._fieldnames or resumed_header
            self._writer = csv.DictWriter(
                self._fh, fieldnames=self._fieldnames, extrasaction="ignore"
            )
            return

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
        if not rows:
            return
        if self._fh is None:
            raise RuntimeError(
                f"{type(self).__name__} 未打开，请先 await open()——"
                "静默丢弃这批数据会让问题在很久以后才暴露"
            )
        if self._writer is None:
            self._init_writer(list(rows[0].keys()))
        await asyncio.to_thread(self._sync_write, rows)

    def _sync_write(self, rows: list[dict[str, Any]]) -> None:
        assert self._writer is not None and self._fh is not None
        # 嵌套结构 CSV 表达不了，序列化成 JSON 字符串塞进单元格；
        # 纯文本则要挡掉公式注入（见模块文档）
        for row in rows:
            self._writer.writerow({
                k: json.dumps(v, ensure_ascii=False)
                if isinstance(v, (dict, list))
                else escape_formula(v)
                for k, v in row.items()
            })
        self._fh.flush()

    async def close(self) -> None:
        if self._fh is not None:
            fh, self._fh = self._fh, None
            self._writer = None
            await asyncio.to_thread(fh.close)
