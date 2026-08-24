"""存储后端与批量缓冲管道。"""

from __future__ import annotations

import asyncio
import csv
import json

import pytest

from aiocrawler.models import Item
from aiocrawler.pipeline.storage import StoragePipeline
from aiocrawler.storage.files import CsvStorage, JsonlStorage
from tests.conftest import MemoryStorage


class Row(Item):
    name: str
    value: int


class TestJsonlStorage:
    async def test_writes_one_json_per_line(self, tmp_path):
        path = tmp_path / "out.jsonl"
        s = JsonlStorage(path)
        await s.open()
        await s.write([{"a": 1}, {"a": 2}])
        await s.close()

        lines = path.read_text(encoding="utf-8").splitlines()
        assert [json.loads(x) for x in lines] == [{"a": 1}, {"a": 2}]

    async def test_preserves_non_ascii(self, tmp_path):
        path = tmp_path / "out.jsonl"
        s = JsonlStorage(path)
        await s.open()
        await s.write([{"标题": "中文内容"}])
        await s.close()
        # 不能被转义成 \uXXXX，否则文件不可直接阅读
        assert "中文内容" in path.read_text(encoding="utf-8")

    async def test_creates_parent_directory(self, tmp_path):
        path = tmp_path / "nested" / "deep" / "out.jsonl"
        s = JsonlStorage(path)
        await s.open()
        await s.write([{"a": 1}])
        await s.close()
        assert path.exists()

    async def test_append_mode_keeps_existing(self, tmp_path):
        path = tmp_path / "out.jsonl"
        path.write_text('{"old": 1}\n', encoding="utf-8")
        s = JsonlStorage(path, append=True)
        await s.open()
        await s.write([{"new": 2}])
        await s.close()
        assert len(path.read_text(encoding="utf-8").splitlines()) == 2


class TestCsvStorage:
    async def test_header_from_first_row(self, tmp_path):
        path = tmp_path / "out.csv"
        s = CsvStorage(path)
        await s.open()
        await s.write([{"a": 1, "b": 2}])
        await s.close()
        assert path.read_text(encoding="utf-8-sig").splitlines()[0] == "a,b"

    async def test_extra_fields_ignored(self, tmp_path):
        path = tmp_path / "out.csv"
        s = CsvStorage(path)
        await s.open()
        await s.write([{"a": 1, "b": 2}, {"a": 3, "b": 4, "c": 5}])
        await s.close()
        rows = path.read_text(encoding="utf-8-sig").splitlines()
        assert rows[0] == "a,b" and rows[2] == "3,4"

    async def test_nested_value_serialized_as_json(self, tmp_path):
        path = tmp_path / "out.csv"
        s = CsvStorage(path)
        await s.open()
        await s.write([{"a": {"k": "v"}}])
        await s.close()
        assert '{""k"": ""v""}' in path.read_text(encoding="utf-8-sig")


class TestStoragePipeline:
    async def test_buffers_until_batch_size(self):
        store = MemoryStorage()
        pipe = StoragePipeline(store, batch_size=3, flush_interval=3600)
        await pipe.open_spider(None)

        for i in range(2):
            await pipe.process_item(Row(name=f"n{i}", value=i), None)
        assert store.rows == []          # 未满一批，不应写盘

        await pipe.process_item(Row(name="n2", value=2), None)
        assert len(store.rows) == 3      # 刚好凑满，触发写入
        assert store.batches == [3]

        await pipe.close_spider(None)

    async def test_close_flushes_remainder(self):
        """收尾时必须刷净不足一批的残留，否则末尾数据会丢失。"""
        store = MemoryStorage()
        pipe = StoragePipeline(store, batch_size=100, flush_interval=3600)
        await pipe.open_spider(None)
        await pipe.process_item(Row(name="only", value=1), None)
        assert store.rows == []

        await pipe.close_spider(None)
        assert len(store.rows) == 1
        assert store.closed is True

    async def test_item_passes_through(self):
        store = MemoryStorage()
        pipe = StoragePipeline(store, batch_size=100, flush_interval=3600)
        await pipe.open_spider(None)
        item = Row(name="x", value=1)
        assert await pipe.process_item(item, None) is item
        await pipe.close_spider(None)


class TestCsvFormulaInjection:
    """CSV 单元格里的公式会被表格软件执行（CWE-1236）。

    爬虫写进 CSV 的每个字段都来自被抓取的页面，属于不可信输入。
    """

    @pytest.mark.parametrize(
        "payload",
        ["=cmd|'/c calc'!A1", "+1+1", "-2+3", "@SUM(1+1)", "\t=1+1", "\r=1+1"],
    )
    async def test_dangerous_prefixes_are_neutralized(self, tmp_path, payload):
        path = tmp_path / "out.csv"
        s = CsvStorage(path)
        await s.open()
        await s.write([{"title": payload}])
        await s.close()

        cell = list(csv.DictReader(path.open(encoding="utf-8-sig")))[0]["title"]
        assert cell.startswith("'"), cell

    @pytest.mark.parametrize("payload", ["=cmd|'/c calc'!A1", "+1+1", "@SUM(1+1)"])
    async def test_escaping_is_reversible(self, tmp_path, payload):
        """去掉前导单引号即还原原文——转义不能损坏数据。

        含 \r 的取值除外：csv 模块自己会把它规范化成 \n，与转义无关。
        """
        path = tmp_path / "out.csv"
        s = CsvStorage(path)
        await s.open()
        await s.write([{"title": payload}])
        await s.close()
        cell = list(csv.DictReader(path.open(encoding="utf-8-sig")))[0]["title"]
        assert cell[1:] == payload

    async def test_ordinary_text_untouched(self, tmp_path):
        path = tmp_path / "out.csv"
        s = CsvStorage(path)
        await s.open()
        await s.write([{"title": "普通标题", "price": "£51.77"}])
        await s.close()
        row = list(csv.DictReader(path.open(encoding="utf-8-sig")))[0]
        assert row == {"title": "普通标题", "price": "£51.77"}


class TestAppendMode:
    """回归：续爬时文件后端必须追加，否则上一轮结果被清空且再也抓不回来。"""

    async def test_jsonl_append_keeps_previous_rows(self, tmp_path):
        path = tmp_path / "out.jsonl"
        first = JsonlStorage(path)
        await first.open()
        await first.write([{"a": 1}])
        await first.close()

        second = JsonlStorage(path, append=True)
        await second.open()
        await second.write([{"a": 2}])
        await second.close()

        rows = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines()]
        assert rows == [{"a": 1}, {"a": 2}]

    async def test_csv_append_reuses_header(self, tmp_path):
        path = tmp_path / "out.csv"
        first = CsvStorage(path)
        await first.open()
        await first.write([{"name": "a", "value": 1}])
        await first.close()

        second = CsvStorage(path, append=True)
        await second.open()
        await second.write([{"name": "b", "value": 2}])
        await second.close()

        rows = list(csv.DictReader(path.open(encoding="utf-8-sig")))
        assert [r["name"] for r in rows] == ["a", "b"]
        # 表头只能有一行
        assert path.read_text(encoding="utf-8-sig").count("name,value") == 1

    async def test_csv_append_on_missing_file_writes_header(self, tmp_path):
        path = tmp_path / "fresh.csv"
        s = CsvStorage(path, append=True)
        await s.open()
        await s.write([{"name": "a"}])
        await s.close()
        assert list(csv.DictReader(path.open(encoding="utf-8-sig"))) == [{"name": "a"}]


class TestWriteBeforeOpen:
    """回归：未 open 时静默丢数据，会让 open 失败这类问题拖到最后才暴露。"""

    async def test_jsonl_raises(self, tmp_path):
        with pytest.raises(RuntimeError, match="未打开"):
            await JsonlStorage(tmp_path / "x.jsonl").write([{"a": 1}])

    async def test_csv_raises(self, tmp_path):
        with pytest.raises(RuntimeError, match="未打开"):
            await CsvStorage(tmp_path / "x.csv").write([{"a": 1}])


class TestStoragePipelineFailureHandling:
    """回归：一次写库抖动曾导致定时 flush 永久停摆 + 整段缓冲静默丢失。"""

    class FlakyStorage:
        def __init__(self, fail_times: int = 1):
            self.remaining_failures = fail_times
            self.rows: list[dict] = []
            self.closed = False

        async def open(self) -> None: ...

        async def write(self, rows):
            if self.remaining_failures > 0:
                self.remaining_failures -= 1
                raise RuntimeError("数据库瞬时抖动")
            self.rows.extend(rows)

        async def close(self) -> None:
            self.closed = True

    async def test_transient_failure_loses_nothing(self):
        storage = self.FlakyStorage(fail_times=1)
        pipe = StoragePipeline(storage, batch_size=1000, flush_interval=0.02)
        await pipe.open_spider(None)

        await pipe.process_item(Row(name="a", value=1), None)
        await asyncio.sleep(0.06)          # 第一次定时 flush：失败
        assert not pipe._ticker.done(), "定时 flush 任务不能因单次失败而退出"

        for i in range(2, 5):
            await pipe.process_item(Row(name=f"r{i}", value=i), None)
        await asyncio.sleep(0.06)          # 第二次定时 flush：成功

        await pipe.close_spider(None)
        assert [r["value"] for r in storage.rows] == [1, 2, 3, 4]
        assert storage.closed

    async def test_storage_closed_even_if_final_flush_fails(self):
        storage = self.FlakyStorage(fail_times=99)
        pipe = StoragePipeline(storage, batch_size=1000, flush_interval=3600)
        await pipe.open_spider(None)
        await pipe.process_item(Row(name="a", value=1), None)

        await pipe.close_spider(None)
        # flush 注定失败，但后端仍必须被关掉，否则连接/句柄会漏
        assert storage.closed

    async def test_write_failure_does_not_break_crawling(self):
        """写库失败不能把异常抛回引擎——那会被记成请求失败，还会打断 parse。"""
        storage = self.FlakyStorage(fail_times=99)
        pipe = StoragePipeline(storage, batch_size=1, flush_interval=3600)
        await pipe.open_spider(None)
        item = Row(name="a", value=1)
        assert await pipe.process_item(item, None) is item
        await pipe.close_spider(None)

    async def test_backlog_is_bounded(self):
        storage = self.FlakyStorage(fail_times=99)
        pipe = StoragePipeline(storage, batch_size=2, flush_interval=3600)
        await pipe.open_spider(None)
        for i in range(100):
            await pipe.process_item(Row(name=f"r{i}", value=i), None)
        # 后端长期不可用时缓冲不能无限涨
        assert len(pipe._buffer) <= pipe._max_backlog
        await pipe.close_spider(None)
