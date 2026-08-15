"""存储后端与批量缓冲管道。"""

from __future__ import annotations

import json

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
