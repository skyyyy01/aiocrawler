"""运行期计数与进度输出。"""

from __future__ import annotations

import time
from collections import Counter


class Stats:
    """简单计数器。asyncio 单线程模型下无需加锁。"""

    __slots__ = ("_counter", "_start")

    def __init__(self) -> None:
        self._counter: Counter[str] = Counter()
        self._start = time.monotonic()

    def inc(self, key: str, n: int = 1) -> None:
        self._counter[key] += n

    def get(self, key: str) -> int:
        return self._counter[key]

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start

    def snapshot(self) -> dict[str, float | int]:
        data: dict[str, float | int] = dict(self._counter)
        elapsed = self.elapsed
        data["elapsed_sec"] = round(elapsed, 2)
        if elapsed > 0:
            data["pages_per_sec"] = round(self._counter["response/received"] / elapsed, 2)
        return data

    def summary(self) -> str:
        s = self.snapshot()
        return (
            f"请求 {s.get('request/scheduled', 0)} 个 | "
            f"响应 {s.get('response/received', 0)} 个 | "
            f"去重丢弃 {s.get('request/duplicated', 0)} 个 | "
            f"失败 {s.get('request/failed', 0)} 个 | "
            f"产出 {s.get('item/stored', 0)} 条 | "
            f"耗时 {s.get('elapsed_sec', 0)}s | "
            f"速率 {s.get('pages_per_sec', 0)} 页/秒"
        )
