"""各 SQL 后端共用的 schema 推断与取值规范化。

爬虫的字段往往在写第一批数据时才真正确定，因此这里支持从数据推断建表结构：
扫描首批记录，为每列选出能容纳所有取值的类型。推断结果只用于自动建表，
生产环境更推荐显式传 `schema` 参数把结构固定下来。
"""

from __future__ import annotations

import json
import re
from enum import Enum
from typing import Any, Iterable

#: 只能出现在 SQL 关键字位置（如 CHARSET=）的取值，无法用引号包起来，
#: 只能走白名单
_BARE_WORD = re.compile(r"^[A-Za-z0-9_]+$")


def quote_ident(name: str, quote: str = '"') -> str:
    """把表名 / 列名安全地包进引号。

    **只加引号是不够的。** 名字里本身带一个引号，就能把我们加的引号提前闭合，
    后面的内容随即变成新的 SQL 片段——表名、列名这类标识符没法用占位符传参，
    所以它是 f-string 拼 SQL 时唯一的注入入口。asyncpg 的 execute() 在不带参数
    时按脚本执行多条语句，注入在 PostgreSQL 上就是完整的堆叠查询。

    SQL 标准的转义方式是把引号本身重复一次（`a"b` → `"a""b"`）；PostgreSQL 与
    SQLite 用双引号，MySQL 用反引号，规则相同。NUL 字节会让不少驱动直接截断
    语句，无法可靠转义，一律拒绝。
    """
    if not isinstance(name, str) or not name:
        raise ValueError(f"SQL 标识符不能为空：{name!r}")
    if "\x00" in name:
        raise ValueError(f"SQL 标识符不能包含 NUL 字节：{name!r}")
    return f"{quote}{name.replace(quote, quote * 2)}{quote}"


def check_bare_word(value: str, *, field: str) -> str:
    """校验只能裸写进 SQL 的取值（字符集名等），不合规直接拒绝。"""
    if not isinstance(value, str) or not _BARE_WORD.match(value):
        raise ValueError(
            f"{field} 只允许字母、数字和下划线，收到 {value!r}——"
            "该取值会被直接拼进 SQL，无法用引号或占位符保护"
        )
    return value


class ColumnType(str, Enum):
    """中立的列类型，由各后端映射到自己的 SQL 类型。"""

    BOOL = "bool"
    INT = "int"
    FLOAT = "float"
    TEXT = "text"
    JSON = "json"


#: 类型合并优先级：数值越大越"宽"。同一列出现多种类型时取最宽的那个，
#: 例如既有 int 又有 float 就用 float，出现字符串则整列退化为 text。
_WIDTH = {
    ColumnType.BOOL: 0,
    ColumnType.INT: 1,
    ColumnType.FLOAT: 2,
    ColumnType.TEXT: 3,
    ColumnType.JSON: 4,
}


def infer_column_type(value: Any) -> ColumnType | None:
    """推断单个取值的类型。None 表示无法判断（该值为 null，不参与推断）。"""
    if value is None:
        return None
    # bool 必须在 int 之前判断——Python 里 bool 是 int 的子类
    if isinstance(value, bool):
        return ColumnType.BOOL
    if isinstance(value, int):
        return ColumnType.INT
    if isinstance(value, float):
        return ColumnType.FLOAT
    if isinstance(value, (dict, list, tuple, set)):
        return ColumnType.JSON
    return ColumnType.TEXT


def infer_schema(rows: Iterable[dict[str, Any]]) -> dict[str, ColumnType]:
    """扫描一批记录，推断出 {列名: 类型}。

    列的顺序按首次出现顺序保留，让生成的表结构与 Item 的字段顺序一致。
    """
    schema: dict[str, ColumnType] = {}
    for row in rows:
        for key, value in row.items():
            found = infer_column_type(value)
            if found is None:
                # 全为 None 的列也要建出来，默认给 TEXT
                schema.setdefault(key, ColumnType.TEXT)
                continue
            current = schema.get(key)
            if current is None or _WIDTH[found] > _WIDTH[current]:
                schema[key] = found
    return schema


def json_safe(value: Any) -> Any:
    """把 SQL 无法直接存储的结构转成 JSON 文本。"""
    if isinstance(value, (dict, list, tuple, set)):
        if isinstance(value, (set, tuple)):
            value = list(value)
        return json.dumps(value, ensure_ascii=False)
    return value


def normalize_rows(
    rows: list[dict[str, Any]], columns: Iterable[str]
) -> list[dict[str, Any]]:
    """按给定列顺序对齐每条记录：缺失的列补 None，多出的列丢弃。

    爬虫产出的记录可能字段参差不齐（可选字段、不同回调产出不同结构），
    直接拼 SQL 会因列数不一致而失败，所以先在这里对齐。
    """
    cols = list(columns)
    return [{c: json_safe(row.get(c)) for c in cols} for row in rows]
