"""逻辑表定义、规范化编码与三路行合并。

湖里只有两张表，并且它们共同构成一个验证对象：

``observations``
    ``observation_id`` 是主键，``map_key`` 显式绑定一个映射键，``score``
    是可空的不透明字符串。

``target_map``
    ``map_key`` 是主键。映射键由 ``source_target`` 与
    ``mapping_version`` 两个不含冒号的分量组成，写成一个不透明字符串
    ``source_target:mapping_version``。``canonical_target`` 只是映射内容，
    不能参与匹配或去重。

所有值按 Parquet 中的字符串保存。空值只允许出现在非主键列（例如
``observations.score``、``target_map.canonical_target``）；读取时既校验
对象摘要，也重新检查主键与映射键格式。显示名（``canonical_target``）不参与
映射键匹配，因此相同显示名的不同版本仍是两个不同的映射键。
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

TABLES = ("observations", "target_map")

SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "observations": (
        ("observation_id", "string"),
        ("map_key", "string"),
        ("score", "string"),
    ),
    "target_map": (
        ("map_key", "string"),
        ("canonical_target", "string"),
    ),
}

PRIMARY_KEYS: dict[str, str] = {
    "observations": "observation_id",
    "target_map": "map_key",
}

ROW = dict[str, str | None]


class TableError(ValueError):
    """表内容不符合逻辑表约定。"""


def schema_fields(table: str) -> tuple[str, ...]:
    return tuple(name for name, _ in SCHEMAS[table])


def primary_key(table: str) -> str:
    return PRIMARY_KEYS[table]


def arrow_schema(table: str) -> pa.Schema:
    return pa.schema([(name, pa.string()) for name, _ in SCHEMAS[table]])


def split_map_key(value: str) -> tuple[str, str]:
    """把 ``source_target:mapping_version`` 拆成两个非空分量。

    两个分量都不含冒号，因此整个映射键必须恰好有一个分隔冒号。显示名
    （``canonical_target``）没有参与这里，也不能代替映射键。
    """

    if not isinstance(value, str):
        raise TableError("map_key 必须是字符串")
    parts = value.split(":")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise TableError(
            "map_key 必须写成 source_target:mapping_version，且两个分量非空、不含冒号"
        )
    return parts[0], parts[1]


def map_key(source_target: str, mapping_version: str) -> str:
    """由两个分量构造映射键；显示名不参与。"""

    if not isinstance(source_target, str) or not isinstance(mapping_version, str):
        raise TableError("映射键分量必须是字符串")
    if ":" in source_target or ":" in mapping_version:
        raise TableError("映射键分量不能含冒号")
    if not source_target or not mapping_version:
        raise TableError("映射键分量不能为空")
    return f"{source_target}:{mapping_version}"


def normalize_row(table: str, row: dict[str, Any]) -> ROW:
    """按逻辑表模式规范化一行；拒绝未知列和缺失主键。"""

    if not isinstance(row, dict):
        raise TableError(f"{table} 的行必须是对象")
    known = set(schema_fields(table))
    unknown = set(row) - known
    if unknown:
        raise TableError(f"{table} 含未知列: {sorted(unknown)}")
    result: ROW = {}
    for name in schema_fields(table):
        value = row.get(name)
        if isinstance(value, str):
            result[name] = value
        elif value is None:
            result[name] = None
        else:
            raise TableError(f"{table}.{name} 必须是字符串或空值")
    key_name = primary_key(table)
    key = result[key_name]
    if not key:
        raise TableError(f"{table}.{key_name} 不能为空")
    if table == "observations":
        if result["map_key"] is None:
            raise TableError("observations.map_key 必须显式绑定映射键")
        split_map_key(result["map_key"])
    else:
        split_map_key(result["map_key"])
    return result


def row_sort_key(table: str, row: ROW) -> str:
    return row[primary_key(table)] or ""


def index_rows(table: str, rows: Iterable[dict[str, Any]]) -> dict[str, ROW]:
    """按主键建立索引，并拒绝重复主键。"""

    result: dict[str, ROW] = {}
    for raw in rows:
        row = normalize_row(table, raw)
        key = row_sort_key(table, row)
        if key in result:
            raise TableError(f"{table} 主键重复: {key}")
        result[key] = row
    return result


def encode(table: str, rows: Iterable[dict[str, Any]]) -> bytes:
    """把行编码为按主键排序的 Parquet 字节。"""

    indexed = index_rows(table, rows)
    ordered = [indexed[key] for key in sorted(indexed)]
    table_data = pa.Table.from_pylist(ordered, schema=arrow_schema(table))
    sink = pa.BufferOutputStream()
    pq.write_table(table_data, sink)
    return sink.getvalue().to_pybytes()


def decode(table: str, data: bytes) -> list[ROW]:
    """解析 Parquet 字节并重新校验逻辑表约定。"""

    source = pa.BufferReader(data)
    try:
        result = pq.read_table(source, schema=arrow_schema(table))
    except (pa.ArrowException, ValueError) as exc:
        raise TableError(f"{table} 不是可读的 Parquet 表: {exc}") from exc
    return list(index_rows(table, result.to_pylist()).values())


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def merge_tables(
    base: dict[str, dict[str, ROW]],
    branch: dict[str, dict[str, ROW]],
    main: dict[str, dict[str, ROW]],
) -> tuple[dict[str, dict[str, ROW]], list[dict[str, Any]]]:
    """对两张表做三路合并，返回合并结果与结构化冲突。

    合并以显式主键为单位：``observation_id`` 或 ``map_key``。同一主键在两边
    产生不同最终内容、一边删除另一边修改、或两边新增不同内容都记为冲突。
    两边最终内容完全相同时不冲突。映射键是 ``source_target`` 与
    ``mapping_version`` 的组合，``canonical_target`` 的显示值不参与匹配。

    主键合并没有冲突并不代表结果可用：观察通过 ``map_key`` 引用映射。合并后
    还要对两张表整体复核，任何观察都必须能追到唯一的映射行。
    """

    merged: dict[str, dict[str, ROW]] = {}
    conflicts: list[dict[str, Any]] = []

    for table in TABLES:
        base_rows = base[table]
        branch_rows = branch[table]
        main_rows = main[table]
        out: dict[str, ROW] = {}
        keys = sorted(set(base_rows) | set(branch_rows) | set(main_rows))
        for key in keys:
            before = base_rows.get(key)
            after_branch = branch_rows.get(key)
            after_main = main_rows.get(key)
            branch_changed = before != after_branch
            main_changed = before != after_main

            if not branch_changed:
                choice = after_main
            elif not main_changed:
                choice = after_branch
            elif after_branch == after_main:
                # 两边得到完全相同的最终内容，可以合并。
                choice = after_branch
            elif after_branch is None:
                conflicts.append(
                    _conflict(
                        table,
                        key,
                        "delete_vs_modify",
                        "分支删除了该行，主线修改了同一行",
                        before,
                        after_branch,
                        after_main,
                    )
                )
                continue
            elif after_main is None:
                conflicts.append(
                    _conflict(
                        table,
                        key,
                        "delete_vs_modify",
                        "主线删除了该行，分支修改了同一行",
                        before,
                        after_branch,
                        after_main,
                    )
                )
                continue
            elif before is None and after_branch is not None and after_main is not None:
                conflicts.append(
                    _conflict(
                        table,
                        key,
                        "both_added",
                        "主线与分支新增了同一主键但内容不同",
                        before,
                        after_branch,
                        after_main,
                    )
                )
                continue
            else:
                conflicts.append(
                    _conflict(
                        table,
                        key,
                        "both_modified",
                        "主线与分支从同一快照出发把该行改成不同内容",
                        before,
                        after_branch,
                        after_main,
                    )
                )
                continue

            if choice is not None:
                out[key] = choice
        merged[table] = out

    conflicts.extend(_reference_conflicts(merged))
    conflicts.sort(
        key=lambda item: (
            item["table"],
            item["key"],
            item["kind"],
        )
    )
    return merged, conflicts


def _conflict(
    table: str,
    key: str,
    kind: str,
    message: str,
    base: ROW | None,
    branch: ROW | None,
    main: ROW | None,
) -> dict[str, Any]:
    return {
        "table": table,
        "key": key,
        "kind": kind,
        "message": message,
        "base": base,
        "branch": branch,
        "main": main,
    }


def _reference_conflicts(
    merged: dict[str, dict[str, ROW]],
) -> list[dict[str, Any]]:
    """在合并后的两张表上复核观察与映射的关联。"""

    map_keys = set(merged["target_map"])
    missing: dict[str, list[str]] = {}
    for observation_id, row in merged["observations"].items():
        reference = row["map_key"]
        if reference is None or reference not in map_keys:
            missing.setdefault(reference or "", []).append(observation_id)
    result = []
    for reference in sorted(missing):
        result.append(
            {
                "table": "observations",
                "key": reference,
                "kind": "dangling_reference",
                "message": (
                    "合并结果中这些观察引用的映射键不存在；"
                    "每个观察都必须能追到唯一映射行"
                ),
                "observation_ids": sorted(missing[reference]),
                "base": None,
                "branch": None,
                "main": None,
            }
        )
    return result


def diff_tables(
    before: dict[str, dict[str, ROW]],
    after: dict[str, dict[str, ROW]],
) -> list[dict[str, Any]]:
    """列出两个快照之间两张表的真实行级差异。"""

    changes = []
    for table in TABLES:
        keys = sorted(set(before[table]) | set(after[table]))
        for key in keys:
            old = before[table].get(key)
            new = after[table].get(key)
            if old == new:
                continue
            if old is None:
                action = "insert"
            elif new is None:
                action = "delete"
            else:
                action = "update"
            changes.append(
                {
                    "table": table,
                    "key": key,
                    "action": action,
                    "before": old,
                    "after": new,
                }
            )
    return changes


def count_rows(tables: dict[str, dict[str, ROW]]) -> dict[str, int]:
    return {table: len(tables[table]) for table in TABLES}
