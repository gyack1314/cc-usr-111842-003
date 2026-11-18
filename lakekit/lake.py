"""带固定快照、三路合并、绑定验证和竞争安全提升的逻辑湖。

存储布局（全部位于一个显式目录下）::

    <root>/objects/<aa>/<62 位摘要>   内容寻址的 Parquet 表与快照清单
    <root>/catalog.json               主线和分支的元数据（原子替换）
    <root>/catalog.lock               进程间互斥锁

一次快照就是两张真实 Parquet 表（``observations``、``target_map``）的摘要
清单。表内容没有原地修改：每次修订都写入新对象，再原子地切换目录中的指针。
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from . import tables
from .objects import Objects

FORMAT = 1
DIGEST = re.compile(r"[0-9a-f]{64}")
BRANCH_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
RESERVED = {"main"}


class LakeError(ValueError):
    """请求不满足逻辑湖的约定。"""


class SnapshotError(LakeError):
    """快照不存在或不属于所请求的谱系。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _valid_digest(value: Any) -> bool:
    return isinstance(value, str) and bool(DIGEST.fullmatch(value))


def _branch_name(value: Any) -> str:
    if not isinstance(value, str) or not BRANCH_NAME.fullmatch(value):
        raise LakeError("分支名必须匹配 [A-Za-z0-9][A-Za-z0-9._-]{0,63}")
    if value in RESERVED:
        raise LakeError("main 是保留名称，不能作为分支名")
    return value


def insert(table: str, row: dict[str, Any]) -> dict[str, Any]:
    return {"op": "insert", "table": table, "row": row}


def update(table: str, key: str, changes: dict[str, Any]) -> dict[str, Any]:
    return {"op": "update", "table": table, "key": key, "row": changes}


def delete(table: str, key: str) -> dict[str, Any]:
    return {"op": "delete", "table": table, "key": key}


class Lake:
    """一个本地研究湖。所有公开方法返回可直接 JSON 序列化的字典。"""

    def __init__(self, root: str | os.PathLike[str]):
        self.root = Path(root)
        self.objects = Objects(self.root / "objects")
        self._thread_lock = threading.RLock()
        self._lock_depth = 0
        self._lock_descriptor: int | None = None

    # ------------------------------------------------------------------ 初始化

    @classmethod
    def initialize(
        cls,
        root: str | os.PathLike[str],
        observations: Iterable[dict[str, Any]],
        target_map: Iterable[dict[str, Any]],
    ) -> dict[str, Any]:
        """从两张表的初始内容建立主线版本 1；不会覆盖已有目录。

        初始内容本身也要通过引用核对，否则第一个快照就不是可验证对象。
        """

        lake = cls(root)
        lake.root.mkdir(parents=True, exist_ok=True)
        with lake._locked():
            if lake._catalog_exists():
                raise LakeError("目录中已有湖目录，拒绝覆盖")
            rows = {
                "observations": tables.index_rows("observations", observations),
                "target_map": tables.index_rows("target_map", target_map),
            }
            empty = {table: {} for table in tables.TABLES}
            merged, conflicts = tables.merge_tables(empty, rows, empty)
            if conflicts:
                raise LakeError(f"初始内容关联不成立: {conflicts}")
            snapshot = lake._put_snapshot(merged)
            sequence = 1
            catalog = {
                "format": FORMAT,
                "sequence": sequence,
                "main": {
                    "version": 1,
                    "snapshot": snapshot,
                    "history": [
                        {
                            "version": 1,
                            "snapshot": snapshot,
                            "created_at": _now(),
                            "message": "初始主数据",
                        }
                    ],
                },
                "branches": {},
                "validations": {},
                "attempts": [],
            }
            lake._write_catalog(catalog)
            return lake.snapshot_manifest("main")

    # ------------------------------------------------------------------ 分支

    def create_branch(
        self, name: str, snapshot: str | None = None
    ) -> dict[str, Any]:
        """从一条已记录的主线快照建立分支。

        未指定 ``snapshot`` 时固定使用当前主线快照。显式快照必须出现在主线
        历史中，不能指向分支或任意外部摘要。
        """

        _branch_name(name)
        with self._locked():
            catalog = self._read_catalog()
            if name in catalog["branches"]:
                raise LakeError(f"分支已存在: {name}")
            main = catalog["main"]
            if snapshot is None:
                snapshot = main["snapshot"]
            version = self._main_version_for(catalog, snapshot)
            if version is None:
                raise SnapshotError("分支只能从主线历史中的快照建立")
            branch = {
                "name": name,
                "base_snapshot": snapshot,
                "base_main_version": version,
                "head_snapshot": snapshot,
                "created_at": _now(),
                "revisions": [
                    {
                        "sequence": catalog["sequence"],
                        "snapshot": snapshot,
                        "parent": None,
                        "created_at": _now(),
                        "message": f"从主线 v{version} 快照建立",
                        "operations": [],
                    }
                ],
                "promotions": [],
            }
            catalog["branches"][name] = branch
            catalog["sequence"] += 1
            self._write_catalog(catalog)
            return {
                "branch": name,
                "base_snapshot": snapshot,
                "base_main_version": version,
                "head_snapshot": snapshot,
                "counts": tables.count_rows(self._load_snapshot(snapshot)),
            }

    def list_branches(self) -> list[dict[str, Any]]:
        with self._locked():
            catalog = self._read_catalog()
            result = []
            for name in sorted(catalog["branches"]):
                branch = catalog["branches"][name]
                result.append(
                    {
                        "branch": name,
                        "base_snapshot": branch["base_snapshot"],
                        "base_main_version": branch["base_main_version"],
                        "head_snapshot": branch["head_snapshot"],
                        "revisions": len(branch["revisions"]),
                        "created_at": branch["created_at"],
                    }
                )
            return result

    # ------------------------------------------------------------------ 修订

    def revise(
        self,
        branch: str,
        operations: Iterable[dict[str, Any]],
        message: str | None = None,
    ) -> dict[str, Any]:
        """在一个分支上原子地追加、修改或删除观察及映射。

        ``branch="main"`` 表示主线接收新资料。全部操作要么一起成功，要么
        分支与主线都不变。
        """

        operations = self._normalize_operations(operations)
        if not operations:
            raise LakeError("修订至少需要一个操作")
        with self._locked():
            catalog = self._read_catalog()
            if branch == "main":
                before = catalog["main"]["snapshot"]
            else:
                if branch not in catalog["branches"]:
                    raise LakeError(f"未知分支: {branch}")
                before = catalog["branches"][branch]["head_snapshot"]
            old_rows = self._load_snapshot(before)
            new_rows = self._apply_operations(old_rows, operations)
            after = self._put_snapshot(new_rows)
            change = tables.diff_tables(old_rows, new_rows)
            if branch == "main":
                version = catalog["main"]["version"] + 1
                catalog["main"]["version"] = version
                catalog["main"]["snapshot"] = after
                catalog["main"]["history"].append(
                    {
                        "version": version,
                        "snapshot": after,
                        "created_at": _now(),
                        "message": message,
                        "operations": operations,
                    }
                )
                revision = {
                    "branch": "main",
                    "version": version,
                    "snapshot": after,
                    "parent": before,
                    "changes": change,
                }
            else:
                branch_state = catalog["branches"][branch]
                revision_entry = {
                    "sequence": catalog["sequence"],
                    "snapshot": after,
                    "parent": before,
                    "created_at": _now(),
                    "message": message,
                    "operations": operations,
                }
                branch_state["revisions"].append(revision_entry)
                branch_state["head_snapshot"] = after
                revision = {
                    "branch": branch,
                    "sequence": revision_entry["sequence"],
                    "snapshot": after,
                    "parent": before,
                    "changes": change,
                }
            catalog["sequence"] += 1
            self._write_catalog(catalog)
            revision["counts"] = tables.count_rows(new_rows)
            return revision

    def preview(
        self, branch: str, operations: Iterable[dict[str, Any]]
    ) -> dict[str, Any]:
        """试算一次修订：显示真实差异及相对当前主线的合并冲突。"""

        operations = self._normalize_operations(operations)
        with self._locked():
            catalog = self._read_catalog()
            branch_state = self._require_branch(catalog, branch)
            before = branch_state["head_snapshot"]
            old_rows = self._load_snapshot(before)
            new_rows = self._apply_operations(old_rows, operations)
            base = self._load_snapshot(branch_state["base_snapshot"])
            main = self._load_snapshot(catalog["main"]["snapshot"])
            merged, conflicts = tables.merge_tables(base, new_rows, main)
            return {
                "branch": branch,
                "head_snapshot": before,
                "base_snapshot": branch_state["base_snapshot"],
                "main_snapshot": catalog["main"]["snapshot"],
                "changes": tables.diff_tables(old_rows, new_rows),
                "conflicts": conflicts,
                "counts": tables.count_rows(merged),
            }

    def apply_events(
        self, events: Iterable[dict[str, Any]], message: str | None = None
    ) -> list[dict[str, Any]]:
        """把事件文件中的动作显式应用到各自分支；事件不会被自动执行。

        事件先整体校验目标分支，然后逐分支提交。每个分支的修订仍是原子的；
        中途某个分支失败时，返回中只包含已成功的分支，异常会指出失败点。
        """

        groups: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            if not isinstance(event, dict):
                raise LakeError("事件必须是对象")
            unknown = set(event) - {"branch", "op", "table", "key", "row"}
            if unknown:
                raise LakeError(f"事件含未知字段: {sorted(unknown)}")
            branch = event.get("branch", "main")
            operation = {key: event[key] for key in ("op", "table", "key", "row") if key in event}
            groups.setdefault(branch, []).append(operation)
        with self._locked():
            catalog = self._read_catalog()
            missing = [name for name in groups if name != "main" and name not in catalog["branches"]]
            if missing:
                raise LakeError(f"事件目标分支不存在: {sorted(missing)}")
        result = []
        for branch, operations in groups.items():
            try:
                result.append(
                    self.revise(branch, operations, message=message or "应用事件")
                )
            except LakeError as exc:
                raise LakeError(
                    f"应用事件在分支 {branch} 失败；已提交: "
                    f"{[item['branch'] for item in result]}: {exc}"
                ) from exc
        return result

    # ------------------------------------------------------------------ 验证

    def validate(self, branch: str, snapshot: str | None = None) -> dict[str, Any]:
        """对两张表的合并结果重新核对，并保存绑定快照的验证记录。

        验证始终重算：分支内容、当前主线快照、三路合并、以及合并后的全部
        引用。记录同时绑定分支快照和当时准备合入的主线快照/版本，后续主线
        变化不会让记录继续授权新的合并结果。
        """

        if branch == "main":
            raise LakeError("主线不能被提升到自身")
        with self._locked():
            catalog = self._read_catalog()
            branch_state = self._require_branch(catalog, branch)
            branch_snapshot = snapshot or branch_state["head_snapshot"]
            revision_sequence = self._branch_revision(catalog, branch, branch_snapshot)
            if revision_sequence is None:
                raise SnapshotError("指定快照不在该分支的修订历史中")
            main = catalog["main"]
            base = self._load_snapshot(branch_state["base_snapshot"])
            head = self._load_snapshot(branch_snapshot)
            main_rows = self._load_snapshot(main["snapshot"])
            merged, conflicts = tables.merge_tables(base, head, main_rows)
            checks = self._merge_checks(merged, conflicts)
            passed = not conflicts and all(checks.values())
            record = {
                "validation_id": self._new_id("V", catalog),
                "branch": branch,
                "branch_snapshot": branch_snapshot,
                "branch_revision": revision_sequence,
                "branch_base_snapshot": branch_state["base_snapshot"],
                "main_snapshot": main["snapshot"],
                "main_version": main["version"],
                "created_at": _now(),
                "passed": passed,
                "checks": checks,
                "conflicts": conflicts,
                "branch_changes": tables.diff_tables(base, head),
                "main_changes": tables.diff_tables(base, main_rows),
                "counts": {
                    "base": tables.count_rows(base),
                    "branch": tables.count_rows(head),
                    "main": tables.count_rows(main_rows),
                    "merged": tables.count_rows(merged),
                },
                "merged": None,
                "merged_snapshot": None,
            }
            if passed:
                merged_snapshot = self._put_snapshot(merged)
                manifest = self._snapshot_manifest(merged_snapshot)
                record["merged"] = manifest["tables"]
                record["merged_snapshot"] = merged_snapshot
            recorded = catalog["validations"].get(record["validation_id"])
            if recorded is not None:
                return recorded
            catalog["validations"][record["validation_id"]] = record
            catalog["sequence"] += 1
            # 仅当记录确实落盘后才返回；写失败时调用方收到异常，目录不变。
            self._write_catalog(catalog)
            stored = self._read_catalog()["validations"].get(record["validation_id"])
            if stored is None:
                raise LakeError("验证记录写入后无法读回，未报告成功")
            return stored

    def revalidate(self, validation_id: str) -> dict[str, Any]:
        """主线或分支变化后重新验证；旧记录保留且不再被信任。

        所有检查都会在当前的“分支 + 主线”两张表上重跑，只复用旧记录的
        分支快照作为输入，不复用其通过结论。
        """

        with self._locked():
            catalog = self._read_catalog()
            previous = catalog["validations"].get(validation_id)
            if previous is None:
                raise LakeError(f"未知验证记录: {validation_id}")
            branch = previous["branch"]
            snapshot = previous["branch_snapshot"]
        record = self.validate(branch, snapshot)
        with self._locked():
            catalog = self._read_catalog()
            stored = catalog["validations"][record["validation_id"]]
            stored["supersedes"] = validation_id
            stored["previous_merged_snapshot"] = previous.get("merged_snapshot")
            stored["merged_content_unchanged"] = (
                previous.get("merged_snapshot") == stored.get("merged_snapshot")
            )
            self._write_catalog(catalog)
        return stored

    # ------------------------------------------------------------------ 提升

    def promote(self, validation_id: str) -> dict[str, Any]:
        """用一条验证记录提升分支；失败时保留分支并记录冲突。

        提升在互斥锁内重读目录、重新做三路合并并复核引用，然后以“主线
        快照 + 版本”为条件做一次指针替换。只有替换结果被重新读回确认后
        才返回成功；竞争中的失败者得到确定失败，可重新检查或重新验证。
        """

        with self._locked():
            catalog = self._read_catalog()
            record = catalog["validations"].get(validation_id)
            if record is None:
                raise LakeError(f"未知验证记录: {validation_id}")
            branch = record["branch"]
            branch_state = self._require_branch(catalog, branch)
            if any(
                item["validation_id"] == validation_id
                for item in branch_state["promotions"]
            ):
                raise LakeError("该验证记录已经用于提升；请重新验证后再提升")
            main = catalog["main"]
            reason = None
            details: dict[str, Any] = {}
            if not record.get("passed"):
                reason = "validation_failed"
            elif branch_state["head_snapshot"] != record["branch_snapshot"]:
                reason = "branch_changed"
                details["current_branch_snapshot"] = branch_state["head_snapshot"]
            elif main["snapshot"] != record["main_snapshot"]:
                reason = "stale_validation"
                details["current_main_snapshot"] = main["snapshot"]
                details["current_main_version"] = main["version"]
            elif main["version"] != record["main_version"]:
                reason = "stale_validation"
                details["current_main_snapshot"] = main["snapshot"]
                details["current_main_version"] = main["version"]

            base = self._load_snapshot(branch_state["base_snapshot"])
            head = self._load_snapshot(branch_state["head_snapshot"])
            main_rows = self._load_snapshot(main["snapshot"])
            merged, conflicts = tables.merge_tables(base, head, main_rows)
            checks = self._merge_checks(merged, conflicts)
            if reason is None and branch_state["head_snapshot"] == branch_state["base_snapshot"]:
                reason = "no_branch_changes"
            if reason is None and (conflicts or not all(checks.values())):
                reason = "merge_conflict" if conflicts else "integrity_failed"
            if reason is None and merged == main_rows:
                # 合并结果与当前主线完全相同（例如同一条记录已经提升过），
                # 没有需要写入的新内容，不增加一个空的主线版本。
                reason = "already_applied"
            if reason is None:
                merged_snapshot = self._put_snapshot(merged)
                if (
                    merged_snapshot != record.get("merged_snapshot")
                    or self._snapshot_manifest(merged_snapshot)["tables"]
                    != record.get("merged")
                ):
                    reason = "merged_content_changed"

            if reason is not None:
                attempt = {
                    "attempt_id": self._new_id("P", catalog),
                    "created_at": _now(),
                    "promoted": False,
                    "reason": reason,
                    "branch": branch,
                    "validation_id": validation_id,
                    "branch_snapshot": branch_state["head_snapshot"],
                    "main_snapshot": main["snapshot"],
                    "main_version": main["version"],
                    "validated_branch_snapshot": record["branch_snapshot"],
                    "validated_main_snapshot": record["main_snapshot"],
                    "validated_main_version": record["main_version"],
                    "checks": checks,
                    "conflicts": conflicts,
                    "details": details,
                    "branch_changes": tables.diff_tables(base, head),
                    "main_changes": tables.diff_tables(base, main_rows),
                }
                catalog["attempts"].append(attempt)
                catalog["sequence"] += 1
                self._write_catalog(catalog)
                return attempt

            version = main["version"] + 1
            catalog["main"]["version"] = version
            catalog["main"]["snapshot"] = merged_snapshot
            catalog["main"]["history"].append(
                {
                    "version": version,
                    "snapshot": merged_snapshot,
                    "created_at": _now(),
                    "message": f"提升分支 {branch}，验证 {validation_id}",
                    "validation_id": validation_id,
                }
            )
            catalog["branches"][branch]["promotions"].append(
                {
                    "validation_id": validation_id,
                    "main_version": version,
                    "main_snapshot": merged_snapshot,
                    "created_at": _now(),
                }
            )
            attempt = {
                "attempt_id": self._new_id("P", catalog),
                "created_at": _now(),
                "promoted": True,
                "reason": None,
                "branch": branch,
                "validation_id": validation_id,
                "branch_snapshot": record["branch_snapshot"],
                "previous_main_snapshot": main["snapshot"],
                "main_snapshot": merged_snapshot,
                "main_version": version,
                "checks": checks,
                "conflicts": [],
                "applied_changes": tables.diff_tables(main_rows, merged),
                "main_changes_since_fork": tables.diff_tables(
                    base, main_rows
                ),
            }
            catalog["attempts"].append(attempt)
            catalog["sequence"] += 1
            self._write_catalog(catalog)
            confirmed = self._read_catalog()
            if (
                confirmed["main"]["snapshot"] != merged_snapshot
                or confirmed["main"]["version"] != version
            ):
                raise LakeError("提升指针替换后无法确认主线快照，未报告成功")
            return attempt

    # ------------------------------------------------------------------ 查询

    def snapshot_manifest(
        self, branch: str = "main", snapshot: str | None = None
    ) -> dict[str, Any]:
        """返回一个固定快照的两张表摘要与行数，不加载全部行。"""

        with self._locked():
            catalog = self._read_catalog()
            resolved = self._resolve_snapshot(catalog, branch, snapshot)
            manifest = self._snapshot_manifest(resolved)
        return {
            "branch": branch,
            "snapshot": resolved,
            "tables": manifest["tables"],
            "counts": {
                table: self._snapshot_row_count(manifest["tables"][table])
                for table in tables.TABLES
            },
        }

    def show(
        self,
        branch: str = "main",
        table: str | None = None,
        snapshot: str | None = None,
    ) -> dict[str, Any]:
        """显示固定快照中的真实行；``table`` 为空时返回两张表。"""

        if table is not None and table not in tables.TABLES:
            raise LakeError(f"未知表: {table}")
        with self._locked():
            catalog = self._read_catalog()
            resolved = self._resolve_snapshot(catalog, branch, snapshot)
            rows = self._load_snapshot(resolved)
        selected = [table] if table else list(tables.TABLES)
        return {
            "branch": branch,
            "snapshot": resolved,
            "tables": {
                name: [rows[name][key] for key in sorted(rows[name])]
                for name in selected
            },
        }

    def query(
        self,
        sql: str,
        branch: str = "main",
        snapshot: str | None = None,
        parameters: list[Any] | tuple[Any, ...] | None = None,
    ) -> dict[str, Any]:
        """用 DuckDB 查询一个固定快照的两张真实 Parquet 表。

        快照在查询开始前解析成不可变对象；即使主线随后继续接收资料，本次
        查询仍只看到解析时的内容。两条谱线都严格按摘要在目录里解析：显式
        快照必须属于所查询谱线，不能拿任意摘要当作主线版本。
        """

        if not isinstance(sql, str) or not sql.strip():
            raise LakeError("SQL 不能为空")
        with self._locked():
            catalog = self._read_catalog()
            resolved = self._resolve_snapshot(catalog, branch, snapshot)
            manifest = self._snapshot_manifest(resolved)
        connection = duckdb.connect(":memory:")
        try:
            for name in tables.TABLES:
                path = self.objects.path(manifest["tables"][name])
                if not path.is_file():
                    raise SnapshotError(f"快照缺少表对象: {name}")
                if tables.digest(path.read_bytes()) != manifest["tables"][name]:
                    raise SnapshotError(f"快照表对象摘要不符: {name}")
                literal = str(path).replace("'", "''")
                connection.execute(
                    f'CREATE VIEW "{name}" AS SELECT * FROM read_parquet(\'{literal}\')'
                )
            cursor = connection.execute(sql, parameters) if parameters else connection.execute(sql)
            columns = [item[0] for item in cursor.description]
            result_rows = [
                dict(zip(columns, row)) for row in cursor.fetchall()
            ]
        finally:
            connection.close()
        return {
            "branch": branch,
            "snapshot": resolved,
            "columns": columns,
            "rows": result_rows,
        }

    # ------------------------------------------------------------------ 差异与状态

    def diff(self, branch: str, against: str = "base") -> dict[str, Any]:
        """列出真实行级差异。

        ``against="base"`` 比较分支与分叉快照（分支自己的修改）；
        ``against="main"`` 比较当前主线与分支头。
        """

        if against not in {"base", "main"}:
            raise LakeError("against 只能是 base 或 main")
        with self._locked():
            catalog = self._read_catalog()
            branch_state = self._require_branch(catalog, branch)
            if against == "base":
                left_snapshot = branch_state["base_snapshot"]
            else:
                left_snapshot = catalog["main"]["snapshot"]
            right_snapshot = branch_state["head_snapshot"]
            left = self._load_snapshot(left_snapshot)
            right = self._load_snapshot(right_snapshot)
            base = self._load_snapshot(branch_state["base_snapshot"])
            main = self._load_snapshot(catalog["main"]["snapshot"])
            _, conflicts = tables.merge_tables(base, right, main)
        return {
            "branch": branch,
            "against": against,
            "left_snapshot": left_snapshot,
            "right_snapshot": right_snapshot,
            "changes": tables.diff_tables(left, right),
            "conflicts": conflicts,
        }

    def status(self, branch: str | None = None) -> dict[str, Any]:
        """显示主线、分支、差异、冲突和验证/提升记录。"""

        with self._locked():
            catalog = self._read_catalog()
            main = catalog["main"]
            result: dict[str, Any] = {
                "main": {
                    "version": main["version"],
                    "snapshot": main["snapshot"],
                    "counts": tables.count_rows(
                        self._load_snapshot(main["snapshot"])
                    ),
                },
                "branches": {},
                "validations": [],
                "attempts": [],
            }
            selected = [branch] if branch else sorted(catalog["branches"])
            for name in selected:
                state = self._require_branch(catalog, name)
                base = self._load_snapshot(state["base_snapshot"])
                head = self._load_snapshot(state["head_snapshot"])
                main_rows = self._load_snapshot(main["snapshot"])
                merged, conflicts = tables.merge_tables(base, head, main_rows)
                result["branches"][name] = {
                    "base_snapshot": state["base_snapshot"],
                    "base_main_version": state["base_main_version"],
                    "head_snapshot": state["head_snapshot"],
                    "revisions": len(state["revisions"]),
                    "promotions": state["promotions"],
                    "counts": tables.count_rows(head),
                    "branch_changes": tables.diff_tables(base, head),
                    "main_changes_since_fork": tables.diff_tables(base, main_rows),
                    "conflicts": conflicts,
                    "merged_counts": tables.count_rows(merged),
                }
                result["validations"].extend(
                    record
                    for record in catalog["validations"].values()
                    if record["branch"] == name
                )
                result["attempts"].extend(
                    attempt for attempt in catalog["attempts"] if attempt["branch"] == name
                )
        result["validations"].sort(key=lambda item: item["validation_id"])
        result["attempts"].sort(key=lambda item: item["attempt_id"])
        return result

    def history(self, branch: str) -> dict[str, Any]:
        """返回主线的版本历史或一个分支的修订历史与验证记录。"""

        with self._locked():
            catalog = self._read_catalog()
            if branch == "main":
                return {
                    "branch": "main",
                    "history": catalog["main"]["history"],
                }
            state = self._require_branch(catalog, branch)
            validations = [
                record
                for record in catalog["validations"].values()
                if record["branch"] == branch
            ]
            attempts = [
                attempt
                for attempt in catalog["attempts"]
                if attempt["branch"] == branch
            ]
            return {
                "branch": branch,
                "base_snapshot": state["base_snapshot"],
                "base_main_version": state["base_main_version"],
                "head_snapshot": state["head_snapshot"],
                "revisions": state["revisions"],
                "promotions": state["promotions"],
                "validations": sorted(validations, key=lambda item: item["validation_id"]),
                "attempts": sorted(attempts, key=lambda item: item["attempt_id"]),
            }

    # ------------------------------------------------------------------ 存储细节

    def _catalog_exists(self) -> bool:
        return (self.root / "catalog.json").exists()

    @contextmanager
    def _locked(self):
        """进程间互斥；同一实例内的嵌套调用复用同一把锁。

        ``flock`` 在已持有锁的文件描述符上不可重入，因此同一实例重入时只
        增加深度。不同进程（或同一进程的不同实例）仍然串行。
        """

        with self._thread_lock:
            if self._lock_depth == 0:
                self.root.mkdir(parents=True, exist_ok=True)
                descriptor = os.open(
                    self.root / "catalog.lock", os.O_CREAT | os.O_RDWR, 0o600
                )
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                except BaseException:
                    os.close(descriptor)
                    raise
                self._lock_descriptor = descriptor
            self._lock_depth += 1
            try:
                yield
            finally:
                self._lock_depth -= 1
                if self._lock_depth == 0:
                    descriptor = self._lock_descriptor
                    self._lock_descriptor = None
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    finally:
                        os.close(descriptor)

    def _read_catalog(self) -> dict[str, Any]:
        path = self.root / "catalog.json"
        if not path.is_file():
            raise LakeError("湖目录尚未初始化")
        try:
            catalog = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LakeError(f"目录元数据不可读: {exc}") from exc
        if catalog.get("format") != FORMAT:
            raise LakeError("不支持的目录格式")
        for key in ("main", "branches", "validations", "attempts", "sequence"):
            if key not in catalog:
                raise LakeError("目录元数据不完整")
        return catalog

    def _write_catalog(self, catalog: dict[str, Any]) -> None:
        data = _canonical(catalog)
        descriptor, temporary = tempfile.mkstemp(
            dir=self.root, prefix=".catalog-", suffix=".json"
        )
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.root / "catalog.json")
            directory = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def _put_snapshot(self, rows: dict[str, dict[str, Any]]) -> str:
        digests = {}
        for table in tables.TABLES:
            data = tables.encode(table, rows[table].values())
            digests[table] = self.objects.put(data)
        manifest = {"format": FORMAT, "tables": digests}
        return self.objects.put(_canonical(manifest))

    def _load_snapshot(self, snapshot: str) -> dict[str, dict[str, Any]]:
        manifest = self._snapshot_manifest(snapshot)
        result = {}
        for table in tables.TABLES:
            data = self.objects.read(manifest["tables"][table])
            result[table] = {
                row[tables.primary_key(table)]: row
                for row in tables.decode(table, data)
            }
        return result

    def _snapshot_manifest(self, snapshot: str) -> dict[str, Any]:
        if not _valid_digest(snapshot):
            raise SnapshotError("快照摘要格式错误")
        try:
            raw = self.objects.read(snapshot)
        except (OSError, ValueError) as exc:
            raise SnapshotError(f"快照不存在或内容损坏: {exc}") from exc
        try:
            manifest = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SnapshotError("快照清单不是有效 JSON") from exc
        if manifest.get("format") != FORMAT or set(manifest.get("tables", {})) != set(
            tables.TABLES
        ):
            raise SnapshotError("快照清单不包含预期的两张表")
        for table in tables.TABLES:
            if not _valid_digest(manifest["tables"][table]):
                raise SnapshotError(f"快照表摘要格式错误: {table}")
        return manifest

    def _snapshot_row_count(self, digest: str) -> int:
        try:
            raw = self.objects.read(digest)
        except (OSError, ValueError) as exc:
            raise SnapshotError(f"表对象不存在或损坏: {exc}") from exc
        return pq.read_metadata(pa.BufferReader(raw)).num_rows

    def _main_version_for(self, catalog: dict[str, Any], snapshot: str) -> int | None:
        if not isinstance(snapshot, str):
            return None
        for entry in reversed(catalog["main"]["history"]):
            if entry["snapshot"] == snapshot:
                return entry["version"]
        return None

    def _branch_revision(
        self, catalog: dict[str, Any], branch: str, snapshot: str
    ) -> int | None:
        for entry in catalog["branches"][branch]["revisions"]:
            if entry["snapshot"] == snapshot:
                return entry["sequence"]
        return None

    def _resolve_snapshot(
        self, catalog: dict[str, Any], branch: str, snapshot: str | None
    ) -> str:
        """把查询请求解析为目录中真实存在的快照。

        未指定摘要时读取分支当前头；显式摘要必须在目录里存在，并且属于所
        查询的分支谱系（主线历史、分支修订，或该分支的合并验证结果）。
        """

        if branch == "main":
            lineage = [entry["snapshot"] for entry in catalog["main"]["history"]]
            current = catalog["main"]["snapshot"]
        else:
            state = self._require_branch(catalog, branch)
            lineage = [entry["snapshot"] for entry in state["revisions"]]
            lineage.extend(
                record["merged_snapshot"]
                for record in catalog["validations"].values()
                if record["branch"] == branch and record.get("merged_snapshot")
            )
            current = state["head_snapshot"]
        if snapshot is None:
            return current
        if snapshot not in lineage:
            raise SnapshotError("指定快照不在该分支的可查询历史中")
        return snapshot

    def _require_branch(
        self, catalog: dict[str, Any], branch: str
    ) -> dict[str, Any]:
        if branch == "main" or branch not in catalog["branches"]:
            raise LakeError(f"未知分支: {branch}")
        return catalog["branches"][branch]

    def _normalize_operations(
        self, operations: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if isinstance(operations, (str, bytes, dict)):
            raise LakeError("operations 必须是操作对象的列表")
        result = []
        for operation in operations:
            if not isinstance(operation, dict):
                raise LakeError("操作必须是对象")
            op = operation.get("op")
            table = operation.get("table")
            if op not in {"insert", "update", "delete"}:
                raise LakeError(f"未知操作: {op!r}")
            if table not in tables.TABLES:
                raise LakeError(f"未知表: {table!r}")
            allowed = {"op", "table"}
            if op == "insert":
                allowed.add("row")
                if not isinstance(operation.get("row"), dict):
                    raise LakeError("insert 需要 row 对象")
            elif op == "update":
                allowed.update({"key", "row"})
                if not isinstance(operation.get("key"), str) or not operation["key"]:
                    raise LakeError("update 需要字符串 key")
                if not isinstance(operation.get("row"), dict):
                    raise LakeError("update 需要 row 对象")
            else:
                allowed.add("key")
                if not isinstance(operation.get("key"), str) or not operation["key"]:
                    raise LakeError("delete 需要字符串 key")
            unknown = set(operation) - allowed
            if unknown:
                raise LakeError(f"操作含未知字段: {sorted(unknown)}")
            result.append({key: operation[key] for key in operation})
        return result

    def _apply_operations(
        self,
        rows: dict[str, dict[str, Any]],
        operations: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        working = {
            table: {key: dict(row) for key, row in rows[table].items()}
            for table in tables.TABLES
        }
        for operation in operations:
            table = operation["table"]
            key_name = tables.primary_key(table)
            if operation["op"] == "insert":
                row = tables.normalize_row(table, operation["row"])
                key = row[key_name]
                if key in working[table]:
                    raise LakeError(f"{table} 主键已存在: {key}")
                working[table][key] = row
            elif operation["op"] == "update":
                key = operation["key"]
                if key not in working[table]:
                    raise LakeError(f"{table} 主键不存在: {key}")
                changes = dict(operation["row"])
                if key_name in changes and changes[key_name] != key:
                    raise LakeError("不能用 update 改变主键；请删除后插入")
                changes.pop(key_name, None)
                candidate = dict(working[table][key])
                candidate.update(changes)
                working[table][key] = tables.normalize_row(table, candidate)
            else:
                key = operation["key"]
                if key not in working[table]:
                    raise LakeError(f"{table} 主键不存在: {key}")
                del working[table][key]
        return working

    def _merge_checks(
        self,
        merged: dict[str, dict[str, Any]],
        conflicts: list[dict[str, Any]],
    ) -> dict[str, bool]:
        """在合并后的两张表上做与验证记录一一对应的检查。"""

        try:
            roundtrip = all(
                {
                    row[tables.primary_key(table)]: row
                    for row in tables.decode(
                        table, tables.encode(table, merged[table].values())
                    )
                }
                == merged[table]
                for table in tables.TABLES
            )
        except tables.TableError:
            roundtrip = False
        map_keys = set(merged["target_map"])
        resolved = all(
            row["map_key"] in map_keys for row in merged["observations"].values()
        )
        unique = len(map_keys) == len(merged["target_map"]) and len(
            merged["observations"]
        ) == len(
            {row["observation_id"] for row in merged["observations"].values()}
        )
        return {
            "parquet_roundtrip": roundtrip,
            "merge_conflicts": not conflicts,
            "references_resolved": resolved,
            "unique_keys": unique,
        }

    def _new_id(self, prefix: str, catalog: dict[str, Any]) -> str:
        return f"{prefix}{catalog['sequence']:06d}-{uuid.uuid4().hex[:8]}"
