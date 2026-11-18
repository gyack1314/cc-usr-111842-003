"""本地研究湖的命令行入口。

所有命令都针对一个显式目录操作，并且只报告目录中实际存在的快照、差异、
冲突和记录。示例（从工程根目录）::

    python -m lakekit --root .local/lake init --seed data/seed.json
    python -m lakekit --root .local/lake branch create review
    python -m lakekit --root .local/lake apply-events data/events.json
    python -m lakekit --root .local/lake diff review
    python -m lakekit --root .local/lake validate review
    python -m lakekit --root .local/lake promote V000002-xxxxxxxx
    python -m lakekit --root .local/lake query review --sql "SELECT * FROM target_map"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .lake import Lake, LakeError, delete, insert, update
from .tables import TableError


def _json(value):
    if not isinstance(value, str):
        raise LakeError("JSON 参数必须是字符串")
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise LakeError(f"JSON 参数无法解析: {exc}") from exc


def _operations(args):
    operations = []
    for table, row in args.insert or []:
        value = _json(row)
        if not isinstance(value, dict):
            raise LakeError("insert 的 ROW_JSON 必须是对象")
        operations.append(insert(table, value))
    for table, key, changes in args.update or []:
        value = _json(changes)
        if not isinstance(value, dict):
            raise LakeError("update 的 CHANGES_JSON 必须是对象")
        operations.append(update(table, key, value))
    for table, key in args.delete or []:
        operations.append(delete(table, key))
    return operations


def _read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LakeError(f"JSON 文件不可读: {path}: {exc}") from exc


def _operation_list(path):
    data = _read_json(path)
    if not isinstance(data, list):
        raise LakeError("操作文件必须是 JSON 数组")
    return data


def build_parser():
    parser = argparse.ArgumentParser(
        prog="lakekit",
        description="靶点证据湖：固定快照、分支修订、关联验证与安全提升",
    )
    parser.add_argument(
        "--root",
        default=".local/lake",
        help="湖目录（默认 .local/lake）；init 要求该目录不存在或为空",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="从种子内容建立主线版本 1")
    init.add_argument("--seed", default="data/seed.json", help="含 observations 与 target_map 的 JSON")
    init.add_argument("--observations", help="仅 observations 的 JSON 数组，会覆盖 --seed")
    init.add_argument("--target-map", help="仅 target_map 的 JSON 数组，会覆盖 --seed")

    branch = commands.add_parser("branch", help="分支操作")
    branch_commands = branch.add_subparsers(dest="branch_command", required=True)
    create = branch_commands.add_parser("create", help="从一条明确的主线快照建立分支")
    create.add_argument("name")
    create.add_argument(
        "--snapshot",
        help="主线历史中的快照摘要；省略时固定当前主线快照",
    )
    branch_commands.add_parser("list", help="列出分支")

    revise = commands.add_parser("revise", help="在分支（或 main）上提交修订")
    revise.add_argument("branch")
    revise.add_argument("--insert", nargs=2, metavar=("TABLE", "ROW_JSON"), action="append")
    revise.add_argument(
        "--update", nargs=3, metavar=("TABLE", "KEY", "CHANGES_JSON"), action="append"
    )
    revise.add_argument("--delete", nargs=2, metavar=("TABLE", "KEY"), action="append")
    revise.add_argument("--operations", help="操作 JSON 数组文件")
    revise.add_argument("--message")

    apply_events = commands.add_parser(
        "apply-events", help="显式应用事件文件（事件不会自动执行）"
    )
    apply_events.add_argument("events")
    apply_events.add_argument("--message")

    preview = commands.add_parser("preview", help="试算修订，不写入目录")
    preview.add_argument("branch")
    preview.add_argument("--operations", required=True, help="操作 JSON 数组文件")

    validate = commands.add_parser("validate", help="验证合并结果并保存绑定记录")
    validate.add_argument("branch")
    validate.add_argument("--snapshot", help="分支历史中的快照；默认分支头")

    revalidate = commands.add_parser("revalidate", help="按当前主线重新验证旧记录")
    revalidate.add_argument("validation_id")

    promote = commands.add_parser("promote", help="按验证记录提升分支")
    promote.add_argument("validation_id")

    show = commands.add_parser("show", help="显示固定快照中的真实行")
    show.add_argument("branch", nargs="?", default="main")
    show.add_argument("--table", choices=["observations", "target_map"])
    show.add_argument("--snapshot")

    query = commands.add_parser("query", help="用 DuckDB 查询固定快照")
    query.add_argument("branch", nargs="?", default="main")
    query.add_argument("--snapshot")
    query.add_argument("--sql", required=True)
    query.add_argument(
        "--param",
        action="append",
        default=[],
        help="位置参数；每次出现代表一个 DuckDB 位置参数（字符串）",
    )

    manifest = commands.add_parser("snapshot", help="显示固定快照摘要和行数")
    manifest.add_argument("branch", nargs="?", default="main")
    manifest.add_argument("--snapshot")

    diff = commands.add_parser("diff", help="显示分支的真实行级差异")
    diff.add_argument("branch")
    diff.add_argument("--against", choices=["base", "main"], default="base")

    status = commands.add_parser("status", help="显示主线、分支、冲突与记录")
    status.add_argument("branch", nargs="?")

    history = commands.add_parser("history", help="显示主线版本或分支修订历史")
    history.add_argument("branch", nargs="?", default="main")

    return parser


def run(args):
    lake = Lake(args.root)
    if args.command == "init":
        if args.observations or args.target_map:
            observations = _read_json(args.observations) if args.observations else []
            target_map = _read_json(args.target_map) if args.target_map else []
            if not isinstance(observations, list) or not isinstance(target_map, list):
                raise LakeError("--observations 与 --target-map 必须是 JSON 数组")
        else:
            seed = _read_json(args.seed)
            if not isinstance(seed, dict) or "observations" not in seed or "target_map" not in seed:
                raise LakeError("种子文件必须包含 observations 与 target_map")
            observations = seed["observations"]
            target_map = seed["target_map"]
        return Lake.initialize(args.root, observations, target_map)
    if args.command == "branch":
        if args.branch_command == "create":
            return lake.create_branch(args.name, args.snapshot)
        return lake.list_branches()
    if args.command == "revise":
        operations = _operations(args)
        if args.operations:
            operations = _operation_list(args.operations) + operations
        return lake.revise(args.branch, operations, args.message)
    if args.command == "apply-events":
        events = _read_json(args.events)
        if not isinstance(events, list):
            raise LakeError("事件文件必须是 JSON 数组")
        return lake.apply_events(events, args.message)
    if args.command == "preview":
        return lake.preview(args.branch, _operation_list(args.operations))
    if args.command == "validate":
        return lake.validate(args.branch, args.snapshot)
    if args.command == "revalidate":
        return lake.revalidate(args.validation_id)
    if args.command == "promote":
        return lake.promote(args.validation_id)
    if args.command == "show":
        return lake.show(args.branch, args.table, args.snapshot)
    if args.command == "query":
        return lake.query(args.sql, args.branch, args.snapshot, args.param or None)
    if args.command == "snapshot":
        return lake.snapshot_manifest(args.branch, args.snapshot)
    if args.command == "diff":
        return lake.diff(args.branch, args.against)
    if args.command == "status":
        return lake.status(args.branch)
    if args.command == "history":
        return lake.history(args.branch)
    raise LakeError(f"未知命令: {args.command}")


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run(args)
    except (LakeError, TableError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
