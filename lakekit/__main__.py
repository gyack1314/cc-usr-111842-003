"""``python -m lakekit``：逻辑湖命令，``inventory`` 保留原始清单检查。"""

from __future__ import annotations

import argparse
import json
import sys

from .cli import main as lake_main
from .inventory import load


def inventory_main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m lakekit inventory", description="实验原始资料清单检查"
    )
    parser.add_argument("manifest")
    args = parser.parse_args(argv)
    print(json.dumps(load(args.manifest), ensure_ascii=False, indent=2))
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "inventory":
        return inventory_main(argv[1:])
    if argv and argv[0].endswith(".json") and not argv[0].startswith("-"):
        # 兼容早期入口：``python -m lakekit data/inventory.json``。
        return inventory_main(argv)
    return lake_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
