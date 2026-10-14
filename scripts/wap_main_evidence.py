"""Capture and compare the five main tables around an Airflow WAP fault-injection run."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

PRIMARY_TABLES = {
    "bronze": ("bronze.orders_raw", "bronze.customers_raw"),
    "silver": ("silver.orders", "silver.customers"),
    "gold": ("gold.daily_order_metrics",),
}
MAIN_FIELDS = ("main_snapshot", "main_rows")


def main_fields(data: object) -> dict[str, dict[str, int]]:
    """Validate complete observations, retaining only main snapshot ids and row counts."""
    if not isinstance(data, dict) or set(data) != set(PRIMARY_TABLES):
        raise ValueError("status evidence must contain bronze, silver and gold")
    result: dict[str, dict[str, int]] = {}
    for layer, names in PRIMARY_TABLES.items():
        state = data[layer]
        tables = state.get("tables") if isinstance(state, dict) else None
        if not isinstance(tables, dict) or set(tables) != set(names):
            raise ValueError(f"{layer} status must contain every primary table")
        for name in names:
            table = tables[name]
            if not isinstance(table, dict):
                raise ValueError(f"invalid status for {name}")
            fields: dict[str, int] = {}
            for field in MAIN_FIELDS:
                value = table.get(field)
                if type(value) is not int or value < 0 or (field == "main_snapshot" and value == 0):
                    raise ValueError(f"invalid {field} for {name}")
                fields[field] = value
            result[name] = fields
    return result


def capture(path: Path) -> None:
    statuses: dict[str, object] = {}
    for layer in PRIMARY_TABLES:
        result = subprocess.run(
            ["uv", "run", "--frozen", "soda-dq", "wap", "status", layer],
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        if result.returncode != 0:
            raise ValueError(f"{layer} status failed (exit {result.returncode}): {result.stderr.strip()}")
        statuses[layer] = json.loads(result.stdout)
    main_fields(statuses)
    path.write_text(json.dumps(statuses, indent=2) + "\n", encoding="utf-8")
    print(f"Captured main snapshots and rows for five tables: {path}")


def compare(before: Path, after: Path) -> None:
    initial = main_fields(json.loads(before.read_text(encoding="utf-8")))
    final = main_fields(json.loads(after.read_text(encoding="utf-8")))
    changed = [name for name in initial if initial[name] != final[name]]
    if changed:
        raise ValueError(f"Airflow WAP bad run changed main snapshots or rows: {', '.join(changed)}")
    print("Airflow WAP bad run preserved all five main snapshot ids and row counts")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture_parser = commands.add_parser("capture")
    capture_parser.add_argument("path", type=Path)
    compare_parser = commands.add_parser("compare")
    compare_parser.add_argument("before", type=Path)
    compare_parser.add_argument("after", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "capture":
            capture(args.path)
        else:
            compare(args.before, args.after)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"WAP main evidence error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
