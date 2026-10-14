"""Require fault injection to fail the bronze gate, with downstream tasks blocked."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

EXPECTED_STATES = {
    "dq": {
        "land_smoke": "success",
        "bronze": "success",
        "inject_bad_batch": "success",
        "dq_bronze": "failed",
        "silver": "upstream_failed",
        "dq_silver": "upstream_failed",
        "gold": "upstream_failed",
        "dq_gold": "upstream_failed",
        "query_timetravel": "upstream_failed",
    },
    "wap": {
        "land_smoke": "success",
        "wap_write_bronze": "success",
        "dq_bronze": "failed",
        "publish_bronze": "upstream_failed",
        "wap_write_silver": "upstream_failed",
        "dq_silver": "upstream_failed",
        "publish_silver": "upstream_failed",
        "wap_write_gold": "upstream_failed",
        "dq_gold": "upstream_failed",
        "publish_gold": "upstream_failed",
        "query_timetravel": "upstream_failed",
    },
}


def validate(kind: str, output: str) -> None:
    """Read final Airflow CLI table rows, regardless of order or logical-date formatting."""
    expected = EXPECTED_STATES[kind]
    dag_id = f"lakehouse_retail_medallion_{kind}"
    observed: dict[str, str] = {}
    for line in output.splitlines():
        fields = line.split()
        if not fields or fields[0] != dag_id:
            continue
        positions = [index for index, value in enumerate(fields) if value in expected]
        if len(positions) != 1:
            continue
        position = positions[0]
        task = fields[position]
        if task in observed or position + 1 >= len(fields):
            raise ValueError(f"ambiguous task-state row for {task}")
        observed[task] = fields[position + 1]
    missing = sorted(set(expected) - set(observed))
    if missing:
        raise ValueError(f"missing required task-state rows: {', '.join(missing)}")
    for task, state in expected.items():
        if observed[task] != state:
            raise ValueError(f"{task} was {observed[task]}, expected {state}")
    print(f"Verified {dag_id}: upstream writes succeeded, dq_bronze failed, all downstream tasks blocked")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=tuple(EXPECTED_STATES))
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        validate(args.kind, args.output.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"Airflow bad-run evidence error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
