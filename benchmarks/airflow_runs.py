"""End-to-end pipeline timings from Airflow: one good and one bad run of the gated DAG, per task.

Triggers ``lakehouse_retail_medallion_dq`` (Spark bronze -> dq_bronze -> silver -> dq_silver -> gold -> dq_gold)
with and without ``inject_bad_batch``, waits for each run and records every task's state and duration.
Needs `make airflow-dq-up`. Writes airflow_runs.csv and airflow_tasks.csv; the good run is repeated last so
the lakehouse is left with good data.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from typing import Any

from benchmarks.common import RESULTS, append_csv

DAG = "lakehouse_retail_medallion_dq"
SCHEDULER = "ldl-airflow-scheduler"
RUN_FIELDS = ("variant", "repeat", "run_id", "state", "start", "end", "duration_s")
TASK_FIELDS = ("variant", "repeat", "run_id", "task", "state", "start", "end", "duration_s")


def airflow(*args: str) -> str:
    proc = subprocess.run(["docker", "exec", SCHEDULER, "airflow", *args], capture_output=True, text=True,
                          check=True, timeout=300)  # fmt: skip
    return proc.stdout


def _json(text: str) -> list[dict[str, Any]]:
    start = text.find("[")
    return list(json.loads(text[start:])) if start >= 0 else []


def _secs(start: str | None, end: str | None) -> float | str:
    if not start or not end:
        return ""
    return round((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(), 2)


def run_once(variant: str, repeat: int, timeout_s: int = 1800) -> str:
    run_id = f"bench_{variant}_{datetime.now(UTC):%Y%m%dT%H%M%S}"
    conf = json.dumps({"inject_bad_batch": variant == "bad"})
    airflow("dags", "trigger", DAG, "--run-id", run_id, "--conf", conf, "-o", "plain")
    deadline = time.monotonic() + timeout_s
    run: dict[str, Any] = {}
    while time.monotonic() < deadline:
        runs = [
            r for r in _json(airflow("dags", "list-runs", DAG, "-o", "json")) if r.get("run_id") == run_id
        ]
        run = runs[0] if runs else {}
        if run.get("state") in {"success", "failed"}:
            break
        time.sleep(5)
    append_csv(RESULTS / "airflow_runs.csv", {
        "variant": variant, "repeat": repeat, "run_id": run_id, "state": run.get("state", "timeout"),
        "start": run.get("start_date"), "end": run.get("end_date"),
        "duration_s": _secs(run.get("start_date"), run.get("end_date")),
    }, RUN_FIELDS)  # fmt: skip
    for task in _json(airflow("tasks", "states-for-dag-run", DAG, run_id, "-o", "json")):
        append_csv(RESULTS / "airflow_tasks.csv", {
            "variant": variant, "repeat": repeat, "run_id": run_id, "task": task.get("task_id"),
            "state": task.get("state"), "start": task.get("start_date"), "end": task.get("end_date"),
            "duration_s": _secs(task.get("start_date"), task.get("end_date")),
        }, TASK_FIELDS)  # fmt: skip
    print(variant, repeat, run_id, run.get("state"))
    return str(run.get("state"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args(argv)
    airflow("dags", "unpause", DAG)
    for repeat in range(args.repeats):
        run_once("good", repeat)
        run_once("bad", repeat)
    run_once("good", args.repeats)  # leave good data behind
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
