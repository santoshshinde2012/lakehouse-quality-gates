"""Spark job time per layer on the lakehouse's own sample, next to the gate time on the same tables.

Runs ``spark-submit`` for the bronze, silver and gold jobs in ``ldl-spark`` (as the Airflow DAG does) and
the host DuckDB gate after each, ``--repeats`` times. Overwrites the live retail tables with the sample.
"""

from __future__ import annotations

import argparse
import subprocess
import time

from benchmarks.common import RESULTS, append_csv, run_probe

JOBS = {"bronze": "retail/02_ingest_bronze.py", "silver": "retail/03_transform_silver.py",
        "gold": "retail/04_publish_gold.py"}  # fmt: skip
FIELDS = ("repeat", "layer", "spark_s", "gate_s", "gate_process_s", "status")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--container", default="ldl-spark")
    args = parser.parse_args(argv)
    for repeat in range(args.repeats):
        for layer, job in JOBS.items():
            cmd = ["docker", "exec", args.container, "/opt/spark/bin/spark-submit", "--master", "local[*]",
                   f"/opt/jobs/{job}"]  # fmt: skip
            start = time.perf_counter()
            subprocess.run(cmd, check=True, capture_output=True)
            spark_s = time.perf_counter() - start
            gate = run_probe("duckdb", layer)
            row = {"repeat": repeat, "layer": layer, "spark_s": round(spark_s, 2),
                   "gate_s": gate.payload["gate_s"], "gate_process_s": round(gate.process_s, 2),
                   "status": gate.payload["status"]}  # fmt: skip
            append_csv(RESULTS / "spark_reference.csv", row, FIELDS)
            print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
