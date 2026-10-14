"""Write-audit-publish overhead at scale (needs the full lakehouse with docker/docker-compose.wap.yml).

For each size and repeat, layer by layer: the build written straight to main (``direct``, the baseline), then
the same build on an audit branch (``write``), the gate on that branch, and the fast-forward (``publish``).
Spark times are measured inside the job (JVM start excluded) and as wall clock around ``docker exec``.

    uv run python benchmarks/wap_overhead.py --sizes 100000 1000000 --repeats 2 --out docs/results/benchmarks/<dir>

Writes ``wap_overhead.csv``. Afterwards the lakehouse holds generated data: restore the sample with
``make wap-demo-good``.
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

from soda_lakehouse_dq.container import build_wap
from soda_lakehouse_dq.domain.models import Layer
from soda_lakehouse_dq.settings import Engine, Settings

ROOT = Path(__file__).resolve().parent.parent
FIELDS = ["rows", "repeat", "layer", "table_rows", "direct_s", "direct_wall_s", "write_s", "write_wall_s",
          "gate_s", "publish_s", "publish_wall_s", "gate_status"]  # fmt: skip


def generate(rows: int) -> str:
    """Batch under reports/ (mounted in ldl-spark at /opt/soda-wap-reports); returns the container path."""
    out = ROOT / "reports" / "wap-bench" / f"data_{rows}"
    if not (out / "customers.csv").exists():
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "gen_scale_data.py"),
                "--rows",
                str(rows),
                "--out",
                str(out),
            ],
            check=True,
        )
    return f"/opt/soda-wap-reports/wap-bench/data_{rows}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", type=int, nargs="+", default=[100_000, 1_000_000])
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    wap = build_wap(Settings(engine=Engine.DUCKDB, sinks=[]))
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / "wap_overhead.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        for rows in args.sizes:
            source = generate(rows)
            for repeat in range(1, args.repeats + 1):
                branch = f"bench_{rows}_{repeat}"
                for layer in Layer.ordered():
                    src = source if layer is Layer.BRONZE else None
                    t0 = time.perf_counter()
                    direct = wap.spark.direct(layer, src)
                    t1 = time.perf_counter()
                    written = wap.spark.write(layer, branch, src)
                    t2 = time.perf_counter()
                    audit = wap.verify_branch.audit("retail", layer, branch, run_id=branch)
                    report = audit.report
                    t3 = time.perf_counter()
                    if report.blocked:
                        wap.spark.drop(layer, branch)
                        raise SystemExit(
                            f"gate blocked generated data at {rows}/{layer.value}: {report.reasons}"
                        )
                    published = wap.spark.publish(layer, branch, audit.pins, main_before=audit.main_before)
                    t4 = time.perf_counter()
                    table_rows = sum(s.main_rows or 0 for s in published.tables.values())
                    row = {"rows": rows, "repeat": repeat, "layer": layer.value, "table_rows": table_rows,
                           "direct_s": direct.seconds, "direct_wall_s": round(t1 - t0, 3),
                           "write_s": written.seconds, "write_wall_s": round(t2 - t1, 3),
                           "gate_s": round(t3 - t2, 3), "publish_s": published.seconds,
                           "publish_wall_s": round(t4 - t3, 3), "gate_status": report.status.value}  # fmt: skip
                    writer.writerow(row)
                    fh.flush()
                    print(row, flush=True)
                    # Benchmark branch names repeat across invocations. Remove only this harness's
                    # successful branch after timing, before a later direct baseline changes main.
                    wap.spark.drop(layer, branch)
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
