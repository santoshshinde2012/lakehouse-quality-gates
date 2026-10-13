"""Performance benchmark: gate latency, CPU, memory and SQL queries per scale x engine x layer.

For each scale: generate a clean batch (scripts/gen_scale_data.py), rebuild the live Iceberg medallion
with per-layer timings (seed_timed.py), then run every gate ``1 + warm`` times per engine as a fresh
process. The first run after the seed is recorded as ``cold``; the rest as ``warm``.

    uv run python -m benchmarks.run_perf --sizes 10000 100000 1000000 5000000 --warm 5
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from benchmarks.common import (
    ENGINES,
    LAYERS,
    RESULTS,
    ROOT,
    append_csv,
    append_jsonl,
    now_utc,
    run_probe,
    trino_window_stats,
)

PERF_FIELDS = (
    "rows", "engine", "layer", "run", "phase", "status", "gate_s", "process_s", "import_s",
    "cpu_user_s", "cpu_sys_s", "max_rss_bytes", "queries", "server_queries", "server_cpu_s",
    "scan_bytes", "peak_mem_bytes",
)  # fmt: skip
PIPE_FIELDS = ("rows", "drop_s", "bronze_s", "silver_s", "gold_s", "table", "table_rows", "table_bytes")


def _bench_json(proc: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    for line in proc.stdout.splitlines():
        if line.startswith("BENCH_JSON "):
            return dict(json.loads(line.removeprefix("BENCH_JSON ")))
    raise RuntimeError(proc.stderr[-2000:])


def table_bytes() -> dict[str, int]:
    """Total Iceberg data-file bytes per retail table, from Trino's ``$files`` metadata tables."""
    import trino

    con = trino.dbapi.connect(host="localhost", port=8088, user="bench", catalog="lakehouse")
    out: dict[str, int] = {}
    cur = con.cursor()
    for table in ("bronze.orders_raw", "bronze.customers_raw", "silver.orders", "silver.customers",
                  "gold.daily_order_metrics"):  # fmt: skip
        schema, name = table.split(".")
        cur.execute(f'SELECT COALESCE(SUM(file_size_in_bytes), 0) FROM {schema}."{name}$files"')
        out[table] = int(cur.fetchone()[0])
    return out


def seed_scale(rows: int, workdir: Path, seed: int = 42) -> dict[str, Any]:
    gen = [sys.executable, "scripts/gen_scale_data.py", "--rows", str(rows), "--out", str(workdir)]
    subprocess.run([*gen, "--seed", str(seed)], cwd=ROOT, check=True, capture_output=True)
    proc = subprocess.run(
        [sys.executable, "-m", "benchmarks.seed_timed", str(workdir)],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )  # fmt: skip
    return _bench_json(proc)


def bench_scale(rows: int, warm: int, engines: list[str], tag: str) -> None:
    with tempfile.TemporaryDirectory(prefix=f"lqg-{rows}-") as tmp:
        seeded = seed_scale(rows, Path(tmp))
    sizes = table_bytes()
    for table, count in seeded["counts"].items():
        append_csv(
            RESULTS / f"pipeline_{tag}.csv",
            {"rows": rows, **seeded["timings"], "table": table, "table_rows": count,
             "table_bytes": sizes.get(table, 0)},
            PIPE_FIELDS,
        )  # fmt: skip
    for engine in engines:
        for layer in LAYERS:
            for run in range(1 + warm):
                since = now_utc()
                result = run_probe(engine, layer)
                row: dict[str, Any] = {
                    "rows": rows, "engine": engine, "layer": layer, "run": run,
                    "phase": "cold" if run == 0 else "warm", "process_s": round(result.process_s, 3),
                    **{k: v for k, v in result.payload.items() if k not in {"checks", "engine"}},
                }  # fmt: skip
                if engine == "trino":
                    row.update(trino_window_stats(since))
                append_csv(RESULTS / f"perf_{tag}.csv", row, PERF_FIELDS)
                append_jsonl(RESULTS / f"perf_{tag}.jsonl", {**row, "checks": result.payload["checks"]})
                print(f"{rows:>8} {engine:9} {layer:6} run{run} {row['status']:8} gate={row['gate_s']}s")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 100_000, 1_000_000, 5_000_000])
    parser.add_argument("--warm", type=int, default=5)
    parser.add_argument("--engines", nargs="+", default=list(ENGINES), choices=ENGINES)
    parser.add_argument("--tag", default="raw")
    args = parser.parse_args(argv)
    for rows in args.sizes:
        bench_scale(rows, args.warm, args.engines, args.tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
