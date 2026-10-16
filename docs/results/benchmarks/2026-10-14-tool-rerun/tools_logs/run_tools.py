"""Same six silver.orders checks in five tools, at scale (see benchmarks/README.md).

Runs in its own virtualenv (benchmarks/tools/requirements.txt) because the tools pin conflicting
dependencies. Each tool runs as a fresh process; the parent records wall time, peak RSS and verdicts.

    python benchmarks/tools/run_tools.py bench --sizes 100000 1000000 --repeats 3 --out results.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import re
import resource
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

TOOLS = ("soda", "datacontract", "gx", "pointblank", "pandera")
CHECKS = ("order_id_missing", "customer_id_missing", "order_id_unique", "status_valid", "amount_min",
          "customer_ref")  # fmt: skip
EXPECTED_FAIL = {"order_id_missing": False, **dict.fromkeys(CHECKS[1:], True)}
STATUSES = ["paid", "shipped", "cancelled", "refunded"]


def defects(rows: int) -> int:
    return max(1, rows // 1000)


def generate(rows: int, out: Path) -> dict[str, Any]:
    """orders.parquet + customers.parquet with k rows of each of five defects (disjoint rows)."""
    import duckdb

    if rows < 6:
        raise ValueError("at least six orders are required for the disjoint defect fixture")
    k = defects(rows)
    customers = max(10, rows // 10)
    con = duckdb.connect()
    con.execute(f"""
      CREATE TABLE orders AS SELECT
        'o-' || lpad(CAST(i AS VARCHAR), 9, '0') AS order_id,
        CASE WHEN i < {k} THEN NULL
             WHEN i < {2 * k} THEN 'x-' || CAST(i AS VARCHAR)
             ELSE 'c-' || lpad(CAST(hash(i) % {customers} AS VARCHAR), 7, '0') END AS customer_id,
        CASE WHEN i >= {2 * k} AND i < {3 * k} THEN 'teleported'
             ELSE ['paid','shipped','cancelled','refunded'][1 + CAST(hash(i * 7) % 4 AS INTEGER)] END AS status,
        CASE WHEN i >= {3 * k} AND i < {4 * k} THEN -10.0
             ELSE 1 + CAST(hash(i * 13) % 49900 AS DOUBLE) / 100 END AS amount
      FROM range({rows - k}) t(i)
    """)
    # k duplicate order_ids: copies of valid rows 4k..5k with the same id
    con.execute(
        f"INSERT INTO orders SELECT * FROM orders WHERE CAST(substr(order_id, 3) AS BIGINT) "
        f"BETWEEN {4 * k} AND {5 * k - 1}"
    )
    con.execute(
        f"CREATE TABLE customers AS SELECT 'c-' || lpad(CAST(i AS VARCHAR), 7, '0') AS customer_id "
        f"FROM range({customers}) t(i)"
    )
    out.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY orders TO '{out / 'orders.parquet'}' (FORMAT parquet)")
    con.execute(f"COPY customers TO '{out / 'customers.parquet'}' (FORMAT parquet)")
    truth = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE order_id IS NULL),
          count(*) FILTER (WHERE customer_id IS NULL), count(*) - count(DISTINCT order_id),
          count(*) FILTER (WHERE status NOT IN ('paid', 'shipped', 'cancelled', 'refunded')),
          count(*) FILTER (WHERE amount < 0),
          count(*) FILTER (WHERE customer_id IS NOT NULL
            AND customer_id NOT IN (SELECT customer_id FROM customers)) FROM orders
    """).fetchone()
    if truth is None or tuple(truth) != (rows, 0, k, k, k, k, k):
        raise RuntimeError(f"generated fixture does not match requested rows and defect counts: {truth}")
    con.close()
    return {
        "rows": int(truth[0]), "k": k, "customers": customers,
        "ground_truth": dict(zip(CHECKS, truth[1:], strict=True)),
        "duplicate_group_rows": 2 * k,
        "files": {name: {"sha256": hashlib.sha256((out / name).read_bytes()).hexdigest(),
                         "bytes": (out / name).stat().st_size}
                  for name in ("orders.parquet", "customers.parquet")},
    }


SODA_CONTRACT = """dataset: duck/main/orders
checks:
  - row_count:
columns:
  - name: order_id
    checks:
      - missing:
          name: order_id_missing
      - duplicate:
          name: order_id_unique
  - name: customer_id
    checks:
      - missing:
          name: customer_id_missing
      - invalid:
          name: customer_ref
          valid_reference_data:
            dataset: duck/main/customers
            column: customer_id
  - name: status
    checks:
      - invalid:
          name: status_valid
          valid_values: [paid, shipped, cancelled, refunded]
  - name: amount
    checks:
      - invalid:
          name: amount_min
          valid_min: 0
"""


def run_soda(data: Path) -> dict[str, dict[str, Any]]:
    import duckdb
    from soda_core.contracts import verify_contract_locally
    from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

    con = duckdb.connect()
    con.execute(f"CREATE TABLE orders AS SELECT * FROM '{data / 'orders.parquet'}'")
    con.execute(f"CREATE TABLE customers AS SELECT * FROM '{data / 'customers.parquet'}'")
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as fh:
        fh.write(SODA_CONTRACT)
    session = verify_contract_locally(
        data_sources=[DuckDBDataSourceImpl.from_existing_cursor(con, name="duck")], contract_file_path=fh.name
    )
    out = {}
    for result in session.contract_verification_results:
        for check in result.check_results:
            name = getattr(check.check, "name", None)
            if name in CHECKS:
                values = getattr(check, "diagnostic_metric_values", {}) or {}
                key = {"missing": "missing_count", "unique": "duplicate_count"}.get(
                    name.split("_")[-1], "invalid_count"
                )
                count = values.get(key)
                out[name] = {"failed": check.outcome.name == "FAILED", "count": count}
    return out


ODCS = """apiVersion: v3.1.0
kind: DataContract
id: silver-orders
name: silver.orders
version: 1.0.0
status: active
servers:
  - server: local
    type: local
    format: parquet
    path: {orders}
schema:
  - name: orders
    physicalType: table
    properties:
      - name: order_id
        logicalType: string
        required: true
        unique: true
      - name: customer_id
        logicalType: string
        required: true
      - name: status
        logicalType: string
        quality:
          - type: library
            metric: invalidValues
            arguments:
              validValues: [paid, shipped, cancelled, refunded]
            mustBe: 0
      - name: amount
        logicalType: number
        quality:
          - type: sql
            description: amount_min
            query: SELECT COUNT(*) FROM orders WHERE amount < 0
            mustBe: 0
    quality:
      - type: sql
        description: customer_ref
        query: SELECT COUNT(*) FROM orders o WHERE o.customer_id IS NOT NULL AND o.customer_id NOT IN (SELECT customer_id FROM read_parquet('{customers}'))
        mustBe: 0
"""


def run_datacontract(data: Path) -> dict[str, dict[str, Any]]:
    from datacontract.data_contract import DataContract

    spec = ODCS.format(orders=data / "orders.parquet", customers=data / "customers.parquet")
    run = DataContract(data_contract_str=spec).test()
    out: dict[str, dict[str, Any]] = {}
    for check in run.checks:
        text = f"{check.name} {check.field or ''} {check.reason or ''}"
        match = re.search(r"was (\d+)", check.reason or "")
        count = int(match[1]) if match else None
        failed = str(check.result).lower().endswith("failed")
        key = None
        if "missing" in text and check.field == "order_id":
            key = "order_id_missing"
        elif "missing" in text and check.field == "customer_id":
            key = "customer_id_missing"
        elif "duplicate" in text:
            key = "order_id_unique"
        elif "invalid_count" in text or ("status" in (check.field or "") and "invalid" in text):
            key = "status_valid"
        elif "amount_min" in text:
            key = "amount_min"
        elif "customer_ref" in text:
            key = "customer_ref"
        if key:
            out[key] = {"failed": failed, "count": count}
    return out


def run_gx(data: Path) -> dict[str, dict[str, Any]]:
    import great_expectations as gx
    import great_expectations.expectations as gxe
    import pandas as pd

    orders = pd.read_parquet(data / "orders.parquet")
    ids = pd.read_parquet(data / "customers.parquet")["customer_id"].tolist()
    ctx = gx.get_context(mode="ephemeral")
    batch = (ctx.data_sources.add_pandas("p").add_dataframe_asset("orders")
             .add_batch_definition_whole_dataframe("all"))  # fmt: skip
    suite = gx.ExpectationSuite(name="silver_orders")
    plan = {
        "order_id_missing": gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        "customer_id_missing": gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
        "order_id_unique": gxe.ExpectColumnValuesToBeUnique(column="order_id"),
        "status_valid": gxe.ExpectColumnValuesToBeInSet(column="status", value_set=STATUSES),
        "amount_min": gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0),
        "customer_ref": gxe.ExpectColumnValuesToBeInSet(column="customer_id", value_set=ids),
    }
    for name, expectation in plan.items():
        expectation.meta = {"benchmark_check": name}
        suite.add_expectation(expectation)
    result = batch.get_batch(batch_parameters={"dataframe": orders}).validate(suite)
    return gx_checks(result.results)


def gx_checks(results: list[Any]) -> dict[str, dict[str, Any]]:
    """GX can reorder validation results; bind each result to its explicit check identity."""
    out: dict[str, dict[str, Any]] = {}
    for result in results:
        name = (result.expectation_config.meta or {}).get("benchmark_check")
        if name not in CHECKS or name in out:
            raise ValueError(f"missing, unknown or duplicate GX benchmark identity: {name}")
        if not isinstance(result.success, bool):
            raise ValueError(f"GX did not return a pass/fail decision for {name}")
        out[name] = {"failed": not result.success, "count": result.result.get("unexpected_count")}
    if set(out) != set(CHECKS):
        raise ValueError(f"missing GX check results: {sorted(set(CHECKS) - set(out))}")
    return out


def run_pointblank(data: Path) -> dict[str, dict[str, Any]]:
    import pointblank as pb
    import polars as pl

    orders = pl.read_parquet(data / "orders.parquet")
    ids = pl.read_parquet(data / "customers.parquet")["customer_id"].to_list()
    validation = (
        pb.Validate(data=orders, thresholds=pb.Thresholds(warning=1, error=1))
        .col_vals_not_null(columns="order_id")
        .col_vals_not_null(columns="customer_id")
        .rows_distinct(columns_subset=["order_id"])
        .col_vals_in_set(columns="status", set=STATUSES)
        .col_vals_ge(columns="amount", value=0)
        .col_vals_in_set(columns="customer_id", set=ids)
        .interrogate()
    )
    failed = validation.n_failed()
    return {name: {"failed": failed[i + 1] > 0, "count": failed[i + 1]} for i, name in enumerate(CHECKS)}


def run_pandera(data: Path) -> dict[str, dict[str, Any]]:
    import pandera.polars as pa
    import polars as pl

    orders = pl.read_parquet(data / "orders.parquet")
    ids = pl.read_parquet(data / "customers.parquet")["customer_id"].to_list()
    schema = pa.DataFrameSchema({
        "order_id": pa.Column(str, nullable=False, unique=True),
        "customer_id": pa.Column(str, nullable=False, checks=pa.Check.isin(ids, name="customer_ref")),
        "status": pa.Column(str, checks=pa.Check.isin(STATUSES, name="status_valid")),
        "amount": pa.Column(float, checks=pa.Check.ge(0, name="amount_min")),
    })  # fmt: skip
    out = {name: {"failed": False, "count": 0} for name in CHECKS}
    try:
        schema.validate(orders, lazy=True)
    except pa.errors.SchemaErrors as exc:
        cases = exc.failure_cases.to_dicts()
        for case in cases:
            column, check = str(case.get("column")), str(case.get("check"))
            if "not_nullable" in check:
                key = f"{column}_missing"
            elif "unique" in check or "field_uniqueness" in check:
                key = "order_id_unique"
            elif column == "status":
                key = "status_valid"
            elif column == "amount":
                key = "amount_min"
            elif column == "customer_id":
                key = "customer_ref"
            else:
                continue
            if key in out:
                out[key]["failed"] = True
                out[key]["count"] = int(out[key]["count"]) + 1
    return out


RUNNERS = {"soda": run_soda, "datacontract": run_datacontract, "gx": run_gx, "pointblank": run_pointblank,
           "pandera": run_pandera}  # fmt: skip


def one(tool: str, data: Path) -> None:
    start = time.perf_counter()
    checks = RUNNERS[tool](data)
    wall = time.perf_counter() - start
    usage = resource.getrusage(resource.RUSAGE_SELF)
    rss = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
    print("BENCH_JSON " + json.dumps({"tool": tool, "run_s": wall, "max_rss_bytes": rss,
                                      "cpu_s": usage.ru_utime + usage.ru_stime, "checks": checks}, default=str))  # fmt: skip


PACKAGES = ("soda-core", "soda-duckdb", "datacontract-cli", "great-expectations", "pointblank", "pandera",
            "duckdb", "pandas", "polars", "pyarrow")  # fmt: skip


def write_versions(path: Path) -> None:
    """Record the exact versions used next to the results."""
    from importlib.metadata import PackageNotFoundError, version

    found: dict[str, str] = {}
    for name in PACKAGES:
        try:
            found[name] = version(name)
        except PackageNotFoundError:
            found[name] = "not installed"
    found["python"] = sys.version.split()[0]
    path.write_text(json.dumps(found, indent=2) + "\n")


def score_result(proc: subprocess.CompletedProcess[str], tool: str) -> tuple[dict[str, Any], list[str]]:
    """Reject partial/crashed executions and compare all six identified pass/fail decisions."""
    if proc.returncode != 0:
        raise ValueError(f"tool exited with status {proc.returncode}")
    lines = [line.removeprefix("BENCH_JSON ") for line in proc.stdout.splitlines()
             if line.startswith("BENCH_JSON ")]
    if len(lines) != 1:
        raise ValueError(f"expected one BENCH_JSON result, got {len(lines)}")
    result = json.loads(lines[0])
    if not isinstance(result, dict):
        raise ValueError("tool measurement must be a JSON object")
    if result.get("tool") != tool:
        raise ValueError("tool result identity differs from requested tool")
    for key in ("run_s", "cpu_s", "max_rss_bytes"):
        value = result.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid measurement: {key}={value!r}")
    checks = result.get("checks")
    if not isinstance(checks, dict) or set(checks) != set(CHECKS):
        raise ValueError("tool result must identify exactly the six scored checks")
    for name, check in checks.items():
        if not isinstance(check, dict) or not isinstance(check.get("failed"), bool):
            raise ValueError(f"invalid pass/fail decision for {name}")
    wrong = [name for name in CHECKS if checks[name]["failed"] != EXPECTED_FAIL[name]]
    return result, wrong


def bench(sizes: list[int], repeats: int, out: Path, tools: list[str]) -> bool:
    if (not sizes or min(sizes) < 6 or len(set(sizes)) != len(sizes)
            or repeats < 1 or not tools or len(set(tools)) != len(tools)):
        raise ValueError("require distinct sizes >= 6, repeats >= 1 and distinct tools")
    fields = ["rows", "k", "tool", "repeat", "process_s", "run_s", "cpu_s", "max_rss_bytes", "correct",
              "wrong_checks", "counts"]  # fmt: skip
    out.parent.mkdir(parents=True, exist_ok=True)
    versions_path = out.with_name("tools_versions.json")
    write_versions(versions_path)
    logs = out.with_name(f"{out.stem}_logs")
    logs.mkdir(exist_ok=True)
    source = Path(__file__).resolve()
    source_bytes = source.read_bytes()
    (logs / "run_tools.py").write_bytes(source_bytes)
    root = source.parents[2]
    git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=False)
    status = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root,
                            capture_output=True, text=True, check=False)
    cpu_model = platform.processor()
    if sys.platform == "darwin":
        cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                             capture_output=True, text=True, check=False)
        if cpu.returncode == 0:
            cpu_model = cpu.stdout.strip()
    metadata = {
        "schema_version": 1, "started_at": datetime.now(UTC).isoformat(),
        "git_commit": git.stdout.strip() if git.returncode == 0 else None,
        "tracked_worktree_dirty": bool(status.stdout.strip()) if status.returncode == 0 else None,
        "harness_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "harness_snapshot": str(logs / "run_tools.py"),
        "versions": json.loads(versions_path.read_text()), "executable": sys.executable,
        "system": {"platform": platform.platform(), "machine": platform.machine(),
                   "processor": cpu_model, "logical_cpu_count": os.cpu_count()},
        "thread_environment": {name: os.environ.get(name) for name in (
            "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "POLARS_MAX_THREADS",
            "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS")},
        "sizes": sizes, "repeats": repeats, "tools": tools, "fixtures": [],
        "execution_order": [],
        "scheduling": "Within each size, tools are interleaved per repeat; starting tool rotates by repeat. No caches are forcibly cleared.",
        "timing": {"run_s": "Tool imports, Parquet loading, setup, validation and diagnostic mapping; excludes interpreter startup",
                   "process_s": "Parent-observed child wall time including interpreter startup and shutdown",
                   "cpu_s": "Whole child user + system CPU from getrusage(RUSAGE_SELF)",
                   "max_rss_bytes": "Whole child peak RSS from getrusage(RUSAGE_SELF), platform-normalized to bytes"},
        "scoring": "Six identified pass/fail decisions; diagnostic counts are recorded, not normalized or scored",
        "count_semantics": "Ground-truth uniqueness counts extra occurrences; some tools count all duplicate-group rows. Null treatment in reference checks also differs.",
    }
    matched = True
    raw_path = out.with_name(f"{out.stem}_raw.jsonl")
    with out.open("w", newline="") as fh, raw_path.open("w") as raw, tempfile.TemporaryDirectory() as tmp:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for rows in sizes:
            data = Path(tmp) / str(rows)
            meta = generate(rows, data)
            if meta["rows"] != rows:
                raise RuntimeError(f"requested {rows} orders, generated {meta['rows']}")
            metadata["fixtures"].append(meta)
            for repeat in range(repeats):
                offset = repeat % len(tools)
                for tool in tools[offset:] + tools[:offset]:
                    metadata["execution_order"].append({"rows": rows, "repeat": repeat, "tool": tool})
                    start = time.perf_counter()
                    proc = subprocess.run([sys.executable, __file__, "one", tool, str(data)],
                                          capture_output=True, text=True, check=False)  # fmt: skip
                    elapsed = time.perf_counter() - start
                    stem = f"{rows}-{tool}-{repeat}"
                    (logs / f"{stem}.stdout.log").write_text(proc.stdout)
                    (logs / f"{stem}.stderr.log").write_text(proc.stderr)
                    record: dict[str, Any] = {"rows": rows, "tool": tool, "repeat": repeat,
                                              "returncode": proc.returncode, "process_s": elapsed,
                                              "stdout_log": f"{stem}.stdout.log", "stderr_log": f"{stem}.stderr.log"}
                    try:
                        res, wrong = score_result(proc, tool)
                    except (ValueError, TypeError) as exc:
                        matched = False
                        record["error"] = str(exc)
                        raw.write(json.dumps(record) + "\n")
                        raw.flush()
                        writer.writerow({"rows": rows, "k": meta["k"], "tool": tool, "repeat": repeat,
                                         "process_s": round(elapsed, 3), "correct": "error",
                                         "wrong_checks": str(exc)})  # fmt: skip
                        fh.flush()
                        print(rows, tool, repeat, round(elapsed, 2), str(exc))
                        continue
                    checks = res["checks"]
                    matched = matched and not wrong
                    record.update({"result": res, "wrong_checks": wrong, "correct": not wrong})
                    raw.write(json.dumps(record) + "\n")
                    raw.flush()
                    writer.writerow({
                        "rows": rows, "k": meta["k"], "tool": tool, "repeat": repeat,
                        "process_s": round(elapsed, 3), "run_s": round(res["run_s"], 3),
                        "cpu_s": round(res["cpu_s"], 3), "max_rss_bytes": res["max_rss_bytes"],
                        "correct": not wrong, "wrong_checks": "|".join(wrong),
                        "counts": "|".join(f"{c}={checks.get(c, {}).get('count')}" for c in CHECKS),
                    })  # fmt: skip
                    fh.flush()
                    print(rows, tool, repeat, round(elapsed, 2), wrong if wrong else "correct")
    metadata.update({"completed_at": datetime.now(UTC).isoformat(), "all_verdicts_match": matched,
                     "csv_sha256": hashlib.sha256(out.read_bytes()).hexdigest(),
                     "raw_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest()})
    out.with_name(f"{out.stem}_metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return matched


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bench")
    b.add_argument("--sizes", type=int, nargs="+", default=[100_000, 1_000_000])
    b.add_argument("--repeats", type=int, default=3)
    b.add_argument("--tools", nargs="+", default=list(TOOLS), choices=TOOLS)
    b.add_argument("--out", type=Path, required=True)
    o = sub.add_parser("one")
    o.add_argument("tool", choices=TOOLS)
    o.add_argument("data", type=Path)
    args = parser.parse_args()
    if args.cmd == "one":
        one(args.tool, args.data)
    else:
        return 0 if bench(args.sizes, args.repeats, args.out, args.tools) else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
