"""Same six silver.orders checks in five tools, at scale (see benchmarks/README.md).

Runs in its own virtualenv (benchmarks/tools/requirements.txt) because the tools pin conflicting
dependencies. Each tool runs as a fresh process; the parent records wall time, peak RSS and verdicts.

    python benchmarks/tools/run_tools.py bench --sizes 100000 1000000 --repeats 3 --out results.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import resource
import subprocess
import sys
import tempfile
import time
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
    total = con.execute("SELECT COUNT(*) FROM orders").fetchone()
    return {"rows": int(total[0]) if total else 0, "k": k, "customers": customers}


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
    for expectation in plan.values():
        suite.add_expectation(expectation)
    result = batch.get_batch(batch_parameters={"dataframe": orders}).validate(suite)
    out = {}
    for name, res in zip(plan, result.results, strict=True):
        out[name] = {"failed": not res.success, "count": res.result.get("unexpected_count")}
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


def bench(sizes: list[int], repeats: int, out: Path, tools: list[str]) -> None:
    fields = ["rows", "k", "tool", "repeat", "process_s", "run_s", "cpu_s", "max_rss_bytes", "correct",
              "wrong_checks", "counts"]  # fmt: skip
    out.parent.mkdir(parents=True, exist_ok=True)
    write_versions(out.with_name("tools_versions.json"))
    with out.open("w", newline="") as fh, tempfile.TemporaryDirectory() as tmp:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for rows in sizes:
            data = Path(tmp) / str(rows)
            meta = generate(rows, data)
            for tool in tools:
                for repeat in range(repeats):
                    start = time.perf_counter()
                    proc = subprocess.run([sys.executable, __file__, "one", tool, str(data)],
                                          capture_output=True, text=True, check=False)  # fmt: skip
                    elapsed = time.perf_counter() - start
                    line = next((x for x in proc.stdout.splitlines() if x.startswith("BENCH_JSON ")), None)
                    if line is None:
                        writer.writerow({"rows": rows, "k": meta["k"], "tool": tool, "repeat": repeat,
                                         "process_s": round(elapsed, 3), "correct": "error",
                                         "wrong_checks": proc.stderr.strip().splitlines()[-1:] or ""})  # fmt: skip
                        continue
                    res = json.loads(line.removeprefix("BENCH_JSON "))
                    checks = res["checks"]
                    wrong = [c for c in CHECKS if c not in checks or checks[c]["failed"] != EXPECTED_FAIL[c]]
                    writer.writerow({
                        "rows": rows, "k": meta["k"], "tool": tool, "repeat": repeat,
                        "process_s": round(elapsed, 3), "run_s": round(res["run_s"], 3),
                        "cpu_s": round(res["cpu_s"], 3), "max_rss_bytes": res["max_rss_bytes"],
                        "correct": not wrong, "wrong_checks": "|".join(wrong),
                        "counts": "|".join(f"{c}={checks.get(c, {}).get('count')}" for c in CHECKS),
                    })  # fmt: skip
                    fh.flush()
                    print(rows, tool, repeat, round(elapsed, 2), wrong if wrong else "correct")


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
        bench(args.sizes, args.repeats, args.out, args.tools)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
