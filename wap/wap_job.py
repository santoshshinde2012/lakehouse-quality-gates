"""Write-audit-publish Spark job for the retail medallion (runs inside ldl-spark).

Mounted at /opt/soda-wap by docker/docker-compose.wap.yml. Reuses the lakehouse's own SQL files
(/opt/sql/retail, read-only). Normal WAP writes stay on audit branches; the direct action writes main
only for the explicit benchmark baseline.

    spark-submit /opt/soda-wap/wap_job.py write   --layer bronze --branch audit_x [--source DIR]
    spark-submit /opt/soda-wap/wap_job.py publish --layer bronze --branch audit_x --audit-file AUDIT_JSON
    spark-submit /opt/soda-wap/wap_job.py drop    --layer bronze --branch audit_x
    spark-submit /opt/soda-wap/wap_job.py status  --layer bronze [--branch audit_x]

Each action prints one line ``WAP_JSON {...}`` with snapshot ids and row counts; the soda-dq Spark adapter
parses it. Only identifiers matching [a-z0-9_] are accepted.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CATALOG = "lakehouse"
SQL_DIR = os.environ.get("LDL_SQL_DIR", "/opt/sql/retail")
SAMPLE_DIR = os.environ.get("SAMPLE_DATA_DIR", "/opt/data/sample")
TABLES = {
    "bronze": ("bronze.orders_raw", "bronze.customers_raw"),
    "silver": ("silver.orders", "silver.customers"),
    "gold": ("gold.daily_order_metrics",),
}
SQL_FILES = {"silver": "silver_orders.sql", "gold": "gold_daily_metrics.sql"}
SAFE = re.compile(r"^[a-z0-9_]{1,64}$")


def safe(name: str) -> str:
    if not SAFE.fullmatch(name):
        raise SystemExit(f"unsafe identifier {name!r}")
    return name


def safe_argument(name: str) -> str:
    try:
        return safe(name)
    except SystemExit as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def statements(path: str) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        text = re.sub(r"--[^\n]*", "", fh.read())
    return [s.strip() for s in text.split(";") if s.strip()]


def ctas_selects(layer: str) -> dict[str, str]:
    """``CREATE OR REPLACE TABLE lakehouse.x.y AS <select>`` in the lakehouse SQL file -> {x.y: select}."""
    out: dict[str, str] = {}
    for stmt in statements(os.path.join(SQL_DIR, SQL_FILES[layer])):
        m = re.match(r"(?is)CREATE\s+OR\s+REPLACE\s+TABLE\s+lakehouse\.(\w+\.\w+)\s+AS\s+(.*)", stmt)
        if m:
            out[m.group(1)] = m.group(2)
    return out


def snapshot(spark: SparkSession, table: str, ref: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {CATALOG}.{table}.refs WHERE name = '{safe(ref)}'").collect()
    return int(rows[0][0]) if rows else None


def count(spark: SparkSession, table: str, ref: str | None) -> int | None:
    if ref and snapshot(spark, table, ref) is None:
        return None
    target = f"{CATALOG}.{table}" + (f".branch_{ref}" if ref and ref != "main" else "")
    return int(spark.sql(f"SELECT COUNT(*) FROM {target}").collect()[0][0])


def state(spark: SparkSession, table: str, branch: str | None) -> dict[str, Any]:
    exists = spark.catalog.tableExists(f"{CATALOG}.{table}")
    out: dict[str, Any] = {"exists": exists}
    if exists:
        out |= {"main_snapshot": snapshot(spark, table, "main"), "main_rows": count(spark, table, "main")}
        refs = spark.sql(f"SELECT name, snapshot_id FROM {CATALOG}.{table}.refs").collect()
        out["refs"] = {str(r[0]): int(r[1]) for r in refs}
        if branch:
            out |= {
                "branch_snapshot": snapshot(spark, table, branch),
                "branch_rows": count(spark, table, branch),
            }
    return out


def ensure_branch(spark: SparkSession, table: str, df: DataFrame, branch: str) -> None:
    """Create the table on first use (empty) and the audit branch from main's current snapshot."""
    full = f"{CATALOG}.{table}"
    if not spark.catalog.tableExists(full):
        df.limit(0).writeTo(full).using("iceberg").create()
    if snapshot(spark, table, "main") is None:  # a branch needs a snapshot to start from
        df.limit(0).writeTo(full).append()
    spark.sql(f"ALTER TABLE {full} CREATE BRANCH IF NOT EXISTS {safe(branch)}")


def target(table: str, branch: str) -> str:
    """Write target: the audit branch, or main itself for the ``direct`` benchmark baseline."""
    return f"{CATALOG}.{table}" if branch == "main" else f"{CATALOG}.{table}.branch_{branch}"


def replace_on_branch(spark: SparkSession, table: str, df: DataFrame, branch: str, batch: str = "") -> None:
    if branch == "main":  # direct write (benchmark baseline only)
        if not spark.catalog.tableExists(f"{CATALOG}.{table}"):
            df.limit(0).writeTo(f"{CATALOG}.{table}").using("iceberg").create()
    else:
        ensure_branch(spark, table, df, branch)
    target_ = target(table, branch)
    spark.sql(f"DELETE FROM {target_}")
    writer = df.writeTo(target_)
    if batch:  # 05_query_timetravel.py finds the day-1 snapshot by this property
        writer = writer.option("snapshot-property.ldl.batch", batch)
    writer.append()


def bronze_frames(spark: SparkSession, source: str) -> dict[str, list[tuple[str, DataFrame]]]:
    def read(path: str) -> DataFrame:
        stamp = datetime.now(timezone.utc).replace(tzinfo=None)
        return (
            spark.read.option("header", True).csv(path)
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingested_at", F.lit(stamp).cast("timestamp"))
        )  # fmt: skip

    orders = [
        (os.path.basename(p), read(p).select(
            "order_id", "customer_id", F.to_timestamp("order_ts").alias("order_ts"), "status",
            F.col("amount").cast("double").alias("amount"), "_source_file", "_ingested_at"))
        for p in sorted(glob.glob(os.path.join(source, "orders_day*.csv")))
    ]  # fmt: skip
    customers = read(os.path.join(source, "customers.csv")).select(
        "customer_id", "name", "email", "city", F.to_date("signup_date").alias("signup_date"),
        "_source_file", "_ingested_at")  # fmt: skip
    return {"bronze.orders_raw": orders, "bronze.customers_raw": [("customers.csv", customers)]}


def write(spark: SparkSession, layer: str, branch: str, source: str) -> dict[str, Any]:
    if layer == "bronze":
        for stmt in statements(os.path.join(SQL_DIR, "bronze_ddl.sql")):
            spark.sql(stmt)  # the lakehouse's own CREATE ... IF NOT EXISTS
        for table, parts in bronze_frames(spark, source).items():
            replace_on_branch(spark, table, parts[0][1], branch, parts[0][0].removesuffix(".csv"))
            for name, df in parts[1:]:  # one append (snapshot) per file, like 02_ingest_bronze.py
                df.writeTo(target(table, branch)).option(
                    "snapshot-property.ldl.batch", name.removesuffix(".csv")
                ).append()
    else:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{layer}")
        for table, select in ctas_selects(layer).items():
            replace_on_branch(spark, table, spark.sql(select), branch)
    return {t: state(spark, t, branch) for t in TABLES[layer]}


def validated_pins(layer: str, pins: Any) -> dict[str, int]:
    if not isinstance(pins, dict) or not pins:
        raise ValueError("publication requires audited snapshot pins")
    for table, pin in pins.items():
        if not isinstance(table, str) or not re.fullmatch(r"[a-z0-9_]+\.[a-z0-9_]+", table):
            raise ValueError(f"invalid pinned table {table!r}")
        if type(pin) is not int or pin <= 0:
            raise ValueError(f"invalid snapshot pin for {table}")
    missing = set(TABLES[layer]) - pins.keys()
    if missing:
        raise ValueError(f"missing audited snapshots: {', '.join(sorted(missing))}")
    return pins


def unique_manifest_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate manifest field {key!r}")
        result[key] = value
    return result


def read_audit(path: str, layer: str, branch: str) -> tuple[dict[str, int], dict[str, int | None]]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh, object_pairs_hook=unique_manifest_object)
    if not isinstance(data, dict) or type(data.get("version")) is not int or data["version"] != 1:
        raise ValueError("expected a version 1 WAP audit manifest")
    if data.get("layer") != layer or data.get("branch") != branch:
        raise ValueError("audit manifest layer/branch does not match this publication")
    if not isinstance(data.get("gate"), str) or data["gate"] not in {"passed", "passed_with_warnings"}:
        raise ValueError("a passing audit manifest is required for publication")
    if any(
        not isinstance(data.get(key), str) or not data[key].strip()
        for key in ("pipeline", "run_id", "engine")
    ):
        raise ValueError("audit manifest pipeline, run_id and engine are required")
    reasons = data.get("reasons", [])
    if not isinstance(reasons, list) or any(not isinstance(reason, str) for reason in reasons):
        raise ValueError("audit manifest reasons must be a list of strings")
    created_at = data.get("created_at")
    if (
        not isinstance(created_at, str)
        or datetime.fromisoformat(created_at.replace("Z", "+00:00")).tzinfo is None
    ):
        raise ValueError("audit manifest created_at must include a timezone")
    pins = validated_pins(layer, data.get("pins"))
    before = validated_before(pins, data.get("main_before", {}))
    return pins, before


def validated_before(pins: dict[str, int], before: Any) -> dict[str, int | None]:
    if not isinstance(before, dict) or any(
        table not in pins or (pin is not None and (type(pin) is not int or pin <= 0))
        for table, pin in before.items()
    ):
        raise ValueError("invalid main_before snapshots")
    return before


def publish(
    spark: SparkSession,
    layer: str,
    branch: str,
    pins: dict[str, int],
    main_before: dict[str, int | None] | None = None,
) -> dict[str, Any]:
    """Publish immutable audited snapshots; retries work after partial promotion or explicit branch cleanup.

    Private refs are created at explicit versions and never used by writers. Advancing the source audit
    branch after verification therefore cannot change the snapshots promoted to main.
    """
    safe(branch)
    if branch == "main":
        raise ValueError("main is not an audit branch")
    pins = validated_pins(layer, pins)
    before = validated_before(pins, main_before or {})
    observed = {table: snapshot(spark, table, "main") for table in TABLES[layer]}
    identity = json.dumps([branch, layer, {table: pins[table] for table in TABLES[layer]}], sort_keys=True)
    publication_ref = f"publish_{hashlib.sha256(identity.encode()).hexdigest()[:32]}"
    created: list[str] = []
    completed = False
    try:
        # Resolve every expected snapshot before moving any table's main pointer.
        for table in TABLES[layer]:
            if observed[table] != pins[table]:
                created.append(table)
                spark.sql(
                    f"ALTER TABLE {CATALOG}.{table} CREATE BRANCH IF NOT EXISTS {publication_ref} "
                    f"AS OF VERSION {pins[table]} RETAIN 1 DAYS"
                )
                if snapshot(spark, table, publication_ref) != pins[table]:
                    raise RuntimeError("publication ref no longer points at the audited snapshot")
        for table in created:
            spark.sql(
                f"CALL {CATALOG}.system.fast_forward(table => '{table}', branch => 'main', "
                f"to => '{publication_ref}')"
            )
        if any(snapshot(spark, table, "main") != pins[table] for table in TABLES[layer]):
            raise RuntimeError("main changed while publishing the audited snapshots")
        # Source branches remain available for triage and explicit `drop`. Iceberg SQL DROP has no
        # expected-snapshot condition, so an automatic drop could erase a concurrent writer's data.
        completed = True
    finally:
        cleanup_errors: list[Exception] = []
        for table in TABLES[layer]:
            try:
                spark.sql(f"ALTER TABLE {CATALOG}.{table} DROP BRANCH IF EXISTS {publication_ref}")
            except Exception as exc:
                cleanup_errors.append(exc)
                print(f"could not clean publication ref on {table}: {exc}", file=sys.stderr)
        if completed and cleanup_errors:
            raise RuntimeError(
                "publication finished but private-ref cleanup failed; publication may be retried"
            )
    return {
        table: {"main_before": before.get(table, observed[table]), **state(spark, table, branch)}
        for table in TABLES[layer]
    }


def drop(spark: SparkSession, layer: str, branch: str) -> dict[str, Any]:
    for table in TABLES[layer]:
        if spark.catalog.tableExists(f"{CATALOG}.{table}"):
            spark.sql(f"ALTER TABLE {CATALOG}.{table} DROP BRANCH IF EXISTS {safe(branch)}")
    return {t: state(spark, t, None) for t in TABLES[layer]}


def main() -> None:
    parser = argparse.ArgumentParser()
    # direct: the same build written straight to main, without a branch (WAP overhead baseline only)
    parser.add_argument("action", choices=["write", "direct", "publish", "drop", "status"])
    parser.add_argument("--layer", required=True, choices=sorted(TABLES))
    parser.add_argument("--branch", type=safe_argument, default=None)
    parser.add_argument("--source", default=SAMPLE_DIR)
    audit = parser.add_mutually_exclusive_group()
    audit.add_argument("--audit-file", help="Passing audit manifest written by soda-dq wap verify")
    audit.add_argument("--pins", help="Internal adapter: exact audited schema.table -> snapshot JSON")
    parser.add_argument("--main-before", default="{}", help="Internal adapter: pre-publication snapshot JSON")
    args = parser.parse_args()
    branch = args.branch
    if args.action not in {"status", "direct"} and not branch:
        raise SystemExit("--branch is required")
    if args.action in {"write", "publish", "drop"} and (
        branch == "main" or (branch is not None and branch.startswith("publish_"))
    ):
        parser.error("main and the reserved publish_ prefix cannot be used as audit branches")
    pins: dict[str, int] = {}
    before: dict[str, int | None] = {}
    if args.action == "publish":
        try:
            if args.audit_file:
                pins, before = read_audit(args.audit_file, args.layer, str(branch))
            else:
                pins = validated_pins(args.layer, json.loads(args.pins or "null"))
                before = validated_before(pins, json.loads(args.main_before))
        except (OSError, ValueError, TypeError) as exc:
            parser.error(str(exc))
    spark = SparkSession.builder.appName(f"wap_{args.action}_{args.layer}").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        started = datetime.now(timezone.utc)
        if args.action == "write":
            tables = write(spark, args.layer, str(branch), args.source)
        elif args.action == "direct":
            tables = write(spark, args.layer, "main", args.source)
        elif args.action == "publish":
            tables = publish(spark, args.layer, str(branch), pins, before)
        elif args.action == "drop":
            tables = drop(spark, args.layer, str(branch))
        else:
            tables = {t: state(spark, t, branch) for t in TABLES[args.layer]}
        seconds = (datetime.now(timezone.utc) - started).total_seconds()
        payload = {"action": args.action, "layer": args.layer, "branch": branch, "seconds": round(seconds, 3),
                   "tables": tables}  # fmt: skip
        print("WAP_JSON " + json.dumps(payload, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
