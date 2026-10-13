"""Rebuild the live medallion from a CSV batch, timing bronze, silver and gold separately.

Same SQL as ``soda-dq seed`` (adapters/local_medallion.py), executed step by step through
local-data-lakehouse's DuckDB client against the live Iceberg REST catalog. This is the benchmark's
"pipeline" time per layer at every scale (the Spark jobs only run on the lakehouse's own sample).
"""

from __future__ import annotations

import contextlib
import json
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from soda_lakehouse_dq.adapters import local_medallion as lm
from soda_lakehouse_dq.adapters.datasources.duckdb_provider import (
    apply_session_settings,
    lakehouse_connection_factory,
)
from soda_lakehouse_dq.settings import Settings


def seed(sample_dir: Path) -> dict[str, Any]:
    settings = Settings()
    con = lakehouse_connection_factory(
        settings.ldl_repo_path,
        settings.lakekeeper_url,
        settings.lakekeeper_warehouse,
        settings.catalog_alias,
    )()
    source = sample_dir.resolve()
    orders = sorted(source.glob(lm.ORDER_FILES))
    customers = source / lm.CUSTOMER_FILE
    stamp = datetime.now(UTC).isoformat()
    timings: dict[str, float] = {}
    try:
        apply_session_settings(con, settings.duckdb_httpfs_client, settings.duckdb_temp_directory)
        with tempfile.TemporaryDirectory() as scratch, contextlib.chdir(scratch):
            start = time.perf_counter()
            for schema in ("bronze", "silver", "gold"):
                con.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema}")
            for table in lm.TABLES:
                con.execute(f"DROP TABLE IF EXISTS lakehouse.{table}")
            timings["drop_s"] = time.perf_counter() - start
            start = time.perf_counter()
            con.execute(lm._BRONZE_ORDERS_DDL)
            for index, path in enumerate(orders):
                con.execute(lm._BRONZE_ORDERS_INSERT, [path.name, stamp, index, str(path)])
            con.execute(lm._BRONZE_CUSTOMERS, [customers.name, stamp, str(customers)])
            timings["bronze_s"] = time.perf_counter() - start
            start = time.perf_counter()
            con.execute(lm._SILVER_ORDERS)
            con.execute(lm._SILVER_CUSTOMERS)
            timings["silver_s"] = time.perf_counter() - start
            start = time.perf_counter()
            con.execute(lm._GOLD)
            timings["gold_s"] = time.perf_counter() - start
        counts = {}
        for table in lm.TABLES:
            row = con.execute(f"SELECT COUNT(*) FROM lakehouse.{table}").fetchone()
            counts[table] = int(row[0]) if row else 0
    finally:
        con.close()
    return {"timings": {k: round(v, 3) for k, v in timings.items()}, "counts": counts}


if __name__ == "__main__":
    sys.stdout.write("BENCH_JSON " + json.dumps(seed(Path(sys.argv[1]))) + "\n")
