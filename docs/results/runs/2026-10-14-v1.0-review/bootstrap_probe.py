"""Verify first-use empty branch creation in an isolated, disposable Iceberg namespace."""

from __future__ import annotations

import importlib
import json
import sys
from uuid import uuid4


def main() -> None:
    from pyspark.sql import SparkSession

    sys.path.insert(0, "/opt/soda-wap")
    job = importlib.import_module("wap_job")
    spark = SparkSession.builder.appName("quality-gate-bootstrap-verification").getOrCreate()
    namespace = f"review_bootstrap_{uuid4().hex[:12]}"
    table = f"{namespace}.empty_table"
    try:
        # All SQL identifiers here are generated from a fixed prefix and UUID hex, never user input.
        spark.sql(f"CREATE NAMESPACE lakehouse.{namespace}")
        job.ensure_branch(spark, table, spark.range(1), "audit_bootstrap")
        actual = job.state(spark, table, "audit_bootstrap")
        assert actual["main_snapshot"] is not None
        assert actual["branch_snapshot"] == actual["main_snapshot"]
        assert actual["main_rows"] == 0
        assert actual["branch_rows"] == 0
        print("BOOTSTRAP_JSON " + json.dumps({"table": table, **actual}), flush=True)
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS lakehouse.{table} PURGE")
            spark.sql(f"DROP NAMESPACE IF EXISTS lakehouse.{namespace}")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
