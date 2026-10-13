"""Build the retail medallion in an in-memory DuckDB catalog from CSV files.

Mirrors local-data-lakehouse's ``sql/retail/{bronze_ddl,silver_orders,gold_daily_metrics}.sql`` in DuckDB
SQL so contracts can be proven without Docker. The catalog is attached as ``lakehouse`` so contracts see
the same ``bronze/silver/gold`` schemas as on the real Iceberg catalog.

Types follow what Spark writes to Iceberg: timestamps are ``timestamp with time zone``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from soda_lakehouse_dq.domain.errors import DataSourceError

ORDER_FILES = "orders_*.csv"
CUSTOMER_FILE = "customers.csv"

_BRONZE_ORDERS_DDL = """
CREATE TABLE lakehouse.bronze.orders_raw (order_id VARCHAR, customer_id VARCHAR, order_ts TIMESTAMPTZ,
  status VARCHAR, amount DOUBLE, _source_file VARCHAR, _ingested_at TIMESTAMPTZ)
"""

# One second apart per file so "latest _ingested_at wins" dedup behaves like successive Spark runs.
_BRONZE_ORDERS_INSERT = """
INSERT INTO lakehouse.bronze.orders_raw
SELECT order_id, customer_id, CAST(order_ts AS TIMESTAMPTZ), status, CAST(amount AS DOUBLE), ?,
       CAST(? AS TIMESTAMPTZ) + to_seconds(?)
FROM read_csv(?, header = true, all_varchar = true)
"""

_BRONZE_CUSTOMERS = """
CREATE TABLE lakehouse.bronze.customers_raw AS
SELECT customer_id, name, email, city, CAST(signup_date AS DATE) AS signup_date, ? AS _source_file,
       CAST(? AS TIMESTAMPTZ) AS _ingested_at
FROM read_csv(?, header = true, all_varchar = true)
"""

_SILVER_ORDERS = """
CREATE TABLE lakehouse.silver.orders AS
SELECT order_id, customer_id, CAST(order_ts AS TIMESTAMPTZ) AS order_ts, LOWER(TRIM(status)) AS status,
       CAST(amount AS DOUBLE) AS amount, _source_file, _ingested_at
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_ts DESC, _ingested_at DESC) AS rn
      FROM lakehouse.bronze.orders_raw)
WHERE rn = 1 AND LOWER(TRIM(status)) IN ('paid', 'shipped', 'cancelled', 'refunded')
"""

_SILVER_CUSTOMERS = """
CREATE TABLE lakehouse.silver.customers AS
SELECT customer_id, TRIM(name) AS name, LOWER(TRIM(email)) AS email, TRIM(city) AS city,
       CAST(signup_date AS DATE) AS signup_date, _source_file, _ingested_at
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) AS rn
      FROM lakehouse.bronze.customers_raw)
WHERE rn = 1
"""

_GOLD = """
CREATE TABLE lakehouse.gold.daily_order_metrics AS
SELECT CAST(order_ts AS DATE) AS order_date, CAST(COUNT(*) AS BIGINT) AS orders,
       SUM(CASE WHEN status IN ('paid', 'shipped') THEN amount ELSE 0 END) AS revenue,
       AVG(CASE WHEN status IN ('paid', 'shipped') THEN amount END) AS avg_order_value,
       CAST(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS BIGINT) AS cancelled_orders,
       CAST(SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS BIGINT) AS refunded_orders
FROM lakehouse.silver.orders
GROUP BY CAST(order_ts AS DATE)
ORDER BY order_date
"""


TABLES = (
    "gold.daily_order_metrics",
    "silver.orders",
    "silver.customers",
    "bronze.orders_raw",
    "bronze.customers_raw",
)


def load_medallion(con: Any, sample_dir: Path, ingested_at: str | None = None) -> dict[str, int]:
    """(Re)build bronze, silver and gold in the catalog attached as ``lakehouse`` from CSV files.

    Works on an in-memory catalog and on the live Iceberg REST catalog (DuckDB's iceberg extension
    supports CREATE SCHEMA / DROP TABLE / CREATE TABLE AS / INSERT). Returns row counts per table.
    """
    orders = sorted(sample_dir.glob(ORDER_FILES))
    customers = sample_dir / CUSTOMER_FILE
    if not orders or not customers.is_file():
        raise DataSourceError(f"{sample_dir} needs {CUSTOMER_FILE} and {ORDER_FILES}")
    for schema in ("bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema}")
    for table in TABLES:
        con.execute(f"DROP TABLE IF EXISTS lakehouse.{table}")
    stamp = ingested_at or datetime.now(UTC).isoformat()
    con.execute(_BRONZE_ORDERS_DDL)
    for index, path in enumerate(orders):
        con.execute(_BRONZE_ORDERS_INSERT, [path.name, stamp, index, str(path)])
    con.execute(_BRONZE_CUSTOMERS, [customers.name, stamp, str(customers)])
    for statement in (_SILVER_ORDERS, _SILVER_CUSTOMERS, _GOLD):
        con.execute(statement)
    counts: dict[str, int] = {}
    for table in reversed(TABLES):
        row = con.execute(f"SELECT COUNT(*) FROM lakehouse.{table}").fetchone()  # noqa: S608 (constant names)
        counts[table] = int(row[0]) if row else 0
    return counts


class LocalMedallionBuilder:
    """Callable connection factory: an in-memory DuckDB with bronze, silver and gold loaded."""

    def __init__(self, sample_dir: Path, ingested_at: str | None = None) -> None:
        self._dir = sample_dir
        self._ingested_at = ingested_at

    def __call__(self) -> Any:
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("SET TimeZone = 'UTC'")
        con.execute("ATTACH ':memory:' AS lakehouse")
        load_medallion(con, self._dir, self._ingested_at)
        return con
