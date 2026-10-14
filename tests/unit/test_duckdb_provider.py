"""DuckDBProvider against a real in-memory DuckDB catalog (fast, no network)."""

from __future__ import annotations

import duckdb
import pytest

from soda_lakehouse_dq.adapters.datasources.duckdb_provider import DuckDBProvider
from soda_lakehouse_dq.domain.errors import DataSourceError


class Recorder:
    """Records statements; delegates to a real connection (DuckDB connections can't be patched)."""

    def __init__(self, con):
        self.con = con
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if sql.startswith(
            ("SET httpfs", "SET temp_directory")
        ):  # httpfs isn't loaded in a plain in-memory DuckDB
            return None
        return self.con.execute(sql, params) if params is not None else self.con.execute(sql)


def _catalog():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS lakehouse")
    con.execute("CREATE SCHEMA lakehouse.silver")
    con.execute("CREATE SCHEMA lakehouse.bronze")
    con.execute("CREATE TABLE lakehouse.silver.orders AS SELECT 1 AS id")
    return con


def test_open_hands_soda_the_connection_with_catalog_selected():
    con = _catalog()
    with DuckDBProvider(lambda: con, "ds", "lakehouse").open() as sources:
        assert len(sources.impls) == 1
        assert con.execute("SELECT current_database(), current_schema()").fetchone() == (
            "lakehouse",
            "bronze",
        )
        assert con.execute("SELECT count(*) FROM silver.orders").fetchone() == (1,)
        sources.prepare("ds/silver/orders")


def test_configure_sets_http_client_then_catalog():
    rec = Recorder(_catalog())
    DuckDBProvider(lambda: rec, "ds", "lakehouse", httpfs_client="httplib").configure(rec)
    assert rec.statements[0] == "SET httpfs_client_implementation = ?"
    assert rec.statements[-1] == 'USE "lakehouse"."bronze"'


def test_configure_without_http_client():
    rec = Recorder(_catalog())
    DuckDBProvider(lambda: rec, "ds", "lakehouse").configure(rec)
    assert not any("httpfs" in s for s in rec.statements)


def test_warm_describes_only_own_datasets():
    rec = Recorder(_catalog())
    provider = DuckDBProvider(lambda: rec, "ds", "lakehouse")
    provider._warm(rec, "ds/silver/orders")
    provider._warm(rec, "other/silver/orders")
    provider._warm(rec, "ds/orders")
    assert rec.statements == ['DESCRIBE "lakehouse"."silver"."orders"']


def test_missing_catalog_raises_and_closes():
    con = duckdb.connect()
    with (
        pytest.raises(DataSourceError, match="not attached"),
        DuckDBProvider(lambda: con, "ds", "lakehouse").open(),
    ):
        pass
    with pytest.raises(duckdb.ConnectionException):
        con.execute("SELECT 1")


def test_missing_table_raises_data_source_error_and_closes_connection():
    con = _catalog()
    with (
        pytest.raises(DataSourceError, match="cannot load dataset 'ds/silver/missing'"),
        DuckDBProvider(lambda: con, "ds", "lakehouse").open() as sources,
    ):
        sources.prepare("ds/silver/missing")
    with pytest.raises(duckdb.ConnectionException):
        con.execute("SELECT 1")


def test_configure_sets_temp_directory_for_read_only_containers():
    rec = Recorder(_catalog())
    DuckDBProvider(lambda: rec, "ds", "lakehouse", temp_directory="/tmp/duckdb").configure(rec)
    assert rec.statements[0] == "SET temp_directory = ?"
