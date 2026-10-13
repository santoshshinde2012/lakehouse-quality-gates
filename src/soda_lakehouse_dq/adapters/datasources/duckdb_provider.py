"""DuckDB providers.

Soda's DuckDB dialect has a single namespace prefix (schema), so contracts address tables as
``<datasource>/<schema>/<table>``. We ``USE`` the attached catalog so ``silver.orders`` resolves to
``lakehouse.silver.orders`` and Soda's information_schema lookups match on schema name.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from soda_lakehouse_dq.adapters.datasources.base import SodaDataSources
from soda_lakehouse_dq.adapters.lakehouse_repo import import_lakehouse_client
from soda_lakehouse_dq.domain.errors import DataSourceError

ConnectionFactory = Callable[[], Any]


def _quote_ident(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise DataSourceError(f"refusing unsafe identifier {name!r}")
    return f'"{name}"'


def apply_session_settings(
    con: Any, httpfs_client: str | None = None, temp_directory: str | None = None
) -> None:
    """Container-friendly DuckDB session settings, shared by the provider and ``soda-dq seed``.

    * ``httpfs_client``: inside a container the curl client resolves ``*.localhost`` (the S3 endpoint
      Lakekeeper vends) to loopback; ``httplib`` asks Docker's DNS, which knows the objectstore alias.
    * ``temp_directory``: DuckDB spills to ``./.tmp`` by default, which fails on a read-only root
      filesystem; point it at a writable tmpfs.
    """
    if httpfs_client:
        con.execute("SET httpfs_client_implementation = ?", [httpfs_client])
    if temp_directory:
        con.execute("SET temp_directory = ?", [temp_directory])


class DuckDBProvider:
    """Wraps any DuckDB connection factory; Soda reuses the connection via ``from_existing_cursor``."""

    engine = "duckdb"

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        name: str,
        catalog_alias: str,
        httpfs_client: str | None = None,
        temp_directory: str | None = None,
    ) -> None:
        self.name = name
        self._factory = connection_factory
        self._alias = catalog_alias
        self._httpfs_client = httpfs_client
        self._temp_directory = temp_directory

    @contextmanager
    def open(self) -> Iterator[SodaDataSources]:
        from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

        con = self._factory()
        try:
            self.configure(con)
            yield SodaDataSources(
                impls=[DuckDBDataSourceImpl.from_existing_cursor(con, name=self.name)],
                prepare=lambda dataset: self._warm(con, dataset),
            )
        finally:
            con.close()

    def configure(self, con: Any) -> None:
        """Session setup before Soda gets the connection: container settings, then the current catalog."""
        apply_session_settings(con, self._httpfs_client, self._temp_directory)
        con.execute(f"USE {self._default_schema(con)}")

    def _warm(self, con: Any, dataset: str) -> None:
        """Load a table's metadata before Soda reads ``information_schema``.

        DuckDB's iceberg extension lists REST-catalog tables lazily: until a table is referenced,
        ``information_schema.columns`` shows a single ``__ UNKNOWN`` placeholder column, which makes
        Soda's schema check fail. ``DESCRIBE`` loads the real columns.
        """
        parts = dataset.split("/")
        if len(parts) < 3 or parts[0] != self.name:  # noqa: PLR2004 (datasource/schema/table)
            return
        schema, table = parts[-2], parts[-1]
        con.execute(f"DESCRIBE {_quote_ident(self._alias)}.{_quote_ident(schema)}.{_quote_ident(table)}")

    def _default_schema(self, con: Any) -> str:
        """``catalog.schema`` to USE. An attached Iceberg REST catalog has no ``main`` schema, so a bare
        ``USE lakehouse`` fails; any existing schema makes the catalog current, after which
        ``silver.orders`` resolves to ``lakehouse.silver.orders``."""
        rows = con.execute(
            "SELECT schema_name FROM duckdb_schemas() WHERE database_name = ? ORDER BY schema_name",
            [self._alias],
        ).fetchall()
        if not rows:
            raise DataSourceError(f"catalog {self._alias!r} is not attached or has no schemas")
        return f"{_quote_ident(self._alias)}.{_quote_ident(str(rows[0][0]))}"


def lakehouse_connection_factory(
    ldl_repo_path: Path, lakekeeper_url: str, warehouse: str, alias: str
) -> ConnectionFactory:
    """Factory calling local-data-lakehouse's own ``lakehouse_client.duckdb_connect()``.

    The repo is referenced by path (``DQ_LDL_REPO_PATH``), not vendored: its ``src`` goes on
    ``sys.path`` so the exact ATTACH (Iceberg REST + vended credentials) logic is reused.
    """

    def factory() -> Any:
        client = import_lakehouse_client(ldl_repo_path)
        try:
            return client.duckdb_connect(url=lakekeeper_url, wh=warehouse, alias=alias)
        except Exception as exc:
            raise DataSourceError(f"cannot attach Lakekeeper at {lakekeeper_url}: {exc}") from exc

    return factory
