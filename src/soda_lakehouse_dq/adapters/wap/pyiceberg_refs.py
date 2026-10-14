"""Reads Iceberg refs (branch -> snapshot id) from the REST catalog with PyIceberg.

Only table metadata is loaded (the catalog returns it in ``loadTable``); no data files are read, so this
works on the host and inside ``ldl-soda`` alike.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from soda_lakehouse_dq.domain.errors import DataSourceError


class PyIcebergSnapshotReader:
    def __init__(self, catalog_factory: Callable[[], Any]) -> None:
        self._factory = catalog_factory
        self._catalog: Any = None

    def snapshot(self, table: str, ref: str) -> int | None:
        from pyiceberg.exceptions import NoSuchTableError

        if self._catalog is None:
            self._catalog = self._factory()
        try:
            refs = self._catalog.load_table(table).metadata.refs
        except NoSuchTableError:
            return None
        except Exception as exc:
            raise DataSourceError(f"cannot read refs of {table}: {exc}") from exc
        found = refs.get(ref)
        return None if found is None else int(found.snapshot_id)
