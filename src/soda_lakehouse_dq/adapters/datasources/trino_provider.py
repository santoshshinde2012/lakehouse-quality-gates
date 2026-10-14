"""Trino provider: a Soda data source YAML file (``datasources/trino.yml``); requires ``soda-trino``."""

from __future__ import annotations

import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

import yaml

from soda_lakehouse_dq.adapters.datasources.base import SodaDataSources
from soda_lakehouse_dq.domain.errors import DataSourceError


class TrinoProvider:
    engine = "trino"

    def __init__(self, yaml_file: Path, name: str, env: Mapping[str, str] | None = None) -> None:
        self.name = name
        self._yaml_file = yaml_file
        self._env = dict(env or {})

    @contextmanager
    def open(self) -> Iterator[SodaDataSources]:
        if not self._yaml_file.is_file():
            raise DataSourceError(f"Trino data source file not found: {self._yaml_file}")
        try:
            document = yaml.safe_load(self._yaml_file.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise DataSourceError(f"cannot read Trino data source {self._yaml_file}: {exc}") from exc
        if not isinstance(document, dict) or not isinstance(document.get("connection"), dict):
            raise DataSourceError(f"Trino data source {self._yaml_file} needs a connection mapping")
        # Give each verification its own configuration. Process-wide environment mutations can
        # redirect a subsequent provider (or a concurrent verification) to another coordinator.
        document["name"] = self.name
        connection = document["connection"]
        if "DQ_TRINO_HOST" in self._env:
            connection["host"] = self._env["DQ_TRINO_HOST"]
        if "DQ_TRINO_PORT" in self._env:
            try:
                connection["port"] = int(self._env["DQ_TRINO_PORT"])
            except ValueError as exc:
                raise DataSourceError("DQ_TRINO_PORT must be an integer") from exc
        with tempfile.TemporaryDirectory(prefix="soda-trino-") as scratch:
            rendered = Path(scratch) / self._yaml_file.name
            rendered.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
            yield SodaDataSources(yaml_files=[rendered])
