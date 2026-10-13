"""Trino provider: a Soda data source YAML file (``datasources/trino.yml``); requires ``soda-trino``."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

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
        # The YAML reads ${env.DQ_TRINO_HOST} / ${env.DQ_TRINO_PORT}; fill them from settings if unset.
        for key, value in self._env.items():
            os.environ.setdefault(key, value)
        yield SodaDataSources(yaml_files=[self._yaml_file])
