"""Provider abstraction used by the Soda verifier (adapter-level port)."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SodaDataSources:
    """What ``verify_contract_locally`` needs: live DataSourceImpl objects and/or YAML files."""

    impls: list[Any] = field(default_factory=list)
    yaml_files: list[Path] = field(default_factory=list)
    prepare: Callable[[str], None] = field(default=lambda _dataset: None)
    """Called with each dataset a contract reads, before Soda runs (engine-specific warm-up)."""


class DataSourceProvider(Protocol):
    engine: str
    name: str

    def open(self) -> AbstractContextManager[SodaDataSources]: ...
