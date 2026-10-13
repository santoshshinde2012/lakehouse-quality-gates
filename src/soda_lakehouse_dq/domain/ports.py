"""Ports (interfaces) the application layer depends on. Adapters implement them.

Dependency inversion: the application never imports Soda, DuckDB, Slack or the file system directly.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

from soda_lakehouse_dq.domain.models import ContractResult, ContractSpec, GateReport, Layer, VariableValue


@runtime_checkable
class ContractRepository(Protocol):
    """Finds and renders the contracts of a pipeline layer."""

    def list_contracts(self, pipeline: str, layer: Layer) -> Sequence[ContractSpec]: ...


@runtime_checkable
class ContractVerifier(Protocol):
    """Verifies contracts against a data source and returns domain results."""

    engine: str

    def verify(
        self, contracts: Sequence[ContractSpec], variables: Mapping[str, VariableValue]
    ) -> Sequence[ContractResult]: ...


@runtime_checkable
class ResultSink(Protocol):
    """Persists a gate report (JSON file, JUnit XML, Markdown, a table...)."""

    def write(self, report: GateReport) -> None: ...


@runtime_checkable
class Notifier(Protocol):
    """Tells people about a gate report (console, Slack...)."""

    def notify(self, report: GateReport) -> None: ...


class Clock(Protocol):
    """Source of time and run ids, injected so tests are deterministic."""

    def now(self) -> datetime: ...

    def new_run_id(self) -> str: ...
