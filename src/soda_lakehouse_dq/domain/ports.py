"""Ports (interfaces) the application layer depends on. Adapters implement them.

Dependency inversion: the application never imports Soda, DuckDB, Slack or the file system directly.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

from soda_lakehouse_dq.domain.models import ContractResult, ContractSpec, GateReport, Layer, VariableValue
from soda_lakehouse_dq.domain.wap import BranchAudit, BranchState


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


# --- write-audit-publish (ADR 0006) -----------------------------------------------------------------


@runtime_checkable
class AuditWriter(Protocol):
    """Builds a layer's tables on an Iceberg audit branch; ``main`` is never written."""

    def write(self, layer: Layer, branch: str, source: str | None = None) -> BranchState: ...


@runtime_checkable
class BranchPublisher(Protocol):
    """Moves ``main`` to exact audited snapshots, or explicitly removes an audit branch."""

    def publish(
        self,
        layer: Layer,
        branch: str,
        pins: Mapping[str, int],
        *,
        main_before: Mapping[str, int | None] | None = None,
    ) -> BranchState: ...

    def drop(self, layer: Layer, branch: str) -> BranchState: ...


@runtime_checkable
class SnapshotReader(Protocol):
    """Reads Iceberg refs from the catalog: the snapshot id a branch (or ``main``) points at."""

    def snapshot(self, table: str, ref: str) -> int | None: ...


@runtime_checkable
class BranchGate(Protocol):
    """Runs a layer's gate against pinned snapshots (``schema.table`` -> snapshot id) instead of main."""

    def verify(self, pipeline: str, layer: Layer, pins: Mapping[str, int], run_id: str) -> GateReport: ...


class BranchAuditSink(Protocol):
    """Durably records the snapshots and decision before any publication."""

    def write(self, audit: BranchAudit) -> None: ...
