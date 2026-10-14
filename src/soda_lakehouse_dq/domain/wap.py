"""Write-audit-publish (WAP) domain model.

A layer is written to an Iceberg audit branch, the gate verifies that branch, and only a passing gate
fast-forwards ``main``. These value objects carry the evidence: snapshot ids and row counts per table.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from soda_lakehouse_dq.domain.errors import DataQualityError
from soda_lakehouse_dq.domain.models import GateReport, Layer

_BRANCH = re.compile(r"^[a-z0-9_]{1,64}$")


def branch_name(run_id: str, prefix: str = "audit") -> str:
    """Iceberg branch for a run: ``audit_<run id>`` lower-cased with every other character as ``_``."""
    name = f"{prefix}_{re.sub(r'[^a-z0-9]+', '_', run_id.lower()).strip('_')}"[:64]
    return validate_branch(name)


def validate_branch(name: str) -> str:
    if not _BRANCH.fullmatch(name) or name == "main" or name.startswith("publish_"):
        raise DataQualityError(
            f"invalid audit branch {name!r}: use 1-64 of [a-z0-9_], excluding 'main' and reserved 'publish_'"
        )
    return name


@dataclass(frozen=True, slots=True)
class TableState:
    """One table as a WAP step left it."""

    table: str  # schema.table
    main_snapshot: int | None = None
    main_rows: int | None = None
    branch_snapshot: int | None = None
    branch_rows: int | None = None
    main_before: int | None = None  # main's snapshot before a publish
    refs: Mapping[str, int] = field(default_factory=dict)  # every branch/tag of the table -> snapshot id

    @property
    def main_moved(self) -> bool:
        return self.main_before is not None and self.main_before != self.main_snapshot


@dataclass(frozen=True, slots=True)
class BranchState:
    """Result of one write / publish / drop / status step for a layer."""

    action: str
    layer: Layer
    branch: str | None
    seconds: float
    tables: Mapping[str, TableState] = field(default_factory=dict)

    def snapshots(self) -> dict[str, int]:
        """``schema.table`` -> branch snapshot id, the pins the gate verifies."""
        return {t: s.branch_snapshot for t, s in self.tables.items() if s.branch_snapshot is not None}


@dataclass(frozen=True, slots=True)
class BranchAudit:
    """The gate decision and the exact immutable snapshots it inspected."""

    branch: str
    report: GateReport
    pins: Mapping[str, int]
    main_before: Mapping[str, int | None] = field(default_factory=dict)


class WapOutcome(StrEnum):
    PUBLISHED = "published"  # gate passed, main fast-forwarded to the exact audited snapshot
    BLOCKED = "blocked"  # gate blocked, main untouched, branch kept for forensics


@dataclass(frozen=True, slots=True)
class WapLayerResult:
    layer: Layer
    branch: str
    outcome: WapOutcome
    report: GateReport
    written: BranchState
    published: BranchState | None = None
    audited_pins: Mapping[str, int] = field(default_factory=dict)
