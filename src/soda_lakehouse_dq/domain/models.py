"""Immutable value objects describing contracts, check outcomes and gate decisions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path

VariableValue = str | int | float
"""Contract variable value. Soda only accepts numbers for numeric keys such as thresholds."""


def coerce_variable(raw: str) -> VariableValue:
    """``"48"`` -> 48, ``"2.5"`` -> 2.5, anything else stays a string.

    Soda 4.26 rejects a string where a threshold expects a number (``--set MAX=48`` from a CLI arrives
    as text), so values that look numeric are converted before verification.
    """
    for kind in (int, float):
        try:
            return kind(raw)
        except ValueError:
            continue
    return raw


class Layer(StrEnum):
    """Medallion layers, in promotion order."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

    @classmethod
    def ordered(cls) -> tuple[Layer, ...]:
        return (cls.BRONZE, cls.SILVER, cls.GOLD)


class Outcome(StrEnum):
    """Engine-agnostic outcome of one check (mirrors Soda's CheckOutcome)."""

    PASSED = "passed"
    WARNED = "warned"
    FAILED = "failed"
    NOT_EVALUATED = "not_evaluated"
    EXCLUDED = "excluded"


class GateStatus(StrEnum):
    """What the gate decided for a layer."""

    PASSED = "passed"
    PASSED_WITH_WARNINGS = "passed_with_warnings"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class ContractSpec:
    """A contract ready to verify: a rendered YAML file plus where it came from."""

    name: str
    pipeline: str
    layer: Layer
    source_path: Path
    rendered_path: Path
    dataset: str
    references: tuple[str, ...] = ()
    """Other datasets read via reference checks or explicit ``attributes.read_dependencies``."""

    @property
    def datasets(self) -> tuple[str, ...]:
        return (self.dataset, *self.references)


@dataclass(frozen=True, slots=True)
class CheckResult:
    """The result of one check inside a contract."""

    name: str
    check_type: str
    outcome: Outcome
    column: str | None = None
    metrics: dict[str, float | int | str] = field(default_factory=dict)
    details: tuple[str, ...] = ()
    """Human-readable diagnostics (e.g. which columns drifted in a schema check)."""


@dataclass(frozen=True, slots=True)
class ContractResult:
    """The verification result of one contract."""

    contract: ContractSpec
    checks: tuple[CheckResult, ...]
    errors: tuple[str, ...] = ()
    started_at: datetime | None = None
    ended_at: datetime | None = None

    def count(self, outcome: Outcome) -> int:
        return sum(1 for c in self.checks if c.outcome is outcome)

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)

    @property
    def failed(self) -> tuple[CheckResult, ...]:
        return tuple(c for c in self.checks if c.outcome is Outcome.FAILED)

    @property
    def warned(self) -> tuple[CheckResult, ...]:
        return tuple(c for c in self.checks if c.outcome is Outcome.WARNED)


@dataclass(frozen=True, slots=True)
class GateReport:
    """Everything one gate run produced: the input results and the decision."""

    run_id: str
    pipeline: str
    layer: Layer
    engine: str
    status: GateStatus
    results: tuple[ContractResult, ...]
    reasons: tuple[str, ...]
    created_at: datetime

    @property
    def blocked(self) -> bool:
        return self.status is GateStatus.BLOCKED

    def total(self, outcome: Outcome) -> int:
        return sum(r.count(outcome) for r in self.results)

    @property
    def total_checks(self) -> int:
        return sum(len(r.checks) for r in self.results)
