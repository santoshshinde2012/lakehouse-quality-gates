"""Gate policy: turns contract results into a promote / block decision (single responsibility)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from soda_lakehouse_dq.domain.models import ContractResult, GateStatus, Outcome


@dataclass(frozen=True, slots=True)
class GateDecision:
    status: GateStatus
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GatePolicy:
    """Rules for blocking promotion.

    - any FAILED check blocks;
    - any contract execution error blocks (a check that could not run is not a pass);
    - NOT_EVALUATED blocks unless ``allow_not_evaluated``;
    - WARNED only blocks when ``fail_on_warn``;
    - an empty result set blocks unless ``allow_empty`` (a gate with no contracts proves nothing).
    - a contract with no check results always blocks.
    """

    fail_on_warn: bool = False
    allow_not_evaluated: bool = False
    allow_empty: bool = False

    def decide(self, results: Sequence[ContractResult]) -> GateDecision:
        blocking: list[str] = []
        warnings: list[str] = []
        if not results and not self.allow_empty:
            blocking.append("no contracts were verified")
        for result in results:
            name = result.contract.name
            if not result.checks and not result.errors:
                blocking.append(f"{name}: no checks were verified")
            blocking.extend(f"{name}: error: {e}" for e in result.errors)
            blocking.extend(
                f"{name}: failed: {_label(c.name, c.column)}"
                + (f" ({'; '.join(c.details)})" if c.details else "")
                for c in result.failed
            )
            if not self.allow_not_evaluated:
                blocking.extend(
                    f"{name}: not evaluated: {_label(c.name, c.column)}"
                    for c in result.checks
                    if c.outcome is Outcome.NOT_EVALUATED
                )
            warned = [f"{name}: warned: {_label(c.name, c.column)}" for c in result.warned]
            (blocking if self.fail_on_warn else warnings).extend(warned)
        if blocking:
            return GateDecision(GateStatus.BLOCKED, tuple(blocking + warnings))
        if warnings:
            return GateDecision(GateStatus.PASSED_WITH_WARNINGS, tuple(warnings))
        return GateDecision(GateStatus.PASSED, ())


def _label(name: str, column: str | None) -> str:
    return f"{column}: {name}" if column else name
