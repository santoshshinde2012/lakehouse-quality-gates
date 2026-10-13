"""Shared fakes and builders. Fakes implement the domain ports structurally (no mocking library)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from soda_lakehouse_dq.domain.models import (
    CheckResult,
    ContractResult,
    ContractSpec,
    GateReport,
    GateStatus,
    Layer,
    Outcome,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXED_NOW = datetime(2026, 10, 13, 5, 30, tzinfo=UTC)


def make_spec(name: str = "orders", layer: Layer = Layer.SILVER) -> ContractSpec:
    return ContractSpec(
        name=f"retail.{layer.value}.{name}",
        pipeline="retail",
        layer=layer,
        source_path=Path(f"contracts/retail/{layer.value}/{name}.yml"),
        rendered_path=Path(f"/tmp/{name}.yml"),
        dataset=f"ds/{layer.value}/{name}",
    )


def make_result(*outcomes: Outcome, errors: tuple[str, ...] = (), name: str = "orders") -> ContractResult:
    checks = tuple(
        CheckResult(
            name=f"check{i}",
            check_type="missing",
            outcome=o,
            column="col" if i % 2 else None,
            metrics={"missing_count": i},
        )
        for i, o in enumerate(outcomes)
    )
    return ContractResult(contract=make_spec(name), checks=checks, errors=errors)


def make_report(
    status: GateStatus = GateStatus.PASSED, *results: ContractResult, reasons: tuple[str, ...] = ()
) -> GateReport:
    return GateReport(
        run_id="run-1",
        pipeline="retail",
        layer=Layer.SILVER,
        engine="fake",
        status=status,
        results=results or (make_result(Outcome.PASSED),),
        reasons=reasons,
        created_at=FIXED_NOW,
    )


class FakeClock:
    def __init__(self) -> None:
        self.ids = 0

    def now(self) -> datetime:
        return FIXED_NOW

    def new_run_id(self) -> str:
        self.ids += 1
        return f"run-{self.ids}"


@dataclass
class FakeRepository:
    by_layer: dict[Layer, list[ContractSpec]] = field(default_factory=dict)

    def list_contracts(self, pipeline: str, layer: Layer) -> Sequence[ContractSpec]:
        return self.by_layer.get(layer, [make_spec(layer=layer)])


@dataclass
class FakeVerifier:
    outcomes_by_layer: dict[Layer, tuple[Outcome, ...]] = field(default_factory=dict)
    engine: str = "fake"
    calls: list[tuple[list[str], dict[str, object]]] = field(default_factory=list)

    def verify(
        self, contracts: Sequence[ContractSpec], variables: Mapping[str, object]
    ) -> Sequence[ContractResult]:
        self.calls.append(([c.name for c in contracts], dict(variables)))
        results = []
        for c in contracts:
            outcomes = self.outcomes_by_layer.get(c.layer, (Outcome.PASSED,))
            checks = tuple(CheckResult(f"c{i}", "missing", o) for i, o in enumerate(outcomes))
            results.append(ContractResult(contract=c, checks=checks))
        return results


@dataclass
class RecordingSink:
    reports: list[GateReport] = field(default_factory=list)

    def write(self, report: GateReport) -> None:
        self.reports.append(report)


@dataclass
class RecordingNotifier:
    reports: list[GateReport] = field(default_factory=list)
    explode: bool = False

    def notify(self, report: GateReport) -> None:
        if self.explode:
            raise RuntimeError("boom")
        self.reports.append(report)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture(autouse=True)
def _forget_lakehouse_client():
    """Tests install fake ``lakehouse_client`` packages; never let one leak into the next test."""
    import sys

    sys.modules.pop("lakehouse_client", None)
    yield
    sys.modules.pop("lakehouse_client", None)
