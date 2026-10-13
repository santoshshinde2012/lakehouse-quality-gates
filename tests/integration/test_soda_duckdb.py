"""Real Soda Core 4.26 verification against DuckDB built from the CSV fixtures (no containers)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from soda_core.contracts import test_contract as soda_test_contract

from soda_lakehouse_dq.application.verify_layer import RunMedallionUseCase
from soda_lakehouse_dq.container import build_container
from soda_lakehouse_dq.domain.models import GateStatus, Layer, Outcome
from soda_lakehouse_dq.settings import Engine, Settings
from tests.conftest import PROJECT_ROOT

pytestmark = pytest.mark.integration


def _container(tmp_path, batch):
    return build_container(
        Settings(
            engine=Engine.DUCKDB_LOCAL, reports_dir=tmp_path, sample_dir=PROJECT_ROOT / "fixtures" / batch
        )
    )


def _failed(report):
    return {c.name for r in report.results for c in r.checks if c.outcome is Outcome.FAILED}


def test_good_batch_passes_every_layer(tmp_path):
    reports = _container(tmp_path, "good_batch").run_medallion.execute("retail")
    assert [r.status for r in reports] == [GateStatus.PASSED] * 3
    assert all(c.outcome is Outcome.PASSED for r in reports for res in r.results for c in res.checks)
    assert len(list(tmp_path.glob("*.json"))) == 3


@pytest.mark.parametrize(
    ("layer", "expected_failed", "expected_warned"),
    [
        (
            Layer.BRONZE,
            {"No order duplicated inside one source file", "Every order has a customer"},
            {"Status is a known value (case-insensitive)", "Amount is not negative (enforced in silver)"},
        ),
        (
            Layer.SILVER,
            {
                "Email is well-formed",
                "Email unique",
                "No order timestamped in the future",
                "Customer id present",
                "Customer exists in silver.customers",
                "Amount is not negative",
            },
            {"Amount below the outlier ceiling"},
        ),
        (Layer.GOLD, {"No metrics for future days"}, {"Average order value is plausible"}),
    ],
)
def test_bad_batch_blocks_with_expected_checks(tmp_path, layer, expected_failed, expected_warned):
    report = _container(tmp_path, "bad_batch").verify_layer.execute("retail", layer)
    assert report.status is GateStatus.BLOCKED
    assert _failed(report) == expected_failed
    assert {c.name for r in report.results for c in r.warned} == expected_warned
    assert not any(r.errors for r in report.results)


def test_bad_batch_medallion_stops_at_bronze(tmp_path):
    container = _container(tmp_path, "bad_batch")
    reports = RunMedallionUseCase(container.verify_layer, _Clock()).execute("retail")
    assert [r.layer for r in reports] == [Layer.BRONZE]


def test_warn_only_variable_override_unblocks_nothing_but_changes_threshold(tmp_path):
    # Raising the ceiling via a contract variable removes the silver outlier warning.
    report = _container(tmp_path, "bad_batch").verify_layer.execute(
        "retail", Layer.SILVER, {"MAX_ORDER_AMOUNT": "100000"}
    )
    assert "Amount below the outlier ceiling" not in {c.name for r in report.results for c in r.warned}


def test_every_contract_parses_with_soda(tmp_path):
    container = _container(tmp_path, "good_batch")
    for layer in Layer.ordered():
        for spec in container.repository.list_contracts("retail", layer):
            assert soda_test_contract(contract_file_path=str(spec.rendered_path)).get_errors() == [], (
                spec.name
            )


class _Clock:
    def now(self):
        return datetime.now(UTC)

    def new_run_id(self):
        return "it"
