"""Real Soda Core 4.26 verification against DuckDB built from the CSV fixtures (no containers)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from soda_core.contracts import test_contract as soda_test_contract

from soda_lakehouse_dq.adapters.contracts import FileContractRepository
from soda_lakehouse_dq.adapters.datasources.duckdb_provider import DuckDBProvider
from soda_lakehouse_dq.adapters.local_medallion import LocalMedallionBuilder
from soda_lakehouse_dq.adapters.soda_verifier import SodaContractVerifier
from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.application.verify_layer import RunMedallionUseCase, VerifyLayerUseCase
from soda_lakehouse_dq.container import build_container
from soda_lakehouse_dq.domain.models import GateStatus, Layer, Outcome
from soda_lakehouse_dq.settings import Engine, Settings
from tests.conftest import PROJECT_ROOT

pytestmark = pytest.mark.integration


def _container(tmp_path, batch, contracts_dir=None):
    return build_container(
        Settings(
            _env_file=None,
            engine=Engine.DUCKDB_LOCAL,
            reports_dir=tmp_path,
            sample_dir=PROJECT_ROOT / "fixtures" / batch,
            contracts_dir=contracts_dir or PROJECT_ROOT / "contracts",
            iceberg_sink=False,
            slack_webhook_url=None,
        )
    )


def _failed(report):
    return {c.name for r in report.results for c in r.checks if c.outcome is Outcome.FAILED}


def _verify_mutated_good_batch(tmp_path, layer, mutation):
    def connection():
        con = LocalMedallionBuilder(PROJECT_ROOT / "fixtures" / "good_batch")()
        con.execute(mutation)
        return con

    return VerifyLayerUseCase(
        repository=FileContractRepository(
            PROJECT_ROOT / "contracts", tmp_path, {"dataset_root": "lakehouse_duckdb"}
        ),
        verifier=SodaContractVerifier(DuckDBProvider(connection, "lakehouse_duckdb", "lakehouse")),
        policy=GatePolicy(),
        clock=_Clock(),
    ).execute("retail", layer)


def test_good_batch_passes_every_layer(tmp_path):
    reports = _container(tmp_path, "good_batch").run_medallion.execute("retail")
    assert [r.status for r in reports] == [GateStatus.PASSED] * 3
    assert [r.total_checks for r in reports] == [14, 22, 17]
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
        "retail", Layer.SILVER, {"MAX_ORDER_AMOUNT": 100000}
    )
    ceiling_checks = [
        c for r in report.results for c in r.checks if c.name == "Amount below the outlier ceiling"
    ]
    assert len(ceiling_checks) == 1
    assert ceiling_checks[0].outcome is Outcome.PASSED
    assert not any(r.errors for r in report.results)
    assert report.status is GateStatus.BLOCKED


def test_a_contract_without_checks_blocks_the_layer(tmp_path):
    contracts_dir = tmp_path / "contracts"
    folder = contracts_dir / "retail" / "silver"
    folder.mkdir(parents=True)
    (folder / "empty.yml").write_text('dataset: "{{ dataset_root }}/silver/orders"\ncolumns: []\n')
    (folder / "nonempty.yml").write_text(
        'dataset: "{{ dataset_root }}/silver/orders"\ncolumns: []\nchecks:\n  - row_count:\n'
    )
    report = _container(tmp_path / "reports", "good_batch", contracts_dir).verify_layer.execute(
        "retail", Layer.SILVER
    )
    assert report.status is GateStatus.BLOCKED
    assert report.total_checks == 1
    assert report.results[0].errors == ("Soda returned no check results",)
    assert report.results[1].checks[0].outcome is Outcome.PASSED


@pytest.mark.parametrize(
    ("invalid_body", "diagnostic"),
    [
        ("columns: []\nchecks: []\n", "must not be an empty list"),
        ("columns: []\ncolumns: []\nchecks:\n  - row_count:\n", "YAML syntax error"),
    ],
)
def test_parser_error_blocks_and_is_reported_while_other_contracts_are_verified(
    tmp_path, invalid_body, diagnostic
):
    import json

    contracts_dir = tmp_path / "contracts"
    folder = contracts_dir / "retail" / "silver"
    folder.mkdir(parents=True)
    (folder / "a_malformed.yml").write_text('dataset: "{{ dataset_root }}/silver/orders"\n' + invalid_body)
    (folder / "z_valid.yml").write_text(
        'dataset: "{{ dataset_root }}/silver/orders"\ncolumns: []\nchecks:\n  - row_count:\n'
    )
    reports_dir = tmp_path / "reports"
    report = _container(reports_dir, "good_batch", contracts_dir).verify_layer.execute("retail", Layer.SILVER)
    assert report.status is GateStatus.BLOCKED
    assert len(report.results) == 2
    assert diagnostic in report.results[0].errors[0]
    assert report.results[1].checks[0].outcome is Outcome.PASSED
    saved = json.loads(next(reports_dir.glob("*.json")).read_text())
    assert saved["status"] == "blocked"
    assert saved["contracts"][0]["errors"] == list(report.results[0].errors)


@pytest.mark.parametrize(
    ("layer", "mutation", "failed_check"),
    [
        (Layer.SILVER, "UPDATE lakehouse.silver.orders SET status = NULL", "Status present"),
        (
            Layer.GOLD,
            "UPDATE lakehouse.gold.daily_order_metrics SET orders = NULL",
            "Order count present",
        ),
        (Layer.GOLD, "UPDATE lakehouse.gold.daily_order_metrics SET revenue = NULL", "Revenue present"),
        (
            Layer.GOLD,
            "UPDATE lakehouse.gold.daily_order_metrics SET cancelled_orders = NULL",
            "Cancelled order count present",
        ),
        (
            Layer.GOLD,
            "UPDATE lakehouse.gold.daily_order_metrics SET refunded_orders = NULL",
            "Refunded order count present",
        ),
        (
            Layer.GOLD,
            "UPDATE lakehouse.gold.daily_order_metrics SET cancelled_orders = -10",
            "Cancelled order count is not negative",
        ),
        (
            Layer.GOLD,
            "UPDATE lakehouse.gold.daily_order_metrics SET refunded_orders = -20",
            "Refunded order count is not negative",
        ),
    ],
)
def test_required_values_and_counter_bounds_block_corrupted_tables(tmp_path, layer, mutation, failed_check):
    report = _verify_mutated_good_batch(tmp_path, layer, mutation)
    assert report.status is GateStatus.BLOCKED
    assert failed_check in _failed(report)


def test_gold_allows_null_average_when_orders_are_cancelled(tmp_path):
    report = _verify_mutated_good_batch(
        tmp_path,
        Layer.GOLD,
        "UPDATE lakehouse.silver.orders SET status = 'cancelled'; "
        "UPDATE lakehouse.gold.daily_order_metrics SET revenue = 0, avg_order_value = NULL, "
        "cancelled_orders = orders, refunded_orders = 0",
    )
    assert report.status is GateStatus.PASSED
    assert all(c.outcome is Outcome.PASSED for r in report.results for c in r.checks)


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
