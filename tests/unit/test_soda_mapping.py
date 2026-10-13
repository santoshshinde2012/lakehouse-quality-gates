from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

from soda_lakehouse_dq.adapters.datasources.base import SodaDataSources
from soda_lakehouse_dq.adapters.soda_verifier import SodaContractVerifier, map_session_result, schema_details
from soda_lakehouse_dq.domain.models import Outcome
from tests.conftest import make_spec


class SodaOutcome(Enum):  # stands in for soda_core's CheckOutcome
    PASSED = "PASSED"
    WARN = "WARN"
    FAILED = "FAILED"
    NOT_EVALUATED = "NOT_EVALUATED"


def _check(type_, outcome, name=None, column=None, metrics=None):
    return SimpleNamespace(
        check=SimpleNamespace(type=type_, name=name, column_name=column),
        outcome=outcome,
        diagnostic_metric_values=metrics,
    )


def _session(checks, errors=()):
    cvr = SimpleNamespace(check_results=checks, started_timestamp=None, ended_timestamp=None)
    return SimpleNamespace(contract_verification_results=[cvr], get_errors=lambda: list(errors))


def test_maps_outcomes_names_and_metrics():
    result = map_session_result(
        make_spec(),
        _session(
            [
                _check("missing", SodaOutcome.PASSED, "Id present", "id", {"missing_count": 0}),
                _check("invalid", SodaOutcome.WARN, column="amount"),
                _check("schema", SodaOutcome.FAILED),
                _check("metric", SodaOutcome.NOT_EVALUATED),
            ]
        ),
    )
    assert [c.outcome for c in result.checks] == [
        Outcome.PASSED,
        Outcome.WARNED,
        Outcome.FAILED,
        Outcome.NOT_EVALUATED,
    ]
    assert result.checks[0].name == "Id present"
    assert result.checks[0].metrics == {"missing_count": 0}
    assert result.checks[1].name == "invalid"  # falls back to the check type


def test_errors_are_deduplicated():
    result = map_session_result(make_spec(), _session([], errors=["x", "x", "y"]))
    assert result.errors == ("x", "y")


def test_empty_session_is_an_error():
    session = SimpleNamespace(contract_verification_results=[], get_errors=list)
    assert map_session_result(make_spec(), session).has_errors


class FakeProvider:
    engine = "duckdb"
    name = "ds"

    def __init__(self):
        self.opened = 0
        self.prepared = []

    @contextmanager
    def open(self):
        self.opened += 1
        yield SodaDataSources(impls=["impl"], yaml_files=[Path("t.yml")], prepare=self.prepared.append)


def test_schema_details_explain_drift():
    mismatch = SimpleNamespace(column="ts", expected_data_type="timestamp", actual_data_type="timestamptz")
    result = SimpleNamespace(
        expected_column_names_not_actual=["a"],
        actual_column_names_not_expected=["b"],
        column_data_type_mismatches=[mismatch],
        are_columns_out_of_order=True,
    )
    assert schema_details(result) == (
        "missing columns: a",
        "unexpected columns: b",
        "ts: expected timestamp, actual timestamptz",
        "columns are out of order",
    )
    assert schema_details(SimpleNamespace()) == ()


def test_verifier_opens_source_once_and_passes_arguments():
    calls = []

    def verify_fn(**kwargs):
        calls.append(kwargs)
        return _session([_check("row_count", SodaOutcome.PASSED)])

    provider = FakeProvider()
    verifier = SodaContractVerifier(provider, verify_fn)
    results = verifier.verify([make_spec("a"), make_spec("b")], {"K": "1"})
    assert provider.opened == 1
    assert provider.prepared == ["ds/silver/a", "ds/silver/b"]
    assert verifier.engine == "duckdb"
    assert len(results) == 2
    assert calls[0] == {
        "data_sources": ["impl"],
        "data_source_file_paths": ["t.yml"],
        "contract_file_path": "/tmp/a.yml",
        "variables": {"K": "1"},
    }
