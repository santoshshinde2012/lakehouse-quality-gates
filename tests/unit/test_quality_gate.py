from __future__ import annotations

import pytest

from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.domain.models import GateStatus, Outcome
from tests.conftest import make_result


@pytest.mark.parametrize(
    ("outcomes", "policy", "expected"),
    [
        ((Outcome.PASSED, Outcome.PASSED), GatePolicy(), GateStatus.PASSED),
        ((Outcome.PASSED, Outcome.WARNED), GatePolicy(), GateStatus.PASSED_WITH_WARNINGS),
        ((Outcome.PASSED, Outcome.WARNED), GatePolicy(fail_on_warn=True), GateStatus.BLOCKED),
        ((Outcome.FAILED,), GatePolicy(), GateStatus.BLOCKED),
        ((Outcome.NOT_EVALUATED,), GatePolicy(), GateStatus.BLOCKED),
        ((Outcome.NOT_EVALUATED,), GatePolicy(allow_not_evaluated=True), GateStatus.PASSED),
        ((Outcome.EXCLUDED, Outcome.PASSED), GatePolicy(), GateStatus.PASSED),
    ],
)
def test_decide_outcomes(outcomes, policy, expected):
    assert policy.decide([make_result(*outcomes)]).status is expected


def test_execution_errors_block_even_when_checks_pass():
    decision = GatePolicy().decide([make_result(Outcome.PASSED, errors=("connection refused",))])
    assert decision.status is GateStatus.BLOCKED
    assert decision.reasons == ("retail.silver.orders: error: connection refused",)


def test_empty_results_block_unless_allowed():
    assert GatePolicy().decide([]).status is GateStatus.BLOCKED
    assert GatePolicy(allow_empty=True).decide([]).status is GateStatus.PASSED


def test_reasons_list_failures_before_warnings_and_name_columns():
    decision = GatePolicy().decide([make_result(Outcome.WARNED, Outcome.FAILED)])
    assert decision.reasons == (
        "retail.silver.orders: failed: col: check1",
        "retail.silver.orders: warned: check0",
    )


def test_failure_reason_includes_details():
    from soda_lakehouse_dq.domain.models import CheckResult, ContractResult
    from tests.conftest import make_spec

    result = ContractResult(
        make_spec(), (CheckResult("Schema", "schema", Outcome.FAILED, details=("x drifted",)),)
    )
    assert GatePolicy().decide([result]).reasons == ("retail.silver.orders: failed: Schema (x drifted)",)
