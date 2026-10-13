from __future__ import annotations

from dataclasses import dataclass

import pytest
from typer.testing import CliRunner

from soda_lakehouse_dq import cli
from soda_lakehouse_dq.domain.errors import ContractNotFoundError
from soda_lakehouse_dq.domain.models import GateStatus
from tests.conftest import make_report

runner = CliRunner()


@dataclass
class FakeUseCase:
    status: GateStatus = GateStatus.PASSED
    error: Exception | None = None

    def execute(self, pipeline, layer=None, variables=None, **_):
        if self.error:
            raise self.error
        self.variables = variables
        report = make_report(self.status)
        return report if layer is not None else [report]


@dataclass
class FakeContainer:
    verify_layer: FakeUseCase
    run_medallion: FakeUseCase


@pytest.fixture
def use_case(monkeypatch):
    uc = FakeUseCase()
    monkeypatch.setattr(cli, "build_container", lambda settings: FakeContainer(uc, uc))
    return uc


def test_verify_pass_exits_zero_and_passes_vars(use_case):
    result = runner.invoke(cli.app, ["verify", "silver", "-e", "duckdb-local", "--set", "A=1"])
    assert result.exit_code == 0, result.output
    assert "passed" in result.output
    assert use_case.variables == {"A": 1}


def test_verify_blocked_exits_one(use_case):
    use_case.status = GateStatus.BLOCKED
    assert runner.invoke(cli.app, ["verify", "gold"]).exit_code == 1


def test_verify_config_error_exits_two(use_case):
    use_case.error = ContractNotFoundError("none")
    assert runner.invoke(cli.app, ["verify", "gold"]).exit_code == 2


def test_bad_variable_is_usage_error(use_case):
    assert runner.invoke(cli.app, ["verify", "gold", "--set", "novalue"]).exit_code == 2


def test_run_blocked(use_case):
    use_case.status = GateStatus.BLOCKED
    assert runner.invoke(cli.app, ["run"]).exit_code == 1


def test_run_error(use_case):
    use_case.error = ContractNotFoundError("none")
    assert runner.invoke(cli.app, ["run"]).exit_code == 2


def test_unknown_layer_rejected(use_case):
    assert runner.invoke(cli.app, ["verify", "platinum"]).exit_code == 2
