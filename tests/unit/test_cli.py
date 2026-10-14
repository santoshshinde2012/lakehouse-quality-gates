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


@pytest.mark.parametrize("args", [["verify", "gold"], ["run"], ["list"], ["validate"]])
def test_invalid_settings_are_usage_errors(monkeypatch, args):
    monkeypatch.setenv("DQ_LOG_LEVEL", "not-a-log-level")
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 2
    assert "invalid settings" in result.output
    assert "Traceback" not in result.output


@pytest.mark.parametrize("command", ["list", "validate"])
def test_missing_contracts_are_configuration_errors(monkeypatch, tmp_path, command):
    monkeypatch.setenv("DQ_CONTRACTS_DIR", str(tmp_path / "missing"))
    result = runner.invoke(cli.app, [command, "-e", "duckdb-local"])
    assert result.exit_code == 2
    assert "no contracts under" in result.output


@pytest.mark.parametrize(
    ("invalid_body", "diagnostic"),
    [
        ("columns: []\nchecks: []\n", "must not be an empty list"),
        ("columns: []\ncolumns: []\nchecks:\n  - row_count:\n", "YAML syntax error"),
    ],
)
def test_validate_reports_parser_failure_and_continues_all_contracts(
    monkeypatch, tmp_path, invalid_body, diagnostic
):
    contracts = tmp_path / "contracts"
    for layer in ("bronze", "silver", "gold"):
        folder = contracts / "retail" / layer
        folder.mkdir(parents=True)
        (folder / "z_valid.yml").write_text(
            f'dataset: "{{{{ dataset_root }}}}/{layer}/orders"\ncolumns: []\nchecks:\n  - row_count:\n'
        )
    (contracts / "retail" / "bronze" / "a_malformed.yml").write_text(
        'dataset: "{{ dataset_root }}/bronze/orders"\n' + invalid_body
    )
    monkeypatch.setenv("DQ_CONTRACTS_DIR", str(contracts))
    monkeypatch.setenv("DQ_REPORTS_DIR", str(tmp_path / "reports"))
    monkeypatch.setenv("DQ_SLACK_WEBHOOK_URL", "")
    monkeypatch.setenv("DQ_ICEBERG_SINK", "false")
    result = runner.invoke(cli.app, ["validate", "-e", "duckdb-local"])
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "FAIL retail.bronze.a_malformed" in result.output
    assert diagnostic in result.output
    for layer in ("bronze", "silver", "gold"):
        assert f"ok   retail.{layer}.z_valid" in result.output
