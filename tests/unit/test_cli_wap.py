from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest
from typer.testing import CliRunner

from soda_lakehouse_dq import cli
from soda_lakehouse_dq.domain.errors import DataSourceError
from soda_lakehouse_dq.domain.models import GateStatus, Layer
from soda_lakehouse_dq.domain.wap import BranchState, TableState, WapLayerResult, WapOutcome
from tests.conftest import make_report

runner = CliRunner()


def _state(action, layer, before=None):
    return BranchState(
        action, layer, "audit_x", 1.5, {"bronze.t": TableState("bronze.t", 2, 10, 3, 12, before)}
    )


@dataclass
class FakeRunWap:
    block: bool = False
    calls: list[tuple] = field(default_factory=list)

    def execute(self, pipeline, layers, *, source=None, branch=None):
        self.calls.append((pipeline, tuple(layers), source, branch))
        if self.block:
            return [
                WapLayerResult(
                    Layer.BRONZE,
                    "audit_x",
                    WapOutcome.BLOCKED,
                    make_report(GateStatus.BLOCKED),
                    _state("write", Layer.BRONZE),
                )
            ]
        return [
            WapLayerResult(
                Layer.BRONZE,
                "audit_x",
                WapOutcome.PUBLISHED,
                make_report(),
                _state("write", Layer.BRONZE),
                _state("publish", Layer.BRONZE, 1),
            )
        ]


@dataclass
class FakeVerifyBranch:
    status: GateStatus = GateStatus.PASSED

    def execute(self, pipeline, layer, branch, run_id):
        return make_report(self.status)


@dataclass
class FakeSpark:
    error: Exception | None = None

    def status(self, layer, branch=None):
        if self.error:
            raise self.error
        return _state("status", layer)

    def drop(self, layer, branch):
        return _state("drop", layer)


@dataclass
class FakeWap:
    run_wap: FakeRunWap = field(default_factory=FakeRunWap)
    verify_branch: FakeVerifyBranch = field(default_factory=FakeVerifyBranch)
    spark: FakeSpark = field(default_factory=FakeSpark)


@pytest.fixture
def wap(monkeypatch):
    services = FakeWap()
    monkeypatch.setattr(cli, "build_wap", lambda settings: services)
    return services


def test_wap_run_publishes_and_writes_evidence(wap, tmp_path):
    out = tmp_path / "e" / "wap.json"
    result = runner.invoke(cli.app, ["wap", "run", "--all", "--evidence", str(out)])
    assert result.exit_code == 0, result.output
    assert "published bronze.t: main 1 -> 2 (10 rows)" in result.output
    assert wap.run_wap.calls == [("retail", Layer.ordered(), "/opt/data/sample", None)]
    evidence = json.loads(out.read_text())
    assert evidence[0]["outcome"] == "published"
    assert evidence[0]["published"]["tables"]["bronze.t"]["main_before"] == 1


def test_wap_run_bad_batch_blocks(wap):
    wap.run_wap.block = True
    result = runner.invoke(
        cli.app, ["wap", "run", "-l", "silver", "-l", "bronze", "--batch", "bad", "--branch", "audit_b"]
    )
    assert result.exit_code == 1
    assert "main stays 2 (10 rows); branch audit_x kept at 3 (12 rows)" in result.output
    assert wap.run_wap.calls[0][1:] == (
        (Layer.BRONZE, Layer.SILVER),
        "/opt/soda-wap-fixtures/bad_batch",
        "audit_b",
    )


def test_wap_run_usage_errors(wap):
    assert runner.invoke(cli.app, ["wap", "run"]).exit_code == 2
    assert runner.invoke(cli.app, ["wap", "run", "--all", "--batch", "ugly"]).exit_code == 2


def test_wap_verify_exit_codes(wap):
    assert runner.invoke(cli.app, ["wap", "verify", "bronze", "--branch", "audit_x"]).exit_code == 0
    wap.verify_branch.status = GateStatus.BLOCKED
    assert runner.invoke(cli.app, ["wap", "verify", "bronze", "--branch", "audit_x"]).exit_code == 1


def test_wap_status_and_drop(wap):
    result = runner.invoke(cli.app, ["wap", "status", "bronze"])
    assert result.exit_code == 0
    assert json.loads(result.output)["tables"]["bronze.t"]["main_rows"] == 10
    assert "dropped audit_x" in runner.invoke(cli.app, ["wap", "drop", "gold", "--branch", "audit_x"]).output
    wap.spark.error = DataSourceError("spark down")
    assert runner.invoke(cli.app, ["wap", "status", "gold"]).exit_code == 2
