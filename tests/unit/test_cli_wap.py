from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest
from typer.testing import CliRunner

from soda_lakehouse_dq import cli
from soda_lakehouse_dq.adapters.wap.audit_json import load_audit, write_audit
from soda_lakehouse_dq.domain.errors import DataSourceError
from soda_lakehouse_dq.domain.models import GateStatus, Layer
from soda_lakehouse_dq.domain.wap import BranchAudit, BranchState, TableState, WapLayerResult, WapOutcome
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

    def audit(self, pipeline, layer, branch, run_id):
        report = replace(make_report(self.status), pipeline=pipeline, layer=layer, run_id=run_id)
        table = f"{layer.value}.t"
        return BranchAudit(branch, report, {table: 3}, {table: 2})


@dataclass
class FakeSpark:
    error: Exception | None = None
    publications: list[tuple] = field(default_factory=list)

    def status(self, layer, branch=None):
        if self.error:
            raise self.error
        return _state("status", layer)

    def drop(self, layer, branch):
        return _state("drop", layer)

    def publish(self, layer, branch, pins, *, main_before=None):
        if self.error:
            raise self.error
        self.publications.append((layer, branch, dict(pins), dict(main_before or {})))
        return _state("publish", layer)


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


@pytest.mark.parametrize("source", ["", "  "])
def test_wap_run_rejects_empty_source_before_writing(wap, source):
    result = runner.invoke(cli.app, ["wap", "run", "--all", "--source", source, "--batch", "unknown"])
    assert result.exit_code == 2
    assert "--source must be a nonempty" in result.output
    assert not wap.run_wap.calls


@pytest.mark.parametrize("destination_is_directory", [False, True])
def test_wap_run_rejects_unusable_evidence_path_before_writing(wap, tmp_path, destination_is_directory):
    if destination_is_directory:
        path = tmp_path / "directory"
        path.mkdir()
    else:
        parent = tmp_path / "file"
        parent.write_text("existing file\n")
        path = parent / "evidence.json"
    result = runner.invoke(cli.app, ["wap", "run", "--all", "--evidence", str(path)])
    assert result.exit_code == 2, result.output
    assert "cannot write WAP evidence" in result.output
    assert "Traceback" not in result.output
    assert not wap.run_wap.calls


def test_wap_evidence_replacement_failure_preserves_previous_attachment(wap, tmp_path, monkeypatch):
    path = tmp_path / "evidence.json"
    path.write_text("previous attachment\n")
    original_replace = Path.replace

    def fail_replace(source, target):
        if target == path:
            raise PermissionError("destination became unavailable")
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_replace)
    result = runner.invoke(cli.app, ["wap", "run", "--all", "--evidence", str(path)])
    assert result.exit_code == 2, result.output
    assert "published bronze.t" in result.output
    assert "cannot persist WAP evidence" in result.output
    assert "Traceback" not in result.output
    assert path.read_text() == "previous attachment\n"
    assert list(tmp_path.iterdir()) == [path]


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


def test_verify_writes_manifest_and_publish_uses_persisted_pins(wap, tmp_path):
    path = tmp_path / "audit.json"
    result = runner.invoke(
        cli.app,
        [
            "wap",
            "verify",
            "bronze",
            "--branch",
            "audit_x",
            "--evidence",
            str(path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert load_audit(path).pins == {"bronze.t": 3}
    result = runner.invoke(cli.app, ["wap", "publish", "bronze", "--audit", str(path)])
    assert result.exit_code == 0, result.output
    assert wap.spark.publications == [(Layer.BRONZE, "audit_x", {"bronze.t": 3}, {"bronze.t": 2})]


@pytest.mark.parametrize(
    ("arguments", "status"),
    [
        (["bronze"], GateStatus.BLOCKED),
        (["silver"], GateStatus.PASSED),
        (["bronze", "--branch", "audit_other"], GateStatus.PASSED),
        (["bronze", "--pipeline", "another"], GateStatus.PASSED),
    ],
)
def test_publish_rejects_blocked_or_mismatched_audit(wap, tmp_path, arguments, status):
    path = tmp_path / "audit.json"
    report = replace(make_report(status), layer=Layer.BRONZE)
    write_audit(path, BranchAudit("audit_x", report, {"bronze.t": 3}))
    result = runner.invoke(cli.app, ["wap", "publish", *arguments, "--audit", str(path)])
    assert result.exit_code == 2
    assert not wap.spark.publications


def test_publish_rejects_missing_or_malformed_audit(wap, tmp_path):
    path = tmp_path / "missing.json"
    arguments = ["wap", "publish", "bronze", "--audit", str(path)]
    assert runner.invoke(cli.app, arguments).exit_code == 2
    path.write_text('{"version": 1}')
    assert runner.invoke(cli.app, arguments).exit_code == 2
    assert not wap.spark.publications


@pytest.mark.parametrize("drop_blocked", [False, True])
def test_blocked_branch_message_matches_configured_cleanup(monkeypatch, drop_blocked):
    from types import SimpleNamespace

    from tests.conftest import FakeClock
    from tests.unit.test_wap import FakeGate, _wap
    from tests.unit.test_wap import FakeSpark as MemorySpark

    spark = MemorySpark()
    monkeypatch.setenv("DQ_WAP_DROP_BLOCKED", str(drop_blocked).lower())

    def build(settings):
        use_case = _wap(
            FakeClock(),
            spark,
            FakeGate({Layer.BRONZE}),
            drop_blocked=settings.wap_drop_blocked,
        )
        return SimpleNamespace(run_wap=use_case)

    monkeypatch.setattr(cli, "build_wap", build)
    result = runner.invoke(cli.app, ["wap", "run", "--layer", "bronze", "--branch", "audit_x"])
    assert result.exit_code == 1, result.output
    assert "main stays 1 (5 rows)" in result.output
    if drop_blocked:
        assert "branch audit_x dropped after the blocked audit" in result.output
        assert "branch audit_x kept" not in result.output
        assert not spark.branches
        assert spark.calls[-1] == ("drop", "bronze", None)
    else:
        assert "branch audit_x kept at 101 (5 rows)" in result.output
        assert "branch audit_x dropped" not in result.output
        assert spark.branches == {("bronze.orders", "audit_x"): 101}
