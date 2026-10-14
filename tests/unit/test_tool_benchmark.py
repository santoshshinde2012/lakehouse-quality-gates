"""Tool benchmarks preserve check identity and fail closed while retaining raw evidence."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest
from benchmarks.tools import run_tools


def measurement():
    return {
        "tool": "gx",
        "run_s": 0.25,
        "cpu_s": 0.3,
        "max_rss_bytes": 1024,
        "checks": {
            name: {"failed": failed, "count": i}
            for i, (name, failed) in enumerate(run_tools.EXPECTED_FAIL.items())
        },
    }


def process(result=None, returncode=0):
    return subprocess.CompletedProcess(
        [], returncode, "BENCH_JSON " + json.dumps(measurement() if result is None else result) + "\n", ""
    )


def test_gx_reordered_results_keep_their_own_check_identity():
    results = [
        SimpleNamespace(
            expectation_config=SimpleNamespace(meta={"benchmark_check": name}),
            success=not failed,
            result={"unexpected_count": i},
        )
        for i, (name, failed) in enumerate(run_tools.EXPECTED_FAIL.items())
    ]
    checks = run_tools.gx_checks(list(reversed(results)))
    assert checks == measurement()["checks"]


def test_gx_duplicate_identity_cannot_hide_missing_check():
    result = SimpleNamespace(
        expectation_config=SimpleNamespace(meta={"benchmark_check": "order_id_missing"}),
        success=True,
        result={"unexpected_count": 0},
    )
    with pytest.raises(ValueError, match="duplicate GX"):
        run_tools.gx_checks([result, result])
    with pytest.raises(ValueError, match="missing GX"):
        run_tools.gx_checks([result])


def test_scoring_rejects_printed_measurement_from_failed_child():
    with pytest.raises(ValueError, match="status 1"):
        run_tools.score_result(process(returncode=1), "gx")


@pytest.mark.parametrize("damage", ["missing", "identity", "decision", "nonfinite"])
def test_scoring_rejects_partial_or_malformed_measurement(damage):
    result = measurement()
    if damage == "missing":
        del result["checks"]["customer_id_missing"]
    elif damage == "identity":
        result["tool"] = "soda"
    elif damage == "decision":
        result["checks"]["customer_id_missing"]["failed"] = "False"
    else:
        result["run_s"] = float("nan")
    with pytest.raises(ValueError, match=r"six scored|identity differs|invalid"):
        run_tools.score_result(process(result), "gx")


def test_wrong_verdict_is_scored_by_check_identity():
    result = measurement()
    result["checks"]["customer_id_missing"]["failed"] = False
    _, wrong = run_tools.score_result(process(result), "gx")
    assert wrong == ["customer_id_missing"]


def test_generate_rejects_fixture_with_wrong_actual_count(tmp_path, monkeypatch):
    class Connection:
        def execute(self, sql):
            return self

        def fetchone(self):
            return (999, 0, 1, 1, 1, 1, 1)

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=Connection))
    with pytest.raises(RuntimeError, match="does not match requested rows"):
        run_tools.generate(1000, tmp_path)


@pytest.mark.parametrize("failed_child", [True, False])
def test_bench_retains_evidence_and_fails_on_errors_or_wrong_verdicts(tmp_path, monkeypatch, failed_child):
    result = measurement()
    result["checks"]["customer_id_missing"]["failed"] = False

    def run(args, **kwargs):
        if args[0] in ("git", "sysctl"):
            return subprocess.CompletedProcess(args, 0, "", "")
        child = process(result, returncode=1 if failed_child else 0)
        child.stderr = "child diagnostic\n"
        return child

    monkeypatch.setattr(run_tools.subprocess, "run", run)
    monkeypatch.setattr(run_tools.platform, "platform", lambda: "test-platform")
    monkeypatch.setattr(run_tools, "generate", lambda rows, path: {"rows": rows, "k": 1})
    out = tmp_path / "tools.csv"
    assert run_tools.bench([1000], 1, out, ["gx"]) is False
    with out.open() as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["correct"] == ("error" if failed_child else "False")
    metadata = json.loads((tmp_path / "tools_metadata.json").read_text())
    assert metadata["all_verdicts_match"] is False
    assert metadata["fixtures"] == [{"rows": 1000, "k": 1}]
    assert metadata["harness_sha256"]
    raw = json.loads((tmp_path / "tools_raw.jsonl").read_text())
    assert raw["returncode"] == (1 if failed_child else 0)
    assert (tmp_path / "tools_logs/1000-gx-0.stderr.log").read_text() == "child diagnostic\n"


def test_bench_rejects_mismatched_generated_size(tmp_path, monkeypatch):
    monkeypatch.setattr(run_tools.platform, "platform", lambda: "test-platform")
    monkeypatch.setattr(
        run_tools.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "", "")
    )
    monkeypatch.setattr(run_tools, "generate", lambda rows, path: {"rows": rows - 1, "k": 1})
    with pytest.raises(RuntimeError, match="requested 1000 orders, generated 999"):
        run_tools.bench([1000], 1, tmp_path / "tools.csv", ["gx"])


def test_cli_returns_failure_when_bench_does_not_match(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_tools.py", "bench", "--out", "unused.csv"])
    monkeypatch.setattr(run_tools, "bench", lambda *args: False)
    assert run_tools.main() == 1


def test_tools_are_interleaved_with_rotating_starts(tmp_path, monkeypatch):
    def run(args, **kwargs):
        if args[0] in ("git", "sysctl"):
            return subprocess.CompletedProcess(args, 0, "", "")
        result = measurement()
        result["tool"] = args[-2]
        return process(result)

    monkeypatch.setattr(run_tools.subprocess, "run", run)
    monkeypatch.setattr(run_tools.platform, "platform", lambda: "test-platform")
    monkeypatch.setattr(run_tools, "generate", lambda rows, path: {"rows": rows, "k": 1})
    assert run_tools.bench([1000], 2, tmp_path / "tools.csv", ["soda", "gx"]) is True
    metadata = json.loads((tmp_path / "tools_metadata.json").read_text())
    assert [(entry["repeat"], entry["tool"]) for entry in metadata["execution_order"]] == [
        (0, "soda"),
        (0, "gx"),
        (1, "gx"),
        (1, "soda"),
    ]
