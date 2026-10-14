"""A printed measurement cannot hide a failed subprocess or an accuracy mismatch."""

from __future__ import annotations

import subprocess

import pytest
from benchmarks import common, run_accuracy, run_perf


def test_probe_rejects_measurement_from_a_failed_process(monkeypatch: pytest.MonkeyPatch) -> None:
    proc = subprocess.CompletedProcess([], 1, 'BENCH_JSON {"status": "passed"}\n', "shutdown failed")
    monkeypatch.setattr(common.subprocess, "run", lambda *args, **kwargs: proc)
    with pytest.raises(RuntimeError, match=r"rc=1.*shutdown failed"):
        common.run_probe("duckdb", "bronze")


def test_failed_seed_cannot_supply_a_successful_benchmark_measurement() -> None:
    proc = subprocess.CompletedProcess([], 1, 'BENCH_JSON {"counts": {}}\n', "write failed")
    with pytest.raises(RuntimeError, match=r"seed failed.*rc=1"):
        run_perf._bench_json(proc)


@pytest.mark.parametrize("engine", ["duckdb", "container"])
def test_probe_disables_optional_side_effects(monkeypatch: pytest.MonkeyPatch, engine: str) -> None:
    captured = {}

    def run(args, **kwargs):
        captured.update(args=args, **kwargs)
        return subprocess.CompletedProcess(args, 0, 'BENCH_JSON {"status": "passed"}\n', "")

    monkeypatch.setattr(common.subprocess, "run", run)
    common.run_probe(engine, "bronze", {"DQ_ICEBERG_SINK": "true", "DQ_SLACK_WEBHOOK_URL": "configured"})
    assert captured["env"]["DQ_SINKS"] == "[]"
    assert captured["env"]["DQ_ICEBERG_SINK"] == "false"
    assert captured["env"]["DQ_SLACK_WEBHOOK_URL"] == ""
    if engine == "container":
        assert "DQ_ICEBERG_SINK=false" in captured["args"]
        assert "DQ_SLACK_WEBHOOK_URL=" in captured["args"]


def test_accuracy_mismatch_fails_after_recording_every_scenario(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def evaluate(rows: int, defect: str | None, rate: float) -> bool:
        calls.append((rows, defect, rate))
        return defect is None

    monkeypatch.setattr(run_accuracy, "evaluate", evaluate)
    assert run_accuracy.main(["--defects", "duplicate_in_file", "--rates", "0.01", "0.1"]) == 1
    assert len(calls) == 3
