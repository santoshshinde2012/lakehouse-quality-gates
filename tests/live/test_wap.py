"""T3, full: write-audit-publish against the running lakehouse (ldl-spark with docker/docker-compose.wap.yml).

A good batch must move main to the audited snapshot with matching row counts; a bad batch must leave main's
snapshot ids and data exactly as they were and keep the audit branch with the bad rows. Leaves the lakehouse
sample published on main.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tests.live.conftest import container_running, soda_dq

pytestmark = pytest.mark.full


@pytest.fixture(scope="module")
def wap_stack(lakehouse: Path) -> None:
    if not container_running("ldl-spark"):
        pytest.skip("ldl-spark is not running: make wap-up")
    probe = subprocess.run(
        ["docker", "exec", "ldl-spark", "test", "-f", "/opt/soda-wap/wap_job.py"],
        check=False,
        capture_output=True,
    )
    if probe.returncode != 0:
        pytest.skip("ldl-spark has no /opt/soda-wap mount: make wap-up (docker/docker-compose.wap.yml)")


def _run(tmp_path: Path, batch: str, branch: str) -> tuple[int, list[dict]]:
    evidence = tmp_path / f"{batch}.json"
    result = soda_dq("wap", "run", "--all", "--batch", batch, "--branch", branch, "--evidence", str(evidence))
    assert evidence.exists(), result.stdout + result.stderr
    return result.returncode, json.loads(evidence.read_text())


def test_good_batch_publishes_and_bad_batch_never_reaches_main(wap_stack: None, tmp_path: Path) -> None:
    code, good = _run(tmp_path, "good", "audit_livetest_good")
    assert code == 0
    assert [e["outcome"] for e in good] == ["published"] * 3
    main_after_good: dict[str, tuple[int, int]] = {}
    for layer in good:
        for table, pub in layer["published"]["tables"].items():
            written = layer["written"]["tables"][table]
            assert (
                pub["main_snapshot"] == written["branch_snapshot"]
            )  # main now points at the audited snapshot
            assert pub["main_snapshot"] != pub["main_before"]
            assert pub["main_rows"] == written["branch_rows"]
            main_after_good[table] = (pub["main_snapshot"], pub["main_rows"])

    code, bad = _run(tmp_path, "bad", "audit_livetest_bad")
    try:
        assert code == 1
        assert [(e["layer"], e["outcome"]) for e in bad] == [("bronze", "blocked")]
        for table, state in bad[0]["written"]["tables"].items():
            assert (state["main_snapshot"], state["main_rows"]) == main_after_good[table]  # main untouched
            assert state["branch_snapshot"] not in (None, state["main_snapshot"])
        assert (
            bad[0]["written"]["tables"]["bronze.orders_raw"]["branch_rows"]
            > main_after_good["bronze.orders_raw"][1]
        )

        status = soda_dq("wap", "status", "bronze", "--branch", "audit_livetest_bad")
        assert status.returncode == 0, status.stderr
        tables = json.loads(status.stdout)["tables"]
        assert tables["bronze.orders_raw"]["branch_snapshot"] is not None  # kept for forensics
        assert tables["bronze.orders_raw"]["main_snapshot"] == main_after_good["bronze.orders_raw"][0]

        gates_on_main = soda_dq("run", "-e", "duckdb", engine="duckdb")
        assert gates_on_main.returncode == 0, gates_on_main.stdout  # main still holds the good data
    finally:
        soda_dq("wap", "drop", "bronze", "--branch", "audit_livetest_bad")
