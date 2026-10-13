"""T3, full: Trino, the ldl-soda runner container and Airflow, against the running full profile.

Each test skips with the reason when its part of the stack is not running. Read-only: no tables are changed.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from pathlib import Path

import pytest

from tests.live.conftest import container_running, port_open, soda_dq

pytestmark = pytest.mark.full


def test_trino_gates_pass_on_good_data(trino: None) -> None:
    result = soda_dq("run", "-e", "trino", engine="trino")
    assert result.returncode == 0, result.stdout + result.stderr


def test_runner_container_runs_the_gates(lakehouse: Path) -> None:
    if not container_running("ldl-soda"):
        pytest.skip("ldl-soda is not running: make dq-up")
    proc = subprocess.run(
        ["docker", "exec", "ldl-soda", "soda-dq", "run"],
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_runner_container_is_hardened(lakehouse: Path) -> None:
    if not container_running("ldl-soda"):
        pytest.skip("ldl-soda is not running: make dq-up")
    proc = subprocess.run(
        ["docker", "inspect", "ldl-soda"], capture_output=True, text=True, check=True, timeout=60
    )
    info = json.loads(proc.stdout)[0]
    assert info["Config"]["User"] == "dq"
    assert info["HostConfig"]["ReadonlyRootfs"] is True
    assert info["HostConfig"]["CapDrop"] == ["ALL"]


def test_airflow_serves_the_gated_dag(lakehouse: Path) -> None:
    port = int(os.environ.get("AIRFLOW_API_PORT", "8080"))
    if not (container_running("ldl-airflow-apiserver") and port_open("localhost", port)):
        pytest.skip(f"Airflow is not running on :{port}: make airflow-dq-up AIRFLOW_API_PORT={port}")
    with urllib.request.urlopen(f"http://localhost:{port}/api/v2/monitor/health", timeout=10) as response:
        health = json.load(response)
    assert health["metadatabase"]["status"] == "healthy"
    proc = subprocess.run(
        ["docker", "exec", "ldl-airflow-scheduler", "airflow", "dags", "list", "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )
    assert "lakehouse_retail_medallion_dq" in proc.stdout
