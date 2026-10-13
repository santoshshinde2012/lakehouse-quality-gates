"""Helpers for the live (T2) and full (T3) tiers. They need a running local-data-lakehouse and skip, with the
reason, when it is not there. Run them with `make test-live` / `make test-full` or `scripts/e2e.sh`."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SODA_DQ = str(Path(sys.executable).with_name("soda-dq"))


def port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def lakehouse_path() -> Path | None:
    value = os.environ.get("DQ_LDL_REPO_PATH")
    if not value:
        env_file = PROJECT_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("DQ_LDL_REPO_PATH="):
                    value = line.split("=", 1)[1].strip()
    if not value:
        return None
    path = (PROJECT_ROOT / value).resolve()
    return path if (path / "docker-compose.yml").exists() else None


def container_running(name: str) -> bool:
    if shutil.which("docker") is None:
        return False
    proc = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"name=^{name}$", "--filter", "status=running"],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(proc.stdout.strip())


def soda_dq(*args: str, engine: str = "duckdb") -> subprocess.CompletedProcess[str]:
    """Run the CLI in a fresh process (as Airflow would), with the given engine."""
    env = {**os.environ, "DQ_ENGINE": engine, "SODA_CORE_TELEMETRY_ENABLED": "false"}
    ldl = lakehouse_path()
    if ldl is not None:
        env["DQ_LDL_REPO_PATH"] = str(ldl)
    return subprocess.run(
        [SODA_DQ, *args], cwd=PROJECT_ROOT, env=env, capture_output=True, text=True, check=False, timeout=600
    )


@pytest.fixture(scope="session")
def lakehouse() -> Path:
    """The lakehouse checkout, with Lakekeeper reachable; otherwise skip."""
    path = lakehouse_path()
    if path is None:
        pytest.skip("DQ_LDL_REPO_PATH is not set or is not a local-data-lakehouse checkout")
    url = urlparse(os.environ.get("DQ_LAKEKEEPER_URL", "http://localhost:8181"))
    if not port_open(url.hostname or "localhost", url.port or 8181):
        pytest.skip(
            f"Lakekeeper is not reachable at {url.geturl()}: start the lakehouse (make up-light there)"
        )
    return path


@pytest.fixture(scope="session")
def trino(lakehouse: Path) -> None:
    host = os.environ.get("DQ_TRINO_HOST", "localhost")
    port = int(os.environ.get("DQ_TRINO_PORT", "8088"))
    if not port_open(host, port):
        pytest.skip(f"Trino is not reachable at {host}:{port}: start the full profile (make up-full TRINO=1)")
