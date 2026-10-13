"""Record the machine and software versions a benchmark ran on (docs/results/benchmarks/specs.json)."""

from __future__ import annotations

import json
import platform
import subprocess
import urllib.request
from importlib.metadata import version
from typing import Any

from benchmarks.common import RESULTS, TRINO_URL, now_utc


def _cmd(*args: str) -> str:
    proc = subprocess.run(list(args), capture_output=True, text=True, check=False)
    return proc.stdout.strip()


def collect() -> dict[str, Any]:
    specs: dict[str, Any] = {
        "collected_at": now_utc().isoformat(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "cpu": _cmd("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
        "cpu_cores": _cmd("sysctl", "-n", "hw.ncpu"),
        "memory_bytes": _cmd("sysctl", "-n", "hw.memsize"),
        "docker": _cmd("docker", "info", "--format", "{{.ServerVersion}} cpus={{.NCPU}} mem={{.MemTotal}}"),
        "packages": {p: version(p) for p in ("soda-core", "soda-duckdb", "soda-trino", "duckdb")},
    }
    try:
        request = urllib.request.Request(f"{TRINO_URL}/v1/info")  # noqa: S310 - fixed local URL
        with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310
            specs["trino"] = json.load(response).get("nodeVersion", {}).get("version")
    except OSError:
        specs["trino"] = None
    return specs


if __name__ == "__main__":
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "specs.json").write_text(json.dumps(collect(), indent=2) + "\n")
    print((RESULTS / "specs.json").read_text())
