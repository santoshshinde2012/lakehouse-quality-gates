"""Record the machine and software versions a benchmark ran on (docs/results/benchmarks/specs.json)."""

from __future__ import annotations

import json
import os
import platform
import subprocess
import urllib.request
from importlib.metadata import version
from typing import Any

from benchmarks.common import RESULTS, TRINO_URL, now_utc


def _cmd(*args: str) -> str:
    """Stdout of a command, or "" when it is missing or fails (specs are best effort)."""
    try:
        proc = subprocess.run(list(args), capture_output=True, text=True, check=False)
    except OSError:
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def _linux_cpu(cpuinfo: str, lscpu: str) -> str:
    for text, key in ((lscpu, "Model name"), (cpuinfo, "model name")):
        for line in text.splitlines():
            name, _, value = line.partition(":")
            if name.strip() == key and value.strip():
                return value.strip()
    return ""


def _linux_memory_bytes(meminfo: str) -> str:
    for line in meminfo.splitlines():
        if line.startswith("MemTotal:"):
            return str(int(line.split()[1]) * 1024)
    return ""


def machine() -> dict[str, str]:
    """CPU model, logical cores and RAM: sysctl on macOS, lscpu or /proc on Linux, stdlib otherwise."""
    system = platform.system()
    if system == "Darwin":
        cpu = _cmd("sysctl", "-n", "machdep.cpu.brand_string")
        cores = _cmd("sysctl", "-n", "hw.ncpu")
        memory = _cmd("sysctl", "-n", "hw.memsize")
    elif system == "Linux":
        cpu = _linux_cpu(_read("/proc/cpuinfo"), _cmd("lscpu"))
        cores = ""
        memory = _linux_memory_bytes(_read("/proc/meminfo"))
    else:
        cpu = cores = memory = ""
    return {
        "cpu": cpu or platform.processor() or platform.machine(),
        "cpu_cores": cores or str(os.cpu_count() or ""),
        "memory_bytes": memory,
    }


def collect() -> dict[str, Any]:
    specs: dict[str, Any] = {
        "collected_at": now_utc().isoformat(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "arch": platform.machine(),
        **machine(),
        "docker": _cmd("docker", "info", "--format", "{{.ServerVersion}} cpus={{.NCPU}} mem={{.MemTotal}}"),
        "packages": {
            p: version(p)
            for p in ("soda-core", "soda-duckdb", "soda-trino", "duckdb", "lakehouse-quality-gates")
        },
        "commit": _cmd("git", "rev-parse", "--short", "HEAD"),
        "lakehouse_commit": _cmd(
            "git",
            "-C",
            os.environ.get("DQ_LDL_REPO_PATH", "../local-data-lakehouse"),
            "rev-parse",
            "--short",
            "HEAD",
        ),
        "trino_heap": _cmd("docker", "exec", "ldl-trino", "grep", "-m1", "Xmx", "/etc/trino/jvm.config"),
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
