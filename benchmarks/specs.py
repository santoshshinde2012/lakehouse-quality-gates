"""Record the machine and software versions a benchmark ran on (docs/results/benchmarks/specs.json)."""

from __future__ import annotations

import json
import os
import platform
import subprocess
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any, Protocol

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


# ---- pure parsers (no I/O) ----


def parse_cpu_model(lscpu: str, cpuinfo: str) -> str:
    """CPU model from `lscpu` output, else from /proc/cpuinfo; "" when neither names it."""
    for text, key in ((lscpu, "Model name"), (cpuinfo, "model name")):
        for line in text.splitlines():
            name, _, value = line.partition(":")
            if name.strip() == key and value.strip():
                return value.strip()
    return ""


def parse_meminfo_bytes(meminfo: str) -> str:
    """Total RAM in bytes from /proc/meminfo (reported in KiB); "" when absent."""
    for line in meminfo.splitlines():
        if line.startswith("MemTotal:"):
            return str(int(line.split()[1]) * 1024)
    return ""


# ---- per-platform collectors ----

Run = Callable[..., str]
Read = Callable[[str], str]


class MachineCollector(Protocol):
    """Reads CPU model, logical cores and RAM in bytes; any field may be "" when unknown."""

    def collect(self) -> dict[str, str]: ...


@dataclass(frozen=True)
class DarwinCollector:
    run: Run = _cmd

    def collect(self) -> dict[str, str]:
        return {
            "cpu": self.run("sysctl", "-n", "machdep.cpu.brand_string"),
            "cpu_cores": self.run("sysctl", "-n", "hw.ncpu"),
            "memory_bytes": self.run("sysctl", "-n", "hw.memsize"),
        }


@dataclass(frozen=True)
class LinuxCollector:
    run: Run = _cmd
    read: Read = _read

    def collect(self) -> dict[str, str]:
        return {
            "cpu": parse_cpu_model(self.run("lscpu"), self.read("/proc/cpuinfo")),
            "cpu_cores": "",
            "memory_bytes": parse_meminfo_bytes(self.read("/proc/meminfo")),
        }


@dataclass(frozen=True)
class GenericCollector:
    def collect(self) -> dict[str, str]:
        return {"cpu": "", "cpu_cores": "", "memory_bytes": ""}


COLLECTORS: dict[str, Callable[[], MachineCollector]] = {"Darwin": DarwinCollector, "Linux": LinuxCollector}


def collector_for(system: str) -> MachineCollector:
    """The collector for a `platform.system()` name; a new platform is one more entry in COLLECTORS."""
    return COLLECTORS.get(system, GenericCollector)()


def with_fallbacks(found: dict[str, str], processor: str, arch: str, cpu_count: int | None) -> dict[str, str]:
    """Fill empty fields from the stdlib so every platform reports something."""
    return {
        "cpu": found["cpu"] or processor or arch,
        "cpu_cores": found["cpu_cores"] or str(cpu_count or ""),
        "memory_bytes": found["memory_bytes"],
    }


def machine(collector: MachineCollector | None = None) -> dict[str, str]:
    """CPU model, logical cores and RAM for this machine (or for the collector given)."""
    found = (collector or collector_for(platform.system())).collect()
    return with_fallbacks(found, platform.processor(), platform.machine(), os.cpu_count())


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
