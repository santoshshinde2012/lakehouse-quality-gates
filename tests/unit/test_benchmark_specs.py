"""specs.json records the machine on Linux as well as macOS, and never fails on a missing tool."""

from __future__ import annotations

import pytest
from benchmarks import specs

pytestmark = pytest.mark.unit

LSCPU = "Architecture:  x86_64\nCPU(s):        4\nModel name:    AMD EPYC\n"
CPUINFO = "processor\t: 0\nmodel name\t: Intel(R) Xeon(R) CPU\n"
MEMINFO = "MemTotal:        8155564 kB\nMemFree:         100 kB\n"


def test_cmd_returns_empty_for_a_missing_command() -> None:
    assert specs._cmd("definitely-not-a-command-lqg") == ""


def test_linux_cpu_prefers_lscpu_then_proc() -> None:
    assert specs._linux_cpu(CPUINFO, LSCPU) == "AMD EPYC"
    assert specs._linux_cpu(CPUINFO, "") == "Intel(R) Xeon(R) CPU"
    assert specs._linux_cpu("", "") == ""


def test_linux_memory_reads_meminfo_kib() -> None:
    assert specs._linux_memory_bytes(MEMINFO) == str(8155564 * 1024)
    assert specs._linux_memory_bytes("") == ""


def test_machine_on_linux_needs_no_sysctl(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    def cmd(*args: str) -> str:
        calls.append(args)
        return LSCPU if args == ("lscpu",) else ""

    files = {"/proc/cpuinfo": CPUINFO, "/proc/meminfo": MEMINFO}
    monkeypatch.setattr(specs.platform, "system", lambda: "Linux")
    monkeypatch.setattr(specs, "_cmd", cmd)
    monkeypatch.setattr(specs, "_read", lambda path: files.get(path, ""))
    monkeypatch.setattr(specs.os, "cpu_count", lambda: 4)
    assert specs.machine() == {"cpu": "AMD EPYC", "cpu_cores": "4", "memory_bytes": str(8155564 * 1024)}
    assert all(c[0] != "sysctl" for c in calls)


def test_machine_on_macos_uses_sysctl(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "machdep.cpu.brand_string": "Apple M1 Pro",
        "hw.ncpu": "10",
        "hw.memsize": "17179869184",
    }
    monkeypatch.setattr(specs.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(specs, "_cmd", lambda *args: values.get(args[-1], ""))
    assert specs.machine() == {"cpu": "Apple M1 Pro", "cpu_cores": "10", "memory_bytes": "17179869184"}
