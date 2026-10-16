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


def test_parse_cpu_model_prefers_lscpu_then_proc() -> None:
    assert specs.parse_cpu_model(LSCPU, CPUINFO) == "AMD EPYC"
    assert specs.parse_cpu_model("", CPUINFO) == "Intel(R) Xeon(R) CPU"
    assert specs.parse_cpu_model("", "") == ""


def test_parse_meminfo_reads_kib() -> None:
    assert specs.parse_meminfo_bytes(MEMINFO) == str(8155564 * 1024)
    assert specs.parse_meminfo_bytes("") == ""


def test_linux_collector_needs_no_sysctl() -> None:
    calls: list[tuple[str, ...]] = []

    def run(*args: str) -> str:
        calls.append(args)
        return LSCPU if args == ("lscpu",) else ""

    files = {"/proc/cpuinfo": CPUINFO, "/proc/meminfo": MEMINFO}
    found = specs.LinuxCollector(run=run, read=lambda path: files.get(path, "")).collect()
    assert found == {"cpu": "AMD EPYC", "cpu_cores": "", "memory_bytes": str(8155564 * 1024)}
    assert all(c[0] != "sysctl" for c in calls)


def test_darwin_collector_uses_sysctl() -> None:
    values = {"machdep.cpu.brand_string": "Apple M1 Pro", "hw.ncpu": "10", "hw.memsize": "17179869184"}
    found = specs.DarwinCollector(run=lambda *args: values.get(args[-1], "")).collect()
    assert found == {"cpu": "Apple M1 Pro", "cpu_cores": "10", "memory_bytes": "17179869184"}


@pytest.mark.parametrize(
    ("system", "kind"),
    [("Darwin", specs.DarwinCollector), ("Linux", specs.LinuxCollector), ("Windows", specs.GenericCollector)],
)
def test_collector_for_each_platform(system: str, kind: type) -> None:
    assert isinstance(specs.collector_for(system), kind)


def test_fallbacks_fill_only_empty_fields() -> None:
    empty = {"cpu": "", "cpu_cores": "", "memory_bytes": ""}
    assert specs.with_fallbacks(empty, "", "x86_64", 4) == {
        "cpu": "x86_64",
        "cpu_cores": "4",
        "memory_bytes": "",
    }
    full = {"cpu": "M1", "cpu_cores": "10", "memory_bytes": "1"}
    assert specs.with_fallbacks(full, "arm", "arm64", 8) == full


def test_machine_uses_the_collector_given() -> None:
    class Fixed:
        def collect(self) -> dict[str, str]:
            return {"cpu": "Fake CPU", "cpu_cores": "2", "memory_bytes": "1024"}

    assert specs.machine(Fixed()) == {"cpu": "Fake CPU", "cpu_cores": "2", "memory_bytes": "1024"}
