"""Run one quality gate in-process and print one ``BENCH_JSON {...}`` line.

Stdlib + soda_lakehouse_dq only, so it also runs inside the ldl-soda container:

    docker exec -i ldl-soda python - silver < benchmarks/probe.py

Measures the gate (``VerifyLayerUseCase.execute``) wall time, process CPU and peak RSS, and counts the SQL
queries Soda issues (its ``SQL query ...`` debug log records). Sinks are disabled (``DQ_SINKS=[]``).
"""

from __future__ import annotations

import json
import logging
import os
import resource
import sys
import time

_T0 = time.perf_counter()
os.environ.setdefault("DQ_SINKS", "[]")
os.environ.setdefault("SODA_CORE_TELEMETRY_ENABLED", "false")


class QueryCounter(logging.Handler):
    """Counts Soda's per-query debug records; swallows everything else."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message.startswith("SQL query ") and not message.startswith("SQL query result"):
            self.count += 1


def main(argv: list[str]) -> int:
    from soda_lakehouse_dq.container import build_container
    from soda_lakehouse_dq.domain.models import Layer

    layer = Layer(argv[0] if argv else "bronze")
    counter = QueryCounter()
    root = logging.getLogger()
    root.handlers = [counter]
    root.setLevel(logging.DEBUG)
    import_s = time.perf_counter() - _T0
    container = build_container()
    start = time.perf_counter()
    report = container.verify_layer.execute("retail", layer, {})
    gate_s = time.perf_counter() - start
    usage = resource.getrusage(resource.RUSAGE_SELF)
    rss = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
    checks = [
        {
            "contract": result.contract.name,
            "name": check.name,
            "outcome": check.outcome.value,
            "metrics": check.metrics,
        }
        for result in report.results
        for check in result.checks
    ]
    payload = {
        "layer": layer.value,
        "engine": report.engine,
        "status": report.status.value,
        "gate_s": round(gate_s, 4),
        "import_s": round(import_s, 4),
        "cpu_user_s": round(usage.ru_utime, 3),
        "cpu_sys_s": round(usage.ru_stime, 3),
        "max_rss_bytes": rss,
        "queries": counter.count,
        "checks": checks,
    }
    sys.stdout.write("BENCH_JSON " + json.dumps(payload, default=str) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
