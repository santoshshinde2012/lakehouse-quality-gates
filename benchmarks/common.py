"""Shared helpers: subprocess measurement, Trino query statistics, result files."""

from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
# DQ_BENCH_RESULTS (make ... BENCH_OUT=dir) sends a run elsewhere, so a small run never mixes with the published set.
RESULTS = Path(os.environ.get("DQ_BENCH_RESULTS") or ROOT / "docs" / "results" / "benchmarks").resolve()
PROBE = ROOT / "benchmarks" / "probe.py"
CONTAINER = os.environ.get("LDL_SODA_CONTAINER", "ldl-soda")
TRINO_URL = os.environ.get("BENCH_TRINO_URL", "http://localhost:8088")
ENGINES = ("duckdb", "trino", "container")
LAYERS = ("bronze", "silver", "gold")

_UNITS = {"B": 1, "kB": 1e3, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}
_DUR = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


@dataclass(frozen=True)
class ProbeRun:
    process_s: float
    payload: dict[str, Any]


def run_probe(engine: str, layer: str, extra_env: Mapping[str, str] | None = None) -> ProbeRun:
    """Run benchmarks/probe.py for one layer on one engine as a fresh process."""
    env = {**os.environ, "DQ_SINKS": "[]", **(extra_env or {})}
    if engine == "container":
        cmd: list[str] = ["docker", "exec", "-i", "-e", "DQ_SINKS=[]", CONTAINER, "python", "-", layer]
        stdin = PROBE.read_text()
    else:
        env["DQ_ENGINE"] = engine
        cmd = [sys.executable, str(PROBE), layer]
        stdin = ""
    start = time.perf_counter()
    proc = subprocess.run(cmd, input=stdin, capture_output=True, text=True, env=env, cwd=ROOT, check=False)
    elapsed = time.perf_counter() - start
    for line in proc.stdout.splitlines():
        if line.startswith("BENCH_JSON "):
            return ProbeRun(elapsed, json.loads(line.removeprefix("BENCH_JSON ")))
    raise RuntimeError(f"probe failed ({engine}/{layer}, rc={proc.returncode}): {proc.stderr[-2000:]}")


def parse_size(value: str | float | None) -> float:
    if value is None:
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    match = re.fullmatch(r"([\d.]+)\s*([A-Za-z]+)", value.strip())
    return float(match[1]) * _UNITS.get(match[2], 1) if match else 0.0


def parse_duration(value: str | float | None) -> float:
    if value is None:
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    match = re.fullmatch(r"([\d.]+)\s*([a-z]+)", value.strip())
    return float(match[1]) * _DUR.get(match[2], 1) if match else 0.0


def trino_queries_since(since: datetime) -> list[dict[str, Any]]:
    """Queries created after ``since`` from Trino's REST API (needs exclusive use during a run)."""
    request = urllib.request.Request(  # noqa: S310 - fixed local http URL
        f"{TRINO_URL}/v1/query", headers={"X-Trino-User": "bench"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
        queries: list[dict[str, Any]] = json.load(response)
    out = []
    for query in queries:
        created = datetime.fromisoformat(str(query["queryStats"]["createTime"]).replace("Z", "+00:00"))
        if created >= since and "system.runtime" not in str(query.get("query", "")):
            out.append(query)
    return out


def trino_window_stats(since: datetime) -> dict[str, float]:
    queries = trino_queries_since(since)
    stats = [q["queryStats"] for q in queries]
    return {
        "server_queries": float(len(queries)),
        "server_cpu_s": sum(parse_duration(s.get("totalCpuTime")) for s in stats),
        "scan_bytes": sum(parse_size(s.get("physicalInputDataSize")) for s in stats),
        "peak_mem_bytes": max((parse_size(s.get("peakUserMemoryReservation")) for s in stats), default=0),
    }


def now_utc() -> datetime:
    return datetime.now(UTC)


def append_csv(path: Path, row: Mapping[str, Any], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fields), extrasaction="ignore")
        if new:
            writer.writeheader()
        writer.writerow(row)


def append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(record, default=str) + "\n")
