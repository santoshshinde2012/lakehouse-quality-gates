"""Summarise raw benchmark results into docs/results/benchmarks/summary.md (Markdown tables).

Percentiles use the nearest-rank method over the warm runs (5 per cell by default), so p95 over five
samples is the slowest warm run. Cold is the first run after the seed.
"""

from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from benchmarks.common import ENGINES, LAYERS, RESULTS


def read(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open() as fh:
        return list(csv.DictReader(fh))


def pct(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def table(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def human(rows: int) -> str:
    return f"{rows // 1_000_000}M" if rows >= 1_000_000 else f"{rows // 1000}k"


def mib(value: float) -> str:
    return f"{value / 2**20:,.0f}"


def perf_sections(perf: list[dict[str, str]], pipe: list[dict[str, str]]) -> list[str]:
    cells: dict[tuple[int, str, str], dict[str, list[dict[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for r in perf:
        cells[(int(r["rows"]), r["engine"], r["layer"])][r["phase"]].append(r)
    sizes = sorted({k[0] for k in cells})
    out = ["## Gate latency (seconds, gate only, excludes process start-up)"]
    rows = []
    for size in sizes:
        for engine in ENGINES:
            for layer in LAYERS:
                c = cells.get((size, engine, layer))
                if not c:
                    continue
                warm = [float(r["gate_s"]) for r in c["warm"]]
                cold = [float(r["gate_s"]) for r in c["cold"]]
                proc = [float(r["process_s"]) for r in c["warm"]]
                rows.append([human(size), engine, layer, f"{pct(warm, 0.5):.2f}", f"{pct(warm, 0.95):.2f}",
                             f"{statistics.pstdev(warm):.2f}", f"{cold[0]:.2f}" if cold else "", f"{pct(proc, 0.5):.2f}",
                             len(warm), "/".join(sorted({r["status"] for r in c["warm"] + c["cold"]}))])  # fmt: skip
    out.append(table(["rows", "engine", "layer", "warm p50", "warm p95", "warm σ", "cold", "process p50",
                      "n warm", "verdict"], rows))  # fmt: skip
    out.append(
        "\nA verdict other than `passed` means the data was fine but the engine could not evaluate the checks "
        "(see the Trino memory note in the system design); those timings measure a failing gate."
    )
    out.append("\n## Resources per gate (median of warm runs)")
    rows = []
    for size in sizes:
        for engine in ENGINES:
            for layer in LAYERS:
                c = cells.get((size, engine, layer))
                if not c:
                    continue
                w = c["warm"]

                def med(k: str, w: list[dict[str, str]] = w) -> float:
                    return statistics.median(float(r[k] or 0) for r in w)

                extra = (f"{med('server_cpu_s'):.2f}", mib(med("scan_bytes")), mib(med("peak_mem_bytes"))) \
                    if engine == "trino" else ("", "", "")  # fmt: skip
                rows.append([human(size), engine, layer, mib(med("max_rss_bytes")),
                             f"{med('cpu_user_s') + med('cpu_sys_s'):.2f}", int(med("queries")), *extra])  # fmt: skip
    out.append(table(["rows", "engine", "layer", "client peak RSS MiB", "client CPU s", "SQL queries",
                      "Trino CPU s", "Trino scan MiB", "Trino peak mem MiB"], rows))  # fmt: skip
    if pipe:
        out.append("\n## Pipeline build vs gate (DuckDB writer, host DuckDB gate warm p50)")
        builds = {int(r["rows"]): r for r in pipe}
        rows = []
        for size in sizes:
            b = builds.get(size)
            if not b:
                continue
            for layer in LAYERS:
                c = cells.get((size, "duckdb", layer))
                if not c:
                    continue
                gate = pct([float(r["gate_s"]) for r in c["warm"]], 0.5)
                build = float(b[f"{layer}_s"])
                rows.append([human(size), layer, f"{build:.2f}", f"{gate:.2f}", f"{100 * gate / build:.0f}%",
                             f"{100 * gate / (build + gate):.0f}%"])  # fmt: skip
        out.append(table(["rows", "layer", "build s", "gate s", "gate / build", "gate share of layer"], rows))
        sizes_tbl: dict[int, dict[str, tuple[int, int]]] = defaultdict(dict)
        for r in pipe:
            sizes_tbl[int(r["rows"])][r["table"]] = (int(r["table_rows"]), int(r["table_bytes"]))
        out.append("\n## Iceberg table sizes (data files, from Trino $files)")
        out.append(table(["rows", "table", "rows in table", "MiB"],
                         [[human(s), t, f"{n:,}", f"{b / 2**20:,.1f}"] for s in sorted(sizes_tbl)
                          for t, (n, b) in sizes_tbl[s].items()]))  # fmt: skip
    out.append("\n## Scaling: all three gates, warm p50 sum (seconds)")
    rows = []
    for size in sizes:
        row: list[object] = [human(size)]
        for engine in ENGINES:
            parts = [cells.get((size, engine, layer)) for layer in LAYERS]
            row.append(f"{sum(pct([float(r['gate_s']) for r in p['warm']], 0.5) for p in parts if p):.2f}")
        rows.append(row)
    out.append(table(["rows", *ENGINES], rows))
    return out


def accuracy_section(acc: list[dict[str, str]]) -> list[str]:
    if not acc:
        return []
    out = ["\n## Detection accuracy (host DuckDB, every gate run regardless of upstream verdict)"]
    rows = []
    for r in acc:
        rows.append([r["defect"], r["rate"], r["injected"], r["layer"], r["expected_status"], r["status"],
                     "✅" if r["verdict_ok"] == "True" else "❌", r["tp"], r["fp"], r["fn"],
                     r["reported_count"] or "-"])  # fmt: skip
    out.append(table(["defect", "rate", "injected rows", "layer", "expected", "actual", "verdict", "TP", "FP",
                      "FN", "reported count"], rows))  # fmt: skip
    tp = sum(int(r["tp"]) for r in acc)
    fp = sum(int(r["fp"]) for r in acc)
    fn = sum(int(r["fn"]) for r in acc)
    verdicts = sum(r["verdict_ok"] == "True" for r in acc)
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    out.append(f"\nCheck-level: precision {precision:.3f}, recall {recall:.3f} (TP {tp}, FP {fp}, FN {fn}). "
               f"Gate verdicts correct: {verdicts}/{len(acc)}.")  # fmt: skip
    return out


def tools_section(tools: list[dict[str, str]]) -> list[str]:
    if not tools:
        return []
    out = ["\n## Tool comparison (same six checks, DuckDB/Polars/pandas, fresh process per run)"]
    groups: dict[tuple[int, str], list[dict[str, str]]] = defaultdict(list)
    for r in tools:
        groups[(int(r["rows"]), r["tool"])].append(r)
    rows = []
    for (size, tool), rs in sorted(groups.items()):
        ok = [r for r in rs if r["correct"] != "error"]
        if not ok:
            rows.append([human(size), tool, "error", "", "", "", rs[0].get("wrong_checks", "")])
            continue
        rows.append([human(size), tool, f"{statistics.median(float(r['process_s']) for r in ok):.2f}",
                     f"{statistics.median(float(r['run_s']) for r in ok):.2f}",
                     mib(statistics.median(float(r["max_rss_bytes"]) for r in ok)),
                     f"{sum(r['correct'] == 'True' for r in ok)}/{len(rs)}", ok[0]["counts"]])  # fmt: skip
    out.append(table(["rows", "tool", "process s (median)", "check run s", "peak RSS MiB", "correct verdicts",
                      "reported counts"], rows))  # fmt: skip
    return out


def spark_section(spark: list[dict[str, str]]) -> list[str]:
    if not spark:
        return []
    out = ["\n## Spark job vs gate on the lakehouse sample (median of repeats)"]
    rows = []
    for layer in LAYERS:
        rs = [r for r in spark if r["layer"] == layer]
        if not rs:
            continue
        sp = statistics.median(float(r["spark_s"]) for r in rs)
        gate = statistics.median(float(r["gate_s"]) for r in rs)
        proc = statistics.median(float(r["gate_process_s"]) for r in rs)
        rows.append([layer, f"{sp:.1f}", f"{gate:.2f}", f"{proc:.2f}", f"{100 * proc / sp:.0f}%", len(rs)])
    out.append(table(["layer", "Spark job s", "gate s", "gate process s", "gate process / Spark", "n"], rows))
    return out


def main() -> int:
    specs = json.loads((RESULTS / "specs.json").read_text()) if (RESULTS / "specs.json").exists() else {}
    parts = ["# Benchmark summary", "", "Generated by `python -m benchmarks.analyze` from the raw files in this folder.",
             "", "```json", json.dumps(specs, indent=2), "```", ""]  # fmt: skip
    parts += perf_sections(read(RESULTS / "perf_raw.csv"), read(RESULTS / "pipeline_raw.csv"))
    parts += spark_section(read(RESULTS / "spark_reference.csv"))
    parts += accuracy_section(read(RESULTS / "accuracy.csv"))
    parts += tools_section(read(RESULTS / "tools.csv"))
    (RESULTS / "summary.md").write_text("\n".join(parts) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
