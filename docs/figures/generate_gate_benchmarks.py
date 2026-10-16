"""Plot archived gate measurements; never run or seed the lakehouse.

Reproduce with the existing plotting environment:
    python3 docs/figures/generate_gate_benchmarks.py

Alternatively use an isolated dependency without changing the project:
    uv run --no-project --with matplotlib python docs/figures/generate_gate_benchmarks.py
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "build"
SOURCE = ROOT / "docs/results/benchmarks/2026-10-14-v0.4.1"
METHODOLOGY = ROOT / "docs/benchmarks/methodology.md"
SIZES = (10_000, 100_000, 1_000_000, 5_000_000)
LAYERS = ("bronze", "silver", "gold")
ENGINES = ("duckdb", "trino", "container")
WARM_RUNS = 5
COLD_RUNS = 1
EXPECTED_OBSERVATIONS = len(SIZES) * len(LAYERS) * len(ENGINES) * (WARM_RUNS + COLD_RUNS)
LABELS = {"duckdb": "DuckDB host", "trino": "Trino client", "container": "DuckDB container"}
COLORS = {"duckdb": "#4658c9", "trino": "#d97706", "container": "#0d9488"}
INK = "#172033"
MUTED = "#536176"
GRID = "#d8e0eb"
FIGURE_SIZE = (24, 12)
EXPORT_DPI = 3200 / FIGURE_SIZE[0]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def finite_nonnegative(value: str, field: str) -> float:
    number = float(value)
    require(math.isfinite(number) and number >= 0, f"invalid {field}: {value!r}")
    return number


def aggregate(folder: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source = folder / "perf_raw.csv"
    with source.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    require(
        len(rows) == EXPECTED_OBSERVATIONS,
        f"expected {EXPECTED_OBSERVATIONS} observations in this archived artifact set",
    )
    groups: dict[tuple[int, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = int(row["rows"]), row["engine"], row["layer"]
        require(
            key[0] in SIZES and key[1] in ENGINES and key[2] in LAYERS, f"unexpected measurement cell {key}"
        )
        require(row["phase"] in ("warm", "cold"), "unexpected run phase")
        require(row["status"] == "passed", f"a {key} run did not pass: {row['status']}")
        finite_nonnegative(row["gate_s"], "gate_s")
        finite_nonnegative(row["max_rss_bytes"], "max_rss_bytes")
        groups[key].append(row)

    summary = []
    for size in SIZES:
        for engine in ENGINES:
            record: dict[str, Any] = {"orders": size, "engine": engine}
            gate_medians = []
            rss_medians = []
            for layer in LAYERS:
                measurements = groups[(size, engine, layer)]
                warm = [row for row in measurements if row["phase"] == "warm"]
                cold = [row for row in measurements if row["phase"] == "cold"]
                require(
                    len(warm) == WARM_RUNS and len(cold) == COLD_RUNS,
                    f"{size}/{engine}/{layer}: expected {WARM_RUNS} warm and {COLD_RUNS} cold runs",
                )
                require({int(row["run"]) for row in warm} == {1, 2, 3, 4, 5}, "warm repeat IDs differ")
                require(int(cold[0]["run"]) == 0, "cold repeat ID differs")
                gate = statistics.median(float(row["gate_s"]) for row in warm)
                rss = statistics.median(int(row["max_rss_bytes"]) / 1024**2 for row in warm)
                record[f"{layer}_warm_n"] = len(warm)
                record[f"{layer}_gate_median_s"] = gate
                record[f"{layer}_rss_median_mib"] = rss
                gate_medians.append(gate)
                rss_medians.append(rss)
            record["warm_observations"] = sum(record[f"{layer}_warm_n"] for layer in LAYERS)
            record["sum_layer_gate_medians_s"] = sum(gate_medians)
            record["max_layer_rss_medians_mib"] = max(rss_medians)
            summary.append(record)

    checks: dict[str, set[int]] = defaultdict(set)
    with gzip.open(folder / "perf_raw.jsonl.gz", "rt", encoding="utf-8") as stream:
        for line in stream:
            payload = json.loads(line)
            checks[payload["layer"]].add(len(payload["checks"]))
    require(
        dict(checks) == {"bronze": {14}, "silver": {21}, "gold": {11}},
        "archived check set differs from 46 checks",
    )
    specs = json.loads((folder / "specs.json").read_text(encoding="utf-8"))
    require(specs["commit"] == "e39dc18", "archived benchmark commit differs")
    require(specs["packages"]["soda-core"] == "4.26.0", "archived Soda version differs")
    require(specs["trino_heap"] == "-Xmx4G", "archived Trino heap differs")
    metadata = {
        "artifact_set": folder.name,
        "scope": (
            "Historical 46-check gate set; current 53-check gates "
            "and frozen-ref publication not rebenchmarked."
        ),
        "measurements": {"total": len(rows), "passed": len(rows), "warm": 180, "cold_excluded": 36},
        "samples_per_point": "5 warm processes per layer; 15 warm observations across three layers",
        "latency_aggregation": (
            "Sum of the bronze, silver and gold warm gate_s medians; not a whole-pipeline median."
        ),
        "rss_aggregation": (
            "Maximum of the three per-layer warm medians of process peak RSS, converted to MiB."
        ),
        "rss_scope": "Trino client only: server memory is excluded. DuckDB RSS includes its embedded engine.",
        "plot_axes": "True logarithmic order-count x-axis; linear y-axes starting at zero; no error bars.",
        "specs": specs,
        "summary": summary,
        "sources": [],
    }
    for path in (source, folder / "specs.json", folder / "perf_raw.jsonl.gz", METHODOLOGY):
        metadata["sources"].append(
            {
                "path": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return summary, metadata


def plot(
    summary: list[dict[str, Any]], metadata: dict[str, Any], output: Path, *, rss: bool = False
) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FixedLocator, NullLocator

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 22,
            "text.color": INK,
            "axes.labelcolor": INK,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "svg.fonttype": "none",
        }
    )
    fig, ax = plt.subplots(figsize=FIGURE_SIZE, dpi=EXPORT_DPI)
    fig.subplots_adjust(left=0.075, right=0.93, top=0.925, bottom=0.11)
    title = "Client peak memory" if rss else "Gate runtime"
    description = (
        f"{metadata['scope']} {metadata['samples_per_point']}. "
        f"{metadata['rss_aggregation'] if rss else metadata['latency_aggregation']} "
        f"{metadata['rss_scope'] if rss else ''} {metadata['plot_axes']}"
    )
    field = "max_layer_rss_medians_mib" if rss else "sum_layer_gate_medians_s"
    for engine in ENGINES:
        values = [
            next(row[field] for row in summary if row["orders"] == size and row["engine"] == engine)
            for size in SIZES
        ]
        legend_label = (
            "Trino client (server excluded)"
            if engine == "trino" and rss
            else "Trino"
            if engine == "trino"
            else LABELS[engine]
        )
        ax.plot(
            SIZES, values, color=COLORS[engine], label=legend_label, marker="o", markersize=11, linewidth=3.6
        )
        label = f"{values[-1]:.1f} MiB" if rss else f"{values[-1]:.3f} s"
        offsets = (
            {"duckdb": (14, -35), "trino": (14, -15), "container": (14, 12)}
            if rss
            else {"duckdb": (14, -15), "trino": (14, -5), "container": (14, 10)}
        )
        ax.annotate(
            label,
            (SIZES[-1], values[-1]),
            xytext=offsets[engine],
            textcoords="offset points",
            color=COLORS[engine],
            fontsize=21,
            weight="bold",
        )
    ax.set_xscale("log")
    ax.set_xlim(8000, 7_000_000)
    ax.xaxis.set_major_locator(FixedLocator(SIZES))
    ax.set_xticklabels(["10k", "100k", "1M", "5M"])
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_xlabel("Input orders (log scale)", labelpad=13)
    ax.set_ylabel("Max. layer median RSS (MiB)" if rss else "Sum of layer medians (s)", labelpad=15)
    ax.set_ylim(0, 800 if rss else 33)
    ax.set_yticks(range(0, 801, 100) if rss else range(0, 31, 5))
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(GRID)
    ax.tick_params(length=0, pad=10)
    fig.legend(
        *ax.get_legend_handles_labels(),
        loc="upper left",
        bbox_to_anchor=(0.075, 0.995),
        frameon=False,
        ncol=3,
        fontsize=21,
        handlelength=2.3,
        columnspacing=2.2,
    )

    name = "gate-client-rss-scaling" if rss else "gate-latency-scaling"
    paths = []
    for extension in ("png", "svg"):
        path = output / f"{name}.{extension}"
        fig.savefig(
            path,
            dpi=EXPORT_DPI,
            facecolor="white",
            metadata={"Title": title, "Description": description},
        )
        paths.append(path)
    plt.close(fig)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=SOURCE)
    parser.add_argument("--output-dir", type=Path, default=OUT)
    args = parser.parse_args()
    summary, metadata = aggregate(args.source_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.output_dir / "gate-benchmark-summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    paths = plot(summary, metadata, args.output_dir) + plot(summary, metadata, args.output_dir, rss=True)
    import matplotlib

    metadata["plotting_library"] = {"name": "matplotlib", "version": matplotlib.__version__}
    metadata["figure_dimensions_px"] = {"width": 3200, "height": 1600}
    metadata["figure_icons"] = None
    metadata["figure_notes"] = (
        "Native 24 by 12 inch landscape figures have no visible titles or decorative icons. "
        "Aggregation axes, units, log x-axis, legends, endpoints, and Trino server exclusion are visible. "
        "Historical check set, sample count, source, environment, scope limitations, and full methods "
        "are retained in captions and metadata, including SVG Title and Description."
    )
    provenance = args.output_dir / "gate-benchmark-provenance.json"
    provenance.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    for path in [*paths, summary_path, provenance]:
        print(path)
    for row in summary:
        print(
            f"{row['orders']:>7} {row['engine']:>9}: {row['sum_layer_gate_medians_s']:.4f} s; "
            f"{row['max_layer_rss_medians_mib']:.6f} MiB"
        )


if __name__ == "__main__":
    main()
