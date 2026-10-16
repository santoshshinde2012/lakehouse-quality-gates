"""Build article diagrams, benchmark plots, and a Markdown comparison table."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
import statistics
import subprocess
from collections.abc import Sequence
from html import escape
from pathlib import Path
from typing import Any

from diagram_icons import icon_svg
from theme import BAR, BLUE, CHART_DPI, CHART_SIZE, GREEN, GRID, INK, LINE, MUTED, RED

HERE = Path(__file__).resolve().parent
OUT = HERE / "build"
ROOT = HERE.parents[1]
TOOL_DATA = ROOT / "docs/results/benchmarks/2026-10-14-tool-rerun"
BENCHMARK_LARGE_ROWS = 1_000_000
BENCHMARK_FIGURE_SIZE = CHART_SIZE
BENCHMARK_EXPORT_DPI = CHART_DPI
SCORED_CHECKS = (
    "order_id_missing",
    "customer_id_missing",
    "order_id_unique",
    "status_valid",
    "amount_min",
    "customer_ref",
)


def source_name(path: Path) -> str:
    return path.relative_to(ROOT).as_posix() if path.is_relative_to(ROOT) else path.as_posix()


def tool_benchmark_data() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read individual repeats and export exact plotted medians with their provenance."""
    from importlib.metadata import PackageNotFoundError, version

    folder = TOOL_DATA
    source = folder / "tools.csv"
    versions_path = folder / "tools_versions.json"
    specs_path = folder / "tools_metadata.json"
    harness_path = folder / "tools_logs/run_tools.py"
    raw_path = folder / "tools_raw.jsonl"
    versions = json.loads(versions_path.read_text(encoding="utf-8"))
    specs = json.loads(specs_path.read_text(encoding="utf-8"))
    if not specs.get("all_verdicts_match") or specs.get("versions") != versions:
        raise ValueError("tool run was incomplete, failed, or has inconsistent environment records")
    for path, key in ((source, "csv_sha256"), (raw_path, "raw_sha256"), (harness_path, "harness_sha256")):
        if hashlib.sha256(path.read_bytes()).hexdigest() != specs.get(key):
            raise ValueError(f"tool provenance hash mismatch: {path.name}")
    fixtures = {fixture["rows"]: fixture for fixture in specs["fixtures"]}
    if set(fixtures) != {100_000, 1_000_000}:
        raise ValueError("tool benchmark must verify both requested fixture sizes")
    for rows, fixture in fixtures.items():
        if fixture["customers"] != rows // 10 or fixture["ground_truth"] != {
            "order_id_missing": 0,
            **dict.fromkeys(
                ("customer_id_missing", "order_id_unique", "status_valid", "amount_min", "customer_ref"),
                rows // 1000,
            ),
        }:
            raise ValueError(f"unexpected measured fixture counts at {rows} orders")
    labels = {
        "soda": "Soda",
        "pointblank": "Pointblank",
        "datacontract": "datacontract-cli",
        "pandera": "Pandera",
        "gx": "GX Core",
    }
    backends = {
        "soda": "DuckDB",
        "datacontract": "DuckDB",
        "pointblank": "Polars",
        "pandera": "Polars",
        "gx": "pandas",
    }
    with source.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        required = {
            "rows",
            "tool",
            "repeat",
            "run_s",
            "process_s",
            "max_rss_bytes",
            "correct",
            "wrong_checks",
            "counts",
        }
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(
                f"tools.csv is missing columns: {sorted(required - set(reader.fieldnames or []))}"
            )
        records = list(reader)
    if not records:
        raise ValueError("tools.csv contains no individual runs")
    raw_records = [json.loads(line) for line in raw_path.read_text(encoding="utf-8").splitlines()]

    def identities(rows: list[dict[str, Any]]) -> list[tuple[int, int, str]]:
        return [(int(row["rows"]), int(row["repeat"]), row["tool"]) for row in rows]

    order = identities(records)
    if (
        len(order) != len(set(order))
        or identities(raw_records) != order
        or identities(specs["execution_order"]) != order
    ):
        raise ValueError(
            "CSV, raw results, and execution metadata must identify the same ordered unique runs"
        )
    for record, raw in zip(records, raw_records, strict=True):
        if raw.get("returncode") != 0 or raw.get("correct") is not True or raw.get("wrong_checks") != []:
            raise ValueError("raw execution did not complete with the expected verdicts")
        result = raw["result"]
        checks = result["checks"]
        if result["tool"] != record["tool"] or set(checks) != set(SCORED_CHECKS):
            raise ValueError("raw result has inconsistent tool or check identities")
        if any(checks[name]["failed"] is not (name != "order_id_missing") for name in SCORED_CHECKS):
            raise ValueError("raw check verdict differs from fixture expectations")
        for key in ("run_s", "cpu_s"):
            if float(record[key]) != round(result[key], 3):
                raise ValueError(f"CSV {key} disagrees with raw measurement")
        if (
            float(record["process_s"]) != round(raw["process_s"], 3)
            or int(record["max_rss_bytes"]) != result["max_rss_bytes"]
        ):
            raise ValueError("CSV process timing or peak RSS disagrees with raw measurement")
        counts = "|".join(f"{name}={checks[name].get('count')}" for name in SCORED_CHECKS)
        if record["counts"] != counts:
            raise ValueError("CSV diagnostic counts disagree with the raw identified checks")
    groups = {}
    for record in records:
        key = (int(record["rows"]), record["tool"])
        if key[0] not in (100_000, 1_000_000) or key[1] not in labels:
            raise ValueError(f"unexpected scale/tool in tools.csv: {key}")
        if record["correct"] != "True" or record["wrong_checks"]:
            raise ValueError(f"a failed tool process cannot supply a valid median: {key}")
        for field in ("run_s", "process_s", "max_rss_bytes"):
            value = float(record[field])
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"invalid {field} in tools.csv: {record[field]}")
        groups.setdefault(key, []).append(record)
    expected = {(rows, tool) for rows in (100_000, 1_000_000) for tool in labels}
    if set(groups) != expected:
        raise ValueError(f"missing tool benchmark groups: {sorted(expected - set(groups))}")
    aggregates = []
    for rows in (100_000, 1_000_000):
        for tool, label in labels.items():
            group = groups[(rows, tool)]
            repeats = sorted(int(record["repeat"]) for record in group)
            if len(repeats) < 3 or repeats != list(range(len(repeats))):
                raise ValueError(
                    f"expected at least three unique consecutive repeats for {rows}/{tool}; got {repeats}"
                )
            rss_bytes = statistics.median(int(record["max_rss_bytes"]) for record in group)
            runtimes_by_repeat = {int(record["repeat"]): float(record["run_s"]) for record in group}
            aggregates.append(
                {
                    "rows": rows,
                    "tool": tool,
                    "label": label,
                    "backend": backends[tool],
                    "n_runs": len(group),
                    "repeat_ids": "|".join(str(repeat) for repeat in repeats),
                    "median_run_s": statistics.median(float(record["run_s"]) for record in group),
                    "min_run_s": min(float(record["run_s"]) for record in group),
                    "max_run_s": max(float(record["run_s"]) for record in group),
                    "median_process_s": statistics.median(float(record["process_s"]) for record in group),
                    "median_max_rss_bytes": rss_bytes,
                    "median_max_rss_mib": rss_bytes / 2**20,
                    "correct_runs": sum(record["correct"] == "True" for record in group),
                    **{f"run_s_repeat_{repeat}": runtimes_by_repeat[repeat] for repeat in repeats},
                }
            )
    if len({row["n_runs"] for row in aggregates}) != 1:
        raise ValueError("all tool/size groups must have the same repeat count")
    if specs["repeats"] != aggregates[0]["n_runs"] or len(specs["execution_order"]) != len(records):
        raise ValueError("CSV repeat counts differ from the recorded execution metadata")
    renderer_versions = {}
    for package in ("seaborn", "matplotlib"):
        try:
            renderer_versions[package] = version(package)
        except PackageNotFoundError:
            renderer_versions[package] = None
    chart_maximum = max(
        2,
        math.ceil(
            max(row["median_run_s"] for row in aggregates if row["rows"] == BENCHMARK_LARGE_ROWS) * 1.10 / 2
        )
        * 2,
    )
    with (OUT / "tool-benchmark-summary.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(aggregates[0]))
        writer.writeheader()
        writer.writerows(aggregates)
    metadata = {
        "schema_version": 1,
        "record_count": len(records),
        "group_count": len(aggregates),
        "records_are": "Individual tool executions, not preaggregated rows",
        "sources": [
            {
                "path": source_name(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in (source, versions_path, specs_path, harness_path, raw_path)
        ],
        "generator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "visual_source_sha256": hashlib.sha256((HERE / "diagram_icons.py").read_bytes()).hexdigest(),
        "versions": versions,
        "environment": specs,
        "renderer": {"library": "seaborn", "versions": renderer_versions},
        "presentation": {
            "width_px": 3200,
            "height_px": round(BENCHMARK_FIGURE_SIZE[1] * BENCHMARK_EXPORT_DPI),
            "visible_title": False,
            "layout": "wide landscape",
        },
        "derivation": {
            "group_by": ["rows", "tool"],
            "repeats": "All recorded repeats, including repeat 0; none discarded",
            "runtime": "statistics.median(float(run_s)) within each group",
            "process_runtime": "statistics.median(float(process_s)); "
            "exported but not used for chart/table runtime",
            "memory": "statistics.median(int(max_rss_bytes)) / 2**20, median of whole-process peak RSS",
            "display_rounding": {"chart_seconds": 3, "table_seconds": 3, "table_mib": 0},
            "timing_scope": "Tool imports, Parquet loading, setup and validation included; "
            "interpreter startup excluded",
            "backends": backends,
            "backend_source": "The tool runner functions in benchmarks/tools/run_tools.py",
            "chart_type": "Seaborn vertical median bar chart",
            "runtime_axis": {"scale": "linear", "units": "seconds", "minimum": 0, "maximum": chart_maximum},
            "chart_aggregation": "Seaborn estimator='median' over all five recorded run_s values per tool at 1M orders",
            "median_marker": "Bar height with exact seconds printed directly above each bar",
            "observations_display": "Figure shows medians only; all observations and observed ranges remain in the CSV and raw results",
            "error_bars": "Disabled explicitly; no confidence interval or observed range is drawn",
        },
        "verdict_matches": sum(record["correct"] == "True" for record in records),
        "verdict_note": "correct compares six scored pass/fail decisions; "
        "it does not prove diagnostic-count equivalence",
        "defect_count_equivalence_verified": False,
        "reported_counts_by_run": [
            {key: record[key] for key in ("rows", "tool", "repeat", "correct", "wrong_checks", "counts")}
            for record in records
        ],
        "limits": "Different selected backends and one local workload; "
        "not a universal ranking or cluster forecast",
        "exports": [
            "tool-benchmark-summary.csv",
            "tool-benchmark-1m.svg",
            "tool-benchmark-1m.png",
            "tool-benchmark-1m-seaborn.svg",
            "tool-benchmark-1m-seaborn.png",
            "tool-benchmark-table.md",
        ],
    }
    (OUT / "tool-benchmark-metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return aggregates, metadata


class Figure:
    def __init__(self, height: int, title: str, description: str, *, top: int = 0) -> None:
        content_height = height - top
        self.parts = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="3200" height="{content_height * 2}" '
            f'viewBox="0 {top} 1600 {content_height}" role="img" aria-labelledby="title description">',
            f'<title id="title">{escape(title)}</title>',
            f'<desc id="description">{escape(description)}</desc>',
            f'<rect x="0" y="{top}" width="1600" height="{content_height}" fill="#ffffff"/>',
            "<defs>",
        ]
        for name, color in (("blue", BLUE), ("green", GREEN), ("red", RED), ("gray", MUTED)):
            self.parts.append(
                f'<marker id="arrow-{name}" viewBox="0 0 12 12" refX="11" refY="6" '
                f'markerWidth="12" markerHeight="12" markerUnits="userSpaceOnUse" orient="auto">'
                f'<path d="M 1 1 L 11 6 L 1 11" fill="none" stroke="{color}" '
                'stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/></marker>'
            )
        self.parts.extend(["</defs>", '<g font-family="DejaVu Sans, Arial, sans-serif">'])

    def text(
        self,
        x: float,
        y: float,
        content: str | int | float,
        *,
        size: float = 28,
        color: str = INK,
        weight: int = 400,
        anchor: str = "start",
    ) -> None:
        self.parts.append(
            f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" '
            f'font-weight="{weight}" text-anchor="{anchor}">{escape(str(content))}</text>'
        )

    def icon(self, name: str, x: float, y: float, size: float = 48, color: str = BLUE) -> None:
        self.parts.append(icon_svg(name, x, y, size=size, color=color))

    def icon_label(
        self,
        center: float,
        baseline: float,
        label: str,
        name: str,
        label_width: float,
        *,
        text_size: float = 32,
        icon_size: float = 48,
        color: str = INK,
        icon_color: str = BLUE,
        weight: int = 600,
    ) -> None:
        start = center - (icon_size + 14 + label_width) / 2
        self.icon(name, start, baseline - text_size * 0.35 - icon_size / 2, icon_size, icon_color)
        self.text(start + icon_size + 14, baseline, label, size=text_size, color=color, weight=weight)

    def rect(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        *,
        fill: str = "#ffffff",
        stroke: str = LINE,
        radius: float = 18,
    ) -> None:
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="{radius}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="2"/>'
        )

    def arrow(self, path: str, kind: str = "blue") -> None:
        color = {"blue": BLUE, "green": GREEN, "red": RED, "gray": MUTED}[kind]
        self.parts.append(
            f'<path d="{path}" fill="none" stroke="{color}" stroke-width="4" '
            f'stroke-linejoin="round" marker-end="url(#arrow-{kind})"/>'
        )

    def card(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        lines: Sequence[str],
        *,
        detail: str | None = None,
        fill: str = "#ffffff",
        accent: str = BLUE,
    ) -> None:
        self.rect(x, y, width, height, fill=fill)
        self.parts.append(
            f'<rect x="{x + 1}" y="{y + 24}" width="5" height="{height - 48}" rx="2" fill="{accent}"/>'
        )
        for index, line in enumerate(lines):
            self.text(x + width / 2, y + 53 + index * 42, line, size=30, weight=600, anchor="middle")
        if detail:
            self.text(x + width / 2, y + height - 28, detail, size=25, color=MUTED, anchor="middle")

    def save(self, name: str) -> None:
        path = OUT / f"{name}.svg"
        path.write_text("\n".join([*self.parts, "</g>", "</svg>"]) + "\n", encoding="utf-8")
        rasterizer = shutil.which("rsvg-convert")
        if rasterizer is None:
            raise SystemExit("SVG files were written; rsvg-convert is required to render the PNGs.")
        subprocess.run([rasterizer, "--output", str(path.with_suffix(".png")), str(path)], check=True)
        print(path.with_suffix(".png"))


def ordinary_gate() -> None:
    fig = Figure(
        560,
        "A post-write gate stops downstream builds",
        "The current layer is written directly to main before its contracts are verified. "
        "A passing gate builds the next layer. A failing gate stops downstream builds, "
        "but the current layer is already visible to readers.",
        top=124,
    )
    fig.arrow("M 418 310 H 550")
    fig.arrow("M 830 292 H 955 V 220 H 1090", "green")
    fig.arrow("M 830 328 H 955 V 410 H 1090", "red")
    fig.arrow("M 253 370 V 429", "gray")
    fig.text(978, 199, "PASS", size=28, color=GREEN, weight=700)
    fig.text(978, 389, "FAIL", size=28, color=RED, weight=700)
    for x, y, width, label, fill, icon, color in (
        (88, 250, 330, "Write main", "#f1f2fc", "database", BLUE),
        (550, 250, 280, "Verify", "#ffffff", "shield_check", BLUE),
        (1090, 160, 420, "Next layer", "#edf7f0", "layers", GREEN),
        (1090, 350, 420, "Stop downstream", "#fff1f1", "stop", RED),
    ):
        fig.rect(x, y, width, 120, fill=fill)
        fig.icon(icon, x + width / 2 - 24, y + 14, 48, color)
        fig.text(x + width / 2, y + 100, label, size=36, weight=600, anchor="middle")
    fig.rect(88, 429, 750, 70, fill="#fff8e9", stroke="#e7d5a5")
    fig.icon("users", 120, 442, 44, MUTED)
    fig.text(180, 476, "Current layer already visible", size=32, weight=600)
    fig.save("ordinary-post-write-gate")


def wap_gate() -> None:
    fig = Figure(
        760,
        "Write, audit, then publish the approved snapshots",
        "Write each layer to a source audit branch, resolve the exact snapshots, "
        "audit them and persist a manifest. "
        "On pass, create private references at the saved snapshots and fast-forward main for each table. "
        "Clean private references and retain source audit branches. A failed gate leaves main unchanged. "
        "Required tables must exist. Publication is per table, "
        "not an atomic transaction for an entire layer. "
        "An interrupted publication can leave a partial layer; retry with the saved manifest.",
        top=142,
    )
    fig.arrow("M 512 242 H 576")
    fig.arrow("M 1024 242 H 1088")
    fig.arrow("M 1312 327 V 470", "green")
    fig.arrow("M 1312 327 V 390 H 288 V 470", "red")
    fig.arrow("M 1088 537 H 1024", "green")
    fig.text(1336, 435, "PASS", size=28, color=GREEN, weight=700)
    fig.text(312, 445, "FAIL", size=28, color=RED, weight=700)
    for x, y, lines, fill, icon, color in (
        (64, 182, ("1 · Write", "audit branch"), "#f1f2fc", "branch", BLUE),
        (576, 182, ("2 · Resolve", "snapshot pins"), "#f1f2fc", "layers", BLUE),
        (1088, 182, ("3 · Audit + save", "manifest"), "#ffffff", "shield_check", BLUE),
        (1088, 470, ("4 · Private ref", "at saved pin"), "#edf7f0", "lock", GREEN),
        (576, 470, ("5 · Advance", "each main"), "#edf7f0", "upload", GREEN),
    ):
        fig.rect(x, y, 448, 145, fill=fill)
        if icon == "shield_check":
            fig.icon(icon, x + 45, y + 22, 44, color)
            fig.icon("file", x + 45, y + 70, 44, color)
        else:
            fig.icon(icon, x + 44, y + 46, 52, color)
        for index, line in enumerate(lines):
            fig.text(x + 254, y + 58 + index * 47, line, size=34, weight=600, anchor="middle")
    fig.rect(64, 470, 448, 145, fill="#fff1f1", stroke="#e5bdc0")
    fig.icon("stop", 105, 517, 48, RED)
    fig.text(316, 557, "Main unchanged", size=34, weight=600, anchor="middle")
    for x, label, icon, label_width in (
        (64, "Keep source branch", "branch", 292),
        (576, "Clean private refs", "trash", 264),
        (1088, "Per-table publish", "database", 246),
    ):
        fig.rect(x, 675, 448, 60, fill="#f4f6fb")
        fig.icon_label(
            x + 224,
            716,
            label,
            icon,
            label_width,
            text_size=30,
            icon_size=44,
            color=MUTED,
            icon_color=MUTED,
            weight=400,
        )
    fig.save("write-audit-publish")


def benchmark() -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns
    from matplotlib.ticker import MultipleLocator

    aggregates, metadata = tool_benchmark_data()
    tools = sorted(
        (row for row in aggregates if row["rows"] == BENCHMARK_LARGE_ROWS),
        key=lambda row: row["median_run_s"],
    )
    n = tools[0]["n_runs"]
    labels = [row["label"] for row in tools]
    maximum = metadata["derivation"]["runtime_axis"]["maximum"]
    plot_data = {
        "tool": [row["label"] for row in tools for _ in range(n)],
        "run_s": [row[f"run_s_repeat_{repeat}"] for row in tools for repeat in range(n)],
    }
    with (
        sns.axes_style("whitegrid"),
        sns.plotting_context("talk"),
        plt.rc_context(
            {
                "font.family": "DejaVu Sans",
                "svg.fonttype": "none",
                "svg.hashsalt": "soda-tool-benchmark-seaborn",
            }
        ),
    ):
        plot_dpi = BENCHMARK_EXPORT_DPI
        fig, ax = plt.subplots(figsize=BENCHMARK_FIGURE_SIZE, dpi=plot_dpi, facecolor="white")
        fig.subplots_adjust(left=0.065, right=0.985, top=0.97, bottom=0.10)
        sns.barplot(
            data=plot_data,
            x="tool",
            y="run_s",
            order=labels,
            estimator="median",
            color=BAR,
            saturation=1,
            errorbar=None,
            width=0.52,
            ax=ax,
        )
        ax.set_ylim(0, maximum)
        ax.yaxis.set_major_locator(MultipleLocator(2))
        ax.set_xlabel("")
        ax.set_ylabel("Runtime (seconds)", fontsize=20, color=MUTED, labelpad=18)
        ax.grid(axis="x", visible=False)
        ax.grid(axis="y", color=GRID, linewidth=1)
        ax.set_axisbelow(True)
        ax.tick_params(axis="both", length=0, pad=12, labelsize=20, colors=MUTED)
        ax.tick_params(axis="x", labelsize=23, colors=INK)
        sns.despine(ax=ax, left=True, bottom=True)
        for bar, row in zip(ax.patches, tools, strict=True):
            if not math.isclose(bar.get_height(), row["median_run_s"], rel_tol=1e-12):
                raise ValueError(f"Seaborn bar height differs from the verified median for {row['label']}")
            ax.annotate(
                f"{row['median_run_s']:.3f} s",
                (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                xytext=(0, 12),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=25,
                fontweight="bold",
                color=INK,
            )
        svg_metadata = {
            "Date": None,
            "Title": "Benchmark runtime at one million orders",
            "Description": f"Seaborn vertical bars show medians of {n} observed run_s values from "
            f"{source_name(TOOL_DATA)}/tools.csv. Linear seconds axis starts at zero. "
            "Exact median runtimes are printed above the bars; individual runs and ranges remain in source data. "
            "Each run used a fresh process. "
            f"{metadata['derivation']['timing_scope']}. "
            f"{metadata['limits']}. {metadata['verdict_note']}. "
            "Backend choices, recorded versions, source hashes and exact medians "
            "are preserved in tool-benchmark-summary.csv and tool-benchmark-metadata.json.",
        }
        for stem in ("tool-benchmark-1m", "tool-benchmark-1m-seaborn"):
            fig.savefig(OUT / f"{stem}.png", dpi=plot_dpi, facecolor="white")
            fig.savefig(OUT / f"{stem}.svg", facecolor="white", metadata=svg_metadata)
        plt.close(fig)
    print(OUT / "tool-benchmark-1m.png")


def layer_architecture() -> None:
    fig = Figure(
        960,
        "Lakehouse layers on the write-audit-publish path",
        "Source CSV data enters Spark's bronze audit branches for orders_raw and customers_raw. "
        "Soda's 14 bronze checks audit exact snapshots before publication to bronze main. "
        "Spark then reads approved bronze main to build silver audit branches for orders and customers. "
        "Soda's 22 silver checks precede silver publication. Approved silver main feeds the gold "
        "daily_order_metrics audit branch; its 17 checks precede gold publication "
        "and consumer queries on main. "
        "A failed gate prevents that layer's main advancement and stops downstream layers. "
        "Airflow or the CLI orchestrates; versioned YAML supplies rules; "
        "DuckDB snapshot views and Soda audit; "
        "Lakekeeper's Iceberg REST catalog manages metadata; Iceberg tables hold the data. "
        "Spark writes and publishes. Each table publishes independently, not as a layer-wide transaction. "
        "The original direct-write paths remain available.",
        top=112,
    )
    fig.rect(64, 140, 448, 62, fill="#f4f6fb")
    fig.icon_label(288, 182, "Source CSV", "file", 205, icon_size=44)
    fig.text(576, 182, "WAP", size=30, color=MUTED)
    fig.text(1536, 182, "Airflow / CLI", size=30, color=MUTED, anchor="end")
    layers = [
        (64, "bronze", 14, ("orders_raw", "customers_raw"), "#fffaf4", "#855936"),
        (576, "silver", 22, ("orders", "customers"), "#f7f9fc", MUTED),
        (1088, "gold", 17, ("daily_order_metrics",), "#fffbed", "#8b6a14"),
    ]
    for x, layer, _checks, _tables, fill, color in layers:
        fig.rect(x, 226, 448, 550, fill=fill)
        fig.text(x + 24, 270, layer.upper(), size=32, color=color, weight=700)
    fig.arrow("M 288 202 V 304")
    fig.arrow("M 488 708 H 548 V 377 H 600")
    fig.arrow("M 1000 708 H 1060 V 377 H 1112")
    for x, _layer, checks, tables, _fill, _color in layers:
        center = x + 224
        fig.arrow(f"M {center} 450 V 513")
        fig.arrow(f"M {center} 605 V 666", "green")
        fig.text(center + 24, 646, "PASS", size=28, color=GREEN, weight=700)
        fig.rect(x + 24, 304, 400, 146, fill="#f1f2fc", stroke="#cbd1ee")
        fig.icon_label(center, 350, "Candidate branch", "branch", 315, icon_size=44)
        for index, table in enumerate(tables):
            fig.text(center, 394 + index * 38, table, size=30, anchor="middle")
        fig.rect(x + 24, 513, 400, 92)
        fig.icon_label(center, 571, f"Soda · {checks} checks", "shield_check", 308, text_size=33)
        fig.rect(x + 24, 666, 400, 85, fill="#edf7f0", stroke="#b8d7c6")
        fig.icon_label(center, 720, "Approved main", "database_check", 280, text_size=33, icon_color=GREEN)
    fig.arrow("M 1312 751 V 823", "green")
    fig.rect(1088, 823, 448, 64, fill="#edf7f0", stroke="#b8d7c6")
    fig.icon_label(1312, 866, "Consumers", "users", 197, icon_size=44, icon_color=GREEN)
    fig.rect(64, 823, 960, 64, fill="#fff1f1", stroke="#e5bdc0")
    fig.icon_label(
        544,
        866,
        "FAIL → layer main unchanged · downstream stops",
        "stop",
        833,
        text_size=30,
        icon_size=44,
        icon_color=RED,
    )
    fig.text(64, 932, "Publish per table", size=30, color=MUTED)
    fig.text(1536, 932, "53 checks", size=30, color=MUTED, weight=600, anchor="end")
    fig.save("lakehouse-layer-architecture")


def benchmark_table() -> None:
    """Generate the editable Markdown table from recorded runs."""
    aggregates, _ = tool_benchmark_data()
    cells = {(row["rows"], row["tool"]): row for row in aggregates}
    large = sorted(
        (row for row in aggregates if row["rows"] == BENCHMARK_LARGE_ROWS),
        key=lambda row: row["median_run_s"],
    )
    lines = [
        "| Tool | Backend | 100k orders: runtime (s) | 1M orders: runtime (s) | 1M orders: peak RSS (MiB) |",
        "| :--- | :--- | ---: | ---: | ---: |",
    ]
    for row in large:
        values = [
            row["label"],
            row["backend"],
            f"{cells[(100_000, row['tool'])]['median_run_s']:.3f}",
            f"{row['median_run_s']:.3f}",
            f"{row['median_max_rss_mib']:,.0f}",
        ]
        lines.append("| " + " | ".join(values) + " |")
    body = "\n".join(lines) + "\n"
    source = OUT / "tool-benchmark-table.md"
    source.write_text(body, encoding="utf-8")
    # Keep a single editable table format and remove obsolete exports.
    for suffix in (".mmd", ".png", ".svg"):
        source.with_suffix(suffix).unlink(missing_ok=True)
    print(source)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table-only", action="store_true", help="Refresh only the Markdown table and its recorded data"
    )
    parser.add_argument(
        "--tool-data", type=Path, default=TOOL_DATA, help="Directory with measured tools.csv and metadata"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=OUT, help="Where figures and summaries are written"
    )
    args = parser.parse_args()
    TOOL_DATA = args.tool_data.resolve()
    OUT = args.output_dir.resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    if not args.table_only:
        ordinary_gate()
        wap_gate()
        layer_architecture()
        benchmark()
    benchmark_table()
