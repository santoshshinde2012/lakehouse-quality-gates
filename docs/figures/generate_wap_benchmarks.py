"""Plot archived WAP costs from paired repeats without running the lakehouse.

Reproduce: python3 docs/figures/generate_wap_benchmarks.py
Requires Matplotlib. This script writes only its own figures, derived CSVs, and metadata beside itself.
The archived experiment used 46 checks and the earlier publisher; it does not time the hardened publisher.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from statistics import median

import matplotlib

matplotlib.use("Agg")
from matplotlib import pyplot as plt
from matplotlib.lines import Line2D

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "build"
SOURCE = ROOT / "docs/results/benchmarks/2026-10-14-wap/wap_overhead.csv"
AIRFLOW_SOURCE = ROOT / "docs/results/benchmarks/2026-10-14-v0.4.1/airflow_runs.csv"
LAYERS = ("bronze", "silver", "gold")
SIZES = (100_000, 1_000_000)
TIME_FIELDS = (
    "direct_s",
    "direct_wall_s",
    "write_s",
    "write_wall_s",
    "gate_s",
    "publish_s",
    "publish_wall_s",
)
DERIVED_FIELDS = (
    "spark_extra_s",
    "audit_elapsed_s",
    "added_wall_s",
    "recorded_work_plus_audit_s",
    "wap_wall_s",
)
INK = "#172033"
MUTED = "#536176"
LINE = "#d8e0eb"
BLUE = "#4658c9"
GREEN = "#19734a"
FIGURE_SIZE = (24, 12)
EXPORT_DPI = 3200 / FIGURE_SIZE[0]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_paired() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    with SOURCE.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    paired: list[dict[str, object]] = []
    groups: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    seen: set[tuple[int, str, int]] = set()
    for row in rows:
        size, repeat, layer = int(row["rows"]), int(row["repeat"]), row["layer"]
        if size not in SIZES or layer not in LAYERS or row["gate_status"] != "passed":
            raise ValueError(f"unexpected measurement or non-passing gate: {row}")
        key = (size, layer, repeat)
        if key in seen:
            raise ValueError(f"duplicate paired repeat: {key}")
        seen.add(key)
        times = {field: Decimal(row[field]) for field in TIME_FIELDS}
        if any(not value.is_finite() or value < 0 for value in times.values()):
            raise ValueError(f"invalid duration in {key}")
        result: dict[str, object] = {
            "rows": size,
            "repeat": repeat,
            "layer": layer,
            "table_rows": int(row["table_rows"]),
            "gate_status": row["gate_status"],
            **times,
            "spark_extra_s": times["write_s"] + times["publish_s"] - times["direct_s"],
            "audit_elapsed_s": times["gate_s"],
            "added_wall_s": times["write_wall_s"]
            + times["gate_s"]
            + times["publish_wall_s"]
            - times["direct_wall_s"],
            "recorded_work_plus_audit_s": times["write_s"] + times["gate_s"] + times["publish_s"],
            "wap_wall_s": times["write_wall_s"] + times["gate_s"] + times["publish_wall_s"],
        }
        paired.append(result)
        groups[(size, layer)].append(result)
    if set(groups) != {(size, layer) for size in SIZES for layer in LAYERS}:
        raise ValueError("archive does not contain the six expected size/layer groups")
    summaries: list[dict[str, object]] = []
    for size in SIZES:
        for layer in LAYERS:
            measurements = groups[(size, layer)]
            if {row["repeat"] for row in measurements} != {1, 2, 3}:
                raise ValueError(f"expected repeats 1, 2, 3 for {size}/{layer}")
            summary: dict[str, object] = {"rows": size, "layer": layer, "n": len(measurements)}
            for field in (*TIME_FIELDS, *DERIVED_FIELDS):
                values = [row[field] for row in measurements]
                summary[f"median_{field}"] = median(values)
                if field in ("spark_extra_s", "audit_elapsed_s", "added_wall_s"):
                    summary[f"min_{field}"] = min(values)
                    summary[f"max_{field}"] = max(values)
            summaries.append(summary)
    return paired, summaries


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def observations(paired: list[dict[str, object]], summary: dict[str, object], field: str) -> list[float]:
    return [
        float(row[field])
        for row in paired
        if row["rows"] == summary["rows"] and row["layer"] == summary["layer"]
    ]


def plot(paired: list[dict[str, object]], summaries: list[dict[str, object]]) -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 22,
            "text.color": INK,
            "axes.labelcolor": INK,
            "xtick.color": MUTED,
            "ytick.color": INK,
            "axes.edgecolor": LINE,
            "svg.fonttype": "none",
            "svg.hashsalt": "archived-wap-cost",
        }
    )
    fig, axes = plt.subplots(1, 2, figsize=FIGURE_SIZE, dpi=EXPORT_DPI, sharey=True)
    fig.subplots_adjust(left=0.15, right=0.975, top=0.925, bottom=0.11, wspace=0.16)

    positions = [0, 1, 2, 3.7, 4.7, 5.7]
    labels = [f"{size} orders · {layer.title()}" for size in ("100k", "1M") for layer in LAYERS]
    for axis in axes:
        axis.set_ylim(6.15, -0.6)
        axis.set_yticks(positions, labels)
        axis.set_axisbelow(True)
        axis.grid(axis="x", color=LINE, alpha=0.8)
        axis.axhspan(-0.45, 2.45, color="#f6f8fc", zorder=0)
        axis.spines[["top", "right", "left"]].set_visible(False)
        axis.tick_params(axis="y", length=0, labelsize=20, pad=14)
        axis.tick_params(axis="x", labelsize=20, length=0, pad=10)
    axes[0].set_xlabel("Extra Spark time / audit elapsed (s)", fontsize=22, labelpad=18)
    axes[1].set_xlabel("Added wall time (s)", fontsize=22, labelpad=18)
    axes[0].set_xlim(0, 6)
    axes[1].set_xlim(0, 22)
    axes[0].set_xticks([0, 1, 2, 3, 4, 5, 6])
    axes[1].set_xticks([0, 5, 10, 15, 20])
    axes[1].tick_params(axis="y", labelleft=False)

    for position, summary in zip(positions, summaries, strict=True):
        spark = observations(paired, summary, "spark_extra_s")
        audit = observations(paired, summary, "audit_elapsed_s")
        wall = observations(paired, summary, "added_wall_s")
        for axis, values, offset, color in (
            (axes[0], spark, -0.11, BLUE),
            (axes[0], audit, 0.17, GREEN),
            (axes[1], wall, 0.0, BLUE),
        ):
            y = position + offset
            center = median(values)
            axis.plot([min(values), max(values)], [y, y], color=color, linewidth=3, alpha=0.3)
            axis.scatter(values, [y - 0.055, y, y + 0.055], s=48, color=color, alpha=0.35, zorder=3)
            axis.scatter([center], [y], s=160, color=color, edgecolor="white", linewidth=1.8, zorder=4)
            axis.text(
                max(values) + (0.12 if axis is axes[0] else 0.25),
                y + 0.075,
                f"{center:.2f}",
                fontsize=20,
                color=color,
                weight="bold",
            )
    fig.legend(
        handles=[
            Line2D([], [], color=BLUE, marker="o", linewidth=3, label="Spark extra"),
            Line2D([], [], color=GREEN, marker="o", linewidth=3, label="Audit elapsed"),
            Line2D([], [], color=MUTED, marker="o", markersize=11, linestyle="none", label="Median"),
            Line2D(
                [],
                [],
                color=MUTED,
                marker="o",
                markersize=6,
                alpha=0.35,
                linestyle="none",
                label="Paired run",
            ),
            Line2D([], [], color=MUTED, linewidth=3, alpha=0.3, label="Observed range"),
        ],
        loc="upper left",
        bbox_to_anchor=(0.15, 0.995),
        frameon=False,
        ncol=5,
        fontsize=20,
        columnspacing=1.9,
    )
    description = (
        "Archived write-audit-publish costs at 100k and 1M input orders, three paired repeats per layer. "
        "Historical 46-check contracts and earlier publisher; current hardened WAP is not rebenchmarked. "
        "The baseline is a direct write without a quality gate. "
        "Spark extra is write plus publish minus direct write within each repeat; audit elapsed is gate_s. "
        "Added wall time is write-command plus audit plus publish-command elapsed minus direct-command elapsed. "
        "Large points show medians; smaller points are repeats and lines are observed ranges, not confidence intervals."
    )
    for suffix in ("png", "svg"):
        fig.savefig(
            OUT / f"wap-cost.{suffix}",
            dpi=EXPORT_DPI,
            facecolor="white",
            metadata={
                "Title": "Write-audit-publish overhead",
                "Description": description,
                "Creator": "generate_wap_benchmarks.py",
            },
        )
    plt.close(fig)


def evaluate_airflow() -> dict[str, object]:
    with AIRFLOW_SOURCE.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    durations: dict[str, list[float]] = defaultdict(list)
    statuses: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        value = float(row["duration_s"])
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid Airflow duration")
        durations[row["variant"]].append(value)
        statuses[row["variant"]][row["state"]] += 1
    return {
        "source": str(AIRFLOW_SOURCE.relative_to(ROOT)),
        "sha256": sha256(AIRFLOW_SOURCE),
        "scope": "Archived post-write direct-main DAG, not WAP; not plotted in the WAP cost figure.",
        "observations": {
            variant: {
                "n": len(values),
                "median_run_elapsed_s": median(values),
                "states": dict(statuses[variant]),
            }
            for variant, values in durations.items()
        },
        "editorial_decision": (
            "Keep the WAP cost figure focused. Airflow has 3 successful good runs and 2 failed bad runs, "
            "not 3 of each."
        ),
    }


def main() -> None:
    paired, summaries = read_paired()
    OUT.mkdir(parents=True, exist_ok=True)
    paired_path, summary_path = OUT / "wap-cost-paired.csv", OUT / "wap-cost-summary.csv"
    write_csv(paired_path, paired)
    write_csv(summary_path, summaries)
    plot(paired, summaries)
    metadata = {
        "figure": "wap-cost",
        "source": str(SOURCE.relative_to(ROOT)),
        "source_sha256": sha256(SOURCE),
        "generator_sha256": sha256(Path(__file__)),
        "paired_data_sha256": sha256(paired_path),
        "summary_data_sha256": sha256(summary_path),
        "source_rows": len(paired),
        "groups": len(summaries),
        "n_per_group": 3,
        "units": "seconds",
        "png_dimensions": [3200, 1600],
        "title_icon": None,
        "figure_notes": (
            "Native 24 by 12 inch landscape with physical metrics and units on the x-axes, six explicit size/layer labels, "
            "and a mark legend. No visible title, panel headings, or decorative icons. "
            "Scope and baseline are retained in caption and SVG Title/Description metadata."
        ),
        "software": {"matplotlib": matplotlib.__version__},
        "formulae": {
            "median_spark_extra_s": "median per paired repeat of write_s + publish_s - direct_s",
            "median_audit_elapsed_s": "median per group of gate_s (outer audit elapsed clock)",
            "median_added_wall_s": (
                "median per paired repeat of write_wall_s + gate_s + publish_wall_s - direct_wall_s"
            ),
            "recorded_work_plus_audit_s": (
                "write_s + gate_s + publish_s per repeat; includes audit elapsed, not pure compute"
            ),
        },
        "limits": [
            "Direct-write baseline has no quality gate.",
            "46-check contracts and the pre-hardening publisher.",
            "Current saved manifests/frozen publication refs have not been rebenchmarked.",
            "Three repeats; ranges are observations, not confidence intervals.",
            "Adding component medians need not equal the median of per-repeat totals or differences.",
            "Wall total combines command clocks and the audit clock; it is not an Airflow DAG duration.",
        ],
        "caption": (
            "Archived WAP costs at 100k and 1M orders on an M1 Pro laptop with Docker Desktop, "
            "with three paired runs per layer. Extra Spark time is computed as branch write plus "
            "publish minus direct write within each repeat before taking the median; audit elapsed "
            "is shown separately. Added wall time is write-command plus audit plus publish-command "
            "elapsed minus direct-command elapsed, against a baseline without a gate. Lines show "
            "observed ranges, not confidence intervals. The experiment used 46 checks and the earlier "
            "publisher; current hardened WAP has not been rebenchmarked."
        ),
        "airflow_evaluation": evaluate_airflow(),
    }
    (OUT / "wap-cost.meta.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    for summary in summaries:
        print(
            f"{summary['rows']}/{summary['layer']}: Spark extra {summary['median_spark_extra_s']} s; "
            f"audit elapsed {summary['median_audit_elapsed_s']} s; "
            f"added wall {summary['median_added_wall_s']} s (n=3)"
        )


if __name__ == "__main__":
    main()
