"""Plot the saved WAP fixture evidence as SVG and PNG, without running the lakehouse.

Reproduce: python docs/figures/generate_snapshot_proof.py
Requires Matplotlib for SVG and PNG export. All counts and snapshot IDs come from the
preserved good.json and bad.json; validation fails if their publication invariants differ.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVIDENCE = ROOT / "docs/results/runs/2026-10-14-v1.0-review/e2e-wap"
OUT = Path(__file__).resolve().parent / "build"
INK = "#172033"
MUTED = "#536176"
LINE = "#d8e0eb"
BLUE = "#4658c9"
GREEN = "#19734a"
RED = "#b83d43"
TABLES = ("bronze.orders_raw", "bronze.customers_raw")
FIGURE_SIZE = (24, 12)
EXPORT_DPI = 3200 / FIGURE_SIZE[0]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


@dataclass(frozen=True)
class TableEvidence:
    name: str
    before_rows: int
    approved_rows: int
    candidate_rows: int
    after_rows: int
    before_snapshot: int
    approved_snapshot: int
    candidate_snapshot: int


def row_count(value: object) -> int:
    require(type(value) is int and value >= 0, f"invalid row count: {value!r}")
    return int(value)


def load_evidence(folder: Path) -> tuple[list[TableEvidence], str]:
    good: list[dict[str, Any]] = json.loads((folder / "good.json").read_text(encoding="utf-8"))
    bad: list[dict[str, Any]] = json.loads((folder / "bad.json").read_text(encoding="utf-8"))
    require(
        [entry["layer"] for entry in good] == ["bronze", "silver", "gold"], "good run lacks all three layers"
    )
    for entry in good:
        require(entry["outcome"] == "published", "good run did not publish every layer")
        require(entry["gate"] in ("passed", "passed_with_warnings"), "good publication lacks a passing audit")
        published = entry["published"]["tables"]
        require(set(published) == set(entry["written"]["tables"]), "publication omitted primary tables")
        require(set(published) <= set(entry["audited_pins"]), "published tables lack audit pins")
        for table, state in published.items():
            pin = entry["audited_pins"][table]
            written = entry["written"]["tables"][table]
            require(state["main_snapshot"] == pin, f"{table}: publication differs from its audited snapshot")
            require(
                written["branch_snapshot"] == pin, f"{table}: written snapshot differs from its audit pin"
            )
            require(state["main_rows"] == written["branch_rows"], f"{table}: published row count differs")
            require(state["main_before"] == written["main_snapshot"], f"{table}: previous main differs")
            require(state["main_snapshot"] != state["main_before"], f"{table}: good run did not move main")

    require(len(bad) == 1 and bad[0]["layer"] == "bronze", "bad run did not stop at bronze")
    failed = bad[0]
    require(failed["outcome"] == "blocked" and failed["gate"] == "blocked", "bad gate was not blocked")
    require(failed["published"] is None, "bad run unexpectedly published")
    approved = good[0]["published"]["tables"]
    rejected = failed["written"]["tables"]
    require(set(approved) == set(rejected) == set(TABLES), "bronze evidence contains unexpected tables")
    require(set(TABLES) <= set(failed["audited_pins"]), "bad audit pins are incomplete")

    tables = []
    for table in TABLES:
        before = good[0]["written"]["tables"][table]
        passed = approved[table]
        blocked = rejected[table]
        require(blocked["main_snapshot"] == passed["main_snapshot"], f"{table}: blocked run changed main")
        require(blocked["main_rows"] == passed["main_rows"], f"{table}: blocked run changed main rows")
        require(
            blocked["branch_snapshot"] == failed["audited_pins"][table], f"{table}: bad audit pin differs"
        )
        require(
            blocked["branch_snapshot"] != blocked["main_snapshot"], f"{table}: rejected candidate is on main"
        )
        tables.append(
            TableEvidence(
                table,
                row_count(before["main_rows"]),
                row_count(passed["main_rows"]),
                row_count(blocked["branch_rows"]),
                row_count(blocked["main_rows"]),
                passed["main_before"],
                passed["main_snapshot"],
                blocked["branch_snapshot"],
            )
        )
    date = datetime.strptime(good[0]["run_id"].split("-")[0], "%Y%m%dT%H%M%SZ").strftime("%-d %B %Y")
    return tables, date


def draw(tables: list[TableEvidence], date: str, output: Path) -> tuple[Path, Path]:
    """Show row-count evidence in a wide chart; detail lives in metadata and the caption."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    orders, customers = tables
    title = "A blocked batch stayed off main"
    description = (
        f"Saved synthetic bronze CLI fixture evidence from {date}. "
        f"The approved main held {orders.approved_rows} order rows "
        f"and {customers.approved_rows} customer rows. "
        f"The rejected candidate held {orders.candidate_rows} orders "
        f"and {customers.candidate_rows} customers. "
        f"Main retained {orders.after_rows} orders and {customers.after_rows} customers "
        "at the same snapshot IDs: "
        f"orders {orders.approved_snapshot}, customers {customers.approved_snapshot}. "
        "The zero-based chart presents row counts, not performance or general defect detection accuracy."
    )
    series = (
        ("Approved main", BLUE, [t.approved_rows for t in tables]),
        ("Blocked candidate", RED, [t.candidate_rows for t in tables]),
        ("Main after block", GREEN, [t.after_rows for t in tables]),
    )
    maximum = max(5, math.ceil(max(t.candidate_rows for t in tables) / 5) * 5)
    with plt.rc_context(
        {
            "font.family": "DejaVu Sans",
            "font.size": 22,
            "text.color": INK,
            "axes.labelcolor": INK,
            "xtick.color": INK,
            "ytick.color": MUTED,
            "svg.fonttype": "none",
            "svg.hashsalt": "snapshot-publication-proof",
        }
    ):
        fig, ax = plt.subplots(figsize=FIGURE_SIZE, dpi=EXPORT_DPI, facecolor="white")
        fig.subplots_adjust(left=0.065, right=0.975, bottom=0.085, top=0.925)
        width = 0.23
        for index, (label, color, values) in enumerate(series):
            positions = [group + (index - 1) * width for group in range(len(tables))]
            bars = ax.bar(positions, values, width=width * 0.88, label=label, color=color, zorder=3)
            ax.bar_label(bars, padding=10, fontsize=25, color=color, weight="bold")
        ax.set_xticks(range(len(tables)), ["Orders", "Customers"])
        ax.set_ylim(0, maximum + 2)
        ax.set_yticks(range(0, maximum + 1, 5))
        ax.set_ylabel("Rows", labelpad=16)
        ax.grid(axis="y", color=LINE, linewidth=1)
        ax.set_axisbelow(True)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.spines["bottom"].set_color(LINE)
        ax.tick_params(axis="both", length=0, pad=12)
        fig.legend(
            *ax.get_legend_handles_labels(),
            loc="upper left",
            bbox_to_anchor=(0.065, 0.995),
            ncol=3,
            frameon=False,
            fontsize=22,
            columnspacing=2.0,
        )
        output.mkdir(parents=True, exist_ok=True)
        svg = output / "snapshot-publication-proof.svg"
        png = svg.with_suffix(".png")
        for artifact in (svg, png):
            fig.savefig(
                artifact,
                dpi=EXPORT_DPI,
                facecolor="white",
                metadata={
                    "Title": title,
                    "Description": description,
                    "Creator": "generate_snapshot_proof.py",
                },
            )
        plt.close(fig)
    return svg, png


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--output-dir", type=Path, default=OUT)
    args = parser.parse_args()
    tables, date = load_evidence(args.evidence_dir)
    svg, png = draw(tables, date, args.output_dir)
    print(svg)
    print(png)
    for table in tables:
        print(
            f"{table.name}: approved {table.approved_rows}, candidate {table.candidate_rows}, "
            f"main after block {table.after_rows}; retained snapshot {table.approved_snapshot}"
        )


if __name__ == "__main__":
    main()
