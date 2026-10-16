"""Survey signals chart (seaborn, same style as the tool benchmark chart).

Data and public sources: survey_sources.csv next to this script.
Run: uv run --no-project --with matplotlib --with seaborn python docs/figures/generate_survey_chart.py [OUT.png]
"""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle

INK, MUTED, BAR, GRID = "#172033", "#536176", "#4b6edb", "#e9edf4"
SIZE = (24, 12)
DPI = 3200 / SIZE[0]
HERE = Path(__file__).resolve().parent


def load(path=HERE / "survey_sources.csv"):
    with open(path, encoding="utf-8", newline="") as fh:
        return [
            (
                r["label"],
                r["source"],
                int(r["value_pct"]),
                int(r["previous_pct"]) if r["previous_pct"] else None,
            )
            for r in csv.DictReader(fh)
        ]


data = load()


def main(out):
    labels = [d[0] for d in data]
    with (
        sns.axes_style("whitegrid"),
        sns.plotting_context("talk"),
        plt.rc_context({"font.family": "DejaVu Sans", "svg.fonttype": "none"}),
    ):
        fig, ax = plt.subplots(figsize=SIZE, dpi=DPI, facecolor="white")
        fig.subplots_adjust(left=0.30, right=0.93, top=0.95, bottom=0.14)
        sns.barplot(
            x=[d[2] for d in data],
            y=labels,
            orient="h",
            color=BAR,
            saturation=1,
            errorbar=None,
            width=0.52,
            ax=ax,
        )
        for bar, (lab, src, v, prev) in zip(ax.patches, data, strict=True):
            assert abs(bar.get_width() - v) < 1e-9
            y, h = bar.get_y(), bar.get_height()
            if prev:
                bar.set_width(prev)
                ax.add_patch(
                    Rectangle(
                        (prev, y), v - prev, h, facecolor="white", edgecolor=BAR, hatch="///", linewidth=0
                    )
                )
                ax.annotate(
                    f"earlier survey: {prev}%",
                    (prev, y + h),
                    xytext=(0, -8),
                    textcoords="offset points",
                    ha="center",
                    va="top",
                    fontsize=24,
                    color=MUTED,
                )
            ax.annotate(
                f"{v}%",
                (v, y + h / 2),
                xytext=(16, 0),
                textcoords="offset points",
                ha="left",
                va="center",
                fontsize=34,
                fontweight="bold",
                color=INK,
            )
        ax.set_xlim(0, 100)
        ax.set_xticks(range(0, 101, 25))
        ax.set_xticklabels([f"{t}%" for t in range(0, 101, 25)])
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.grid(axis="y", visible=False)
        ax.grid(axis="x", color=GRID, linewidth=1)
        ax.set_axisbelow(True)
        ax.tick_params(axis="both", length=0, pad=14, labelsize=24, colors=MUTED)
        ax.tick_params(axis="y", labelsize=30, colors=INK, pad=20)
        sns.despine(ax=ax, left=True, bottom=True)
        fig.canvas.draw()
        for tl, d in zip(ax.get_yticklabels(), data):
            tl.set_verticalalignment("bottom")
            x, y = tl.get_position()
            ax.annotate(
                d[1],
                (0, y),
                xytext=(-20, -8),
                textcoords="offset points",
                xycoords=("axes fraction", "data"),
                ha="right",
                va="top",
                fontsize=24,
                color=MUTED,
                annotation_clip=False,
            )
        fig.text(0.30, 0.04, "Separate self-reported surveys", fontsize=24, color=MUTED, ha="left")
        fig.savefig(
            out, dpi=DPI, facecolor="white", metadata={"Title": "Survey signals on data quality and trust"}
        )
        # QA: text in figure bounds and no overlaps
        r = fig.canvas.get_renderer()
        W, H = fig.bbox.width, fig.bbox.height
        boxes = [
            t.get_window_extent(r)
            for t in fig.findobj(matplotlib.text.Text)
            if t.get_text() and t.get_visible()
        ]
        fails = [b for b in boxes if b.x0 < 0 or b.y0 < 0 or b.x1 > W or b.y1 > H]
        ov = sum(1 for i, a in enumerate(boxes) for b in boxes[i + 1 :] if a.overlaps(b))
        print("out-of-bounds", len(fails), "overlaps", ov, "size", W, H)
        plt.close(fig)


if __name__ == "__main__":
    default = HERE / "build" / "agree-before-enforce-survey-signals.png"
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else default
    target.parent.mkdir(parents=True, exist_ok=True)
    main(target)
