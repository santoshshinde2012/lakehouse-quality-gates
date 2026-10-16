"""Agree-before-enforce lifecycle figure, drawn with matplotlib shapes and its built-in DejaVu Sans font.

The fail/warn split per layer is counted from the contracts in contracts/retail/, so the figure follows the repo.
Run: uv run --no-project --with matplotlib --with pyyaml python docs/figures/generate_lifecycle.py [OUT.png]
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
import yaml

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, FancyBboxPatch, Polygon, Rectangle, Wedge

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CONTRACTS = ROOT / "contracts" / "retail"
LAYERS = ("bronze", "silver", "gold")

INK, SEC, MUT, RULE, TRACK = "#0F172A", "#475569", "#64748B", "#CBD5E1", "#F1F5F9"
BLUE, WHITE, BLK = "#1D4ED8", "#FFFFFF", "#000000"
MED = {"bronze": ("#9A5B26", "#FBF3EC"), "silver": ("#5B6B80", "#F1F5F9"), "gold": ("#8B6508", "#FBF6E6")}
W, H = 1600, 900
STEPS = ("Agree", "Encode", "Review", "Observe", "Enforce")
ROLES = ("Producer", "Steward / Consumer", "Platform", "Owner")


def _checks(contract: dict) -> list[dict]:
    found = list(contract.get("checks") or [])
    for column in contract.get("columns") or []:
        found += column.get("checks") or []
    return found


def count_levels(folder: Path = CONTRACTS) -> dict[str, tuple[int, int]]:
    """(fail, warn) checks per layer; a check warns when its threshold says level: warn."""
    counts = {}
    for layer in LAYERS:
        fail = warn = 0
        for path in sorted((folder / layer).glob("*.yml")):
            for check in _checks(yaml.safe_load(path.read_text(encoding="utf-8"))):
                body = next(iter(check.values())) or {}
                if (body.get("threshold") or {}).get("level") == "warn":
                    warn += 1
                else:
                    fail += 1
        counts[layer] = (fail, warn)
    return counts


def text(ax, x, y, s, size, color=INK, weight="normal", ha="left", va="center"):
    ax.text(x, y, s, fontsize=size, color=color, fontweight=weight, ha=ha, va=va, family="DejaVu Sans")


def icon(ax, kind, x, y, color):
    """Simple outline icons built from shapes (about 40 units wide)."""
    lw = 4
    if kind == "Agree":  # two people
        for dx, r in ((-9, 1.0), (11, 0.85)):
            ax.add_patch(Circle((x + dx, y - 9 * r), 7 * r, fill=False, ec=color, lw=lw))
            ax.add_patch(Wedge((x + dx, y + 16), 14 * r, 180, 360, width=0.01, fill=False, ec=color, lw=lw))
    elif kind == "Encode":  # document with lines
        ax.add_patch(
            FancyBboxPatch(
                (x - 15, y - 20), 30, 40, boxstyle="round,pad=0,rounding_size=4", fill=False, ec=color, lw=lw
            )
        )
        for dy in (-7, 1, 9):
            ax.plot([x - 8, x + 8], [y + dy, y + dy], color=color, lw=lw - 1, solid_capstyle="round")
    elif kind == "Review":  # merge
        ax.plot([x - 10, x - 10], [y - 18, y + 18], color=color, lw=lw)
        ax.plot([x - 10, x + 10], [y - 6, y + 8], color=color, lw=lw)
        for cx, cy in ((x - 10, y - 18), (x - 10, y + 18), (x + 10, y + 8)):
            ax.add_patch(Circle((cx, cy), 5, fc=WHITE if color != WHITE else BLUE, ec=color, lw=lw - 1))
    elif kind == "Observe":  # bell
        ax.add_patch(
            Polygon(
                [
                    (x - 16, y + 10),
                    (x - 12, y - 6),
                    (x - 6, y - 16),
                    (x + 6, y - 16),
                    (x + 12, y - 6),
                    (x + 16, y + 10),
                ],
                closed=True,
                fill=False,
                ec=color,
                lw=lw,
                joinstyle="round",
            )
        )
        ax.add_patch(Wedge((x, y + 13), 6, 0, 180, fill=False, ec=color, lw=lw - 1))
    else:  # shield with check
        ax.add_patch(
            Polygon(
                [
                    (x, y - 21),
                    (x + 17, y - 14),
                    (x + 15, y + 6),
                    (x, y + 20),
                    (x - 15, y + 6),
                    (x - 17, y - 14),
                ],
                closed=True,
                fill=False,
                ec=color,
                lw=lw,
                joinstyle="round",
            )
        )
        ax.plot(
            [x - 8, x - 2, x + 9],
            [y, y + 7, y - 7],
            color=color,
            lw=lw,
            solid_capstyle="round",
            solid_joinstyle="round",
        )


def draw(out: Path) -> None:
    counts = count_levels()
    total_fail = sum(f for f, _ in counts.values())
    total_warn = sum(w for _, w in counts.values())
    fig = plt.figure(figsize=(16, 9), dpi=200, facecolor="white")
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_xlim(0, W)
    ax.set_ylim(H, 0)
    ax.axis("off")
    xs, ty, r = (200, 500, 800, 1100, 1400), 200, 62
    ax.plot([xs[0] + r, xs[1] - r], [ty, ty], color=MUT, lw=3, ls=(0, (4, 3)))
    ax.plot([xs[1] + r, xs[4] - r], [ty, ty], color=INK, lw=3)
    for i in range(1, 4):
        mx = (xs[i] + xs[i + 1]) / 2
        ax.add_patch(Polygon([(mx + 10, ty), (mx - 7, ty - 9), (mx - 7, ty + 9)], closed=True, color=INK))
    for i, (x, name) in enumerate(zip(xs, STEPS, strict=True)):
        if i == 0:
            ax.add_patch(Circle((x, ty), r, fc=TRACK, ec=MUT, lw=2.5, ls=(0, (3, 3))))
            col = MUT
        elif i == len(xs) - 1:
            ax.add_patch(Circle((x, ty), r, fc=BLUE, ec=BLK, lw=1.5))
            col = WHITE
        else:
            ax.add_patch(Circle((x, ty), r, fc=WHITE, ec=BLK, lw=1.5))
            col = BLUE
        icon(ax, name, x, ty, col)
        text(ax, x, ty + r + 52, name, 24, SEC if i == 0 else INK, "bold", "center", "baseline")
    ax.plot([80, W - 80], [400, 400], color=RULE, lw=1)
    text(ax, 80, 500, str(total_fail), 50, INK, "bold", va="baseline")
    text(ax, 200, 500, "fail", 19, SEC, va="baseline")
    text(ax, 300, 500, str(total_warn), 50, INK, "bold", va="baseline")
    text(ax, 370, 500, "warn", 19, SEC, va="baseline")
    lx, scale = 200, 620 / max(f + w for f, w in counts.values())
    for j, layer in enumerate(LAYERS):
        fail, warn = counts[layer]
        y = 550 + j * 80
        dark, light = MED[layer]
        text(ax, 80, y + 24, layer.capitalize(), 18)
        ax.add_patch(Rectangle((lx, y), fail * scale, 48, fc=dark, ec="none"))
        ax.add_patch(Rectangle((lx + fail * scale, y), warn * scale, 48, fc=light, ec="none"))
        ax.add_patch(Rectangle((lx, y), (fail + warn) * scale, 48, fill=False, ec=BLK, lw=1.5))
        ax.plot([lx + fail * scale] * 2, [y, y + 48], color=BLK, lw=1.5)
        text(ax, lx + 14, y + 24, str(fail), 17, WHITE, "bold")
        text(ax, lx + (fail + warn) * scale + 16, y + 24, f"+{warn}", 17, SEC, "bold")
    rx = 1100
    text(ax, rx, 470, "Who agrees", 19, INK, "bold", va="baseline")
    for j, who in enumerate(ROLES):
        y = 520 + j * 62
        ax.plot([rx, rx], [y, y + 40], color=BLUE if j == len(ROLES) - 1 else RULE, lw=4)
        text(ax, rx + 22, y + 20, who, 18)
    ax.plot([80, 140], [H - 48, H - 48], color=MUT, lw=3, ls=(0, (4, 3)))
    text(ax, 156, H - 48, "dashed = team practice", 14, MUT)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=200, facecolor="white", metadata={"Title": "Agree before enforce lifecycle"})
    plt.close(fig)
    print(out, counts)


if __name__ == "__main__":
    draw(Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "build" / "agree-before-enforce-lifecycle.png")
