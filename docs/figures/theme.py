"""Shared colours and sizes for the figures, so every script draws from one palette."""

from __future__ import annotations

# Benchmark charts and diagrams (generate_assets.py, generate_survey_chart.py).
INK = "#172033"
MUTED = "#536176"
LINE = "#d8e0eb"
BLUE = "#4658c9"
GREEN = "#19734a"
RED = "#b83d43"
BAR = "#4b6edb"
GRID = "#e9edf4"
CHART_SIZE = (24, 12)  # inches; exported 3200 px wide
CHART_DPI = 3200 / CHART_SIZE[0]

# Editorial figures (generate_lifecycle.py): slate text, one blue accent, medallion layer tones.
SLATE_INK = "#0F172A"
SLATE_SECONDARY = "#475569"
SLATE_MUTED = "#64748B"
SLATE_RULE = "#CBD5E1"
SLATE_TRACK = "#F1F5F9"
ACCENT = "#1D4ED8"
WHITE = "#FFFFFF"
BLACK = "#000000"
MEDALLION = {
    "bronze": ("#9A5B26", "#FBF3EC"),
    "silver": ("#5B6B80", "#F1F5F9"),
    "gold": ("#8B6508", "#FBF6E6"),
}
