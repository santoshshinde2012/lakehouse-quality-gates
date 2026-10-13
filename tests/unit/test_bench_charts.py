"""Benchmark charts are generated from the raw results (scripts/bench_charts.py)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bench_charts", ROOT / "scripts" / "bench_charts.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_results_page_is_current() -> None:
    assert _load().main(["--check"]) == 0


def test_every_section_has_a_chart_table_and_raw_link() -> None:
    charts = _load()
    page = charts.Charts(charts.latest_data()).render()
    sections = page.split("\n### ")[1:]
    assert len(sections) >= 7
    for text in sections:
        assert "```mermaid" in text
        assert "|---|" in text
        assert "Raw data: [`" in text


def test_helpers() -> None:
    charts = _load()
    assert charts.human(5_000_000) == "5M"
    assert charts.human(10_000) == "10k"
    assert charts.nearest_rank([1.0, 2.0, 3.0, 4.0, 5.0], 0.95) == 5.0
    chart = charts.xychart("", "t", ["a"], "s", [("x", [1.0])], "bar")
    assert "bar [1.000]" in chart
    assert 'pie showData title T\n  "a" : 2.00' in charts.pie("", "T", [("a", 2.0)])
