"""The data behind the figures in docs/figures: survey sources and the lifecycle check counts."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

FIGURES = Path(__file__).resolve().parents[2] / "docs" / "figures"
sys.path.insert(0, str(FIGURES))

import lifecycle_data  # noqa: E402
import survey_data  # noqa: E402

ROW = {
    "label": "Top team challenge",
    "source": "dbt Labs",
    "value_pct": "56",
    "previous_pct": "",
    "publisher": "dbt Labs",
    "url": "https://www.getdbt.com/",
}


def test_committed_survey_sources_load_with_public_urls() -> None:
    signals = survey_data.load_signals()
    assert [s.value_pct for s in signals] == [74, 67, 59, 56]
    assert signals[1].previous_pct == 55
    assert all(s.url.startswith("https://") for s in signals)


def test_survey_rejects_bad_rows() -> None:
    assert survey_data.parse_signals([ROW])[0].previous_pct is None
    with pytest.raises(ValueError, match="https"):
        survey_data.parse_signals([{**ROW, "url": "http://example.com"}])
    with pytest.raises(ValueError, match="range"):
        survey_data.parse_signals([{**ROW, "value_pct": "101"}])
    with pytest.raises(ValueError, match="missing"):
        survey_data.parse_signals([{"label": "x"}])


def test_count_levels_splits_fail_and_warn() -> None:
    contract = {
        "checks": [{"schema": {"name": "s"}}, {"row_count": {"threshold": {"must_be_greater_than": 0}}}],
        "columns": [
            {"name": "a", "checks": [{"missing": None}, {"invalid": {"threshold": {"level": "warn"}}}]},
            {"name": "b"},
        ],
    }
    assert lifecycle_data.count_levels([contract]) == (3, 1)
    assert lifecycle_data.count_levels([]) == (0, 0)


def test_repo_contracts_give_the_documented_split() -> None:
    counts = lifecycle_data.load_layer_counts()
    assert counts == {"bronze": (11, 3), "silver": (21, 1), "gold": (16, 1)}
    assert sum(f + w for f, w in counts.values()) == 53


@pytest.mark.skipif(importlib.util.find_spec("matplotlib") is None, reason="matplotlib not installed")
def test_lifecycle_renders(tmp_path: Path) -> None:
    import generate_lifecycle

    out = tmp_path / "lifecycle.png"
    generate_lifecycle.render(lifecycle_data.load_layer_counts(), out)
    assert out.stat().st_size > 0
