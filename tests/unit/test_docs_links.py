"""Every relative link and #anchor in the repository's Markdown resolves (scripts/check_links.py)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_links", ROOT / "scripts" / "check_links.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_repository_has_no_broken_links() -> None:
    links = _load()
    errors = [e for p in links.markdown_files() for e in links.check_file(p)]
    assert errors == []


def test_slug_matches_github() -> None:
    slug = _load().slug
    assert slug("Settings (`DQ_*`)") == "settings-dq_"
    assert slug("12. Benchmarks and performance analysis") == "12-benchmarks-and-performance-analysis"
    assert slug("Airflow DAG `lakehouse_retail_medallion_dq`") == "airflow-dag-lakehouse_retail_medallion_dq"


def test_broken_file_and_anchor_are_reported(tmp_path: Path) -> None:
    links = _load()
    (tmp_path / "a.md").write_text("# Title\n\n[ok](#title) [gone](b.md) [bad](#nope)\n`[code](x.md)`\n")
    errors = links.check_file(tmp_path / "a.md", tmp_path)
    assert errors == ["a.md: b.md -> missing b.md", "a.md: #nope -> no heading #nope in a.md"]
