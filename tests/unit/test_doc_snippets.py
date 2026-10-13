"""Shared documentation is written once (scripts/doc_snippets.py)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("doc_snippets", ROOT / "scripts" / "doc_snippets.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_includes_are_current() -> None:
    assert _load().problems() == []


def test_no_prose_is_written_twice() -> None:
    assert _load().duplicates() == {}


def test_include_is_refreshed_and_problems_reported(tmp_path: Path) -> None:
    snippets = _load()
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "a.md").write_text("<!-- region:r -->\nshared line\n<!-- /region -->\n")
    (tmp_path / "docs" / "b.md").write_text("<!-- region:bad -->\n[x](a.md)\n<!-- /region -->\n")
    (tmp_path / "README.md").write_text(
        "<!-- include:docs/a.md#r -->\nold\n<!-- /include -->\n"
        "<!-- include:docs/a.md#gone -->\n<!-- /include -->\n"
    )
    assert snippets.problems(tmp_path) == [
        "docs/b.md#bad: region has a relative link",
        "README.md: no region docs/a.md#gone",
        "README.md: stale include (run: python scripts/doc_snippets.py)",
    ]
    snippets.problems(tmp_path, fix=True)
    refreshed = (tmp_path / "README.md").read_text()
    assert "<!-- include:docs/a.md#r -->\nshared line\n<!-- /include -->" in refreshed


def test_duplicates_ignore_includes_and_short_lines(tmp_path: Path) -> None:
    snippets = _load()
    (tmp_path / "docs").mkdir()
    long = "x" * 60
    (tmp_path / "docs" / "a.md").write_text(f"{long}\nshort\n")
    included = f"<!-- include:docs/a.md#r -->\n{'y' * 60}\n<!-- /include -->\n"
    (tmp_path / "README.md").write_text(f"{long}\nshort\n{included}")
    (tmp_path / "CONTRIBUTING.md").write_text("y" * 60 + "\n")
    assert snippets.duplicates(tmp_path) == {long: ["README.md", "docs/a.md"]}
