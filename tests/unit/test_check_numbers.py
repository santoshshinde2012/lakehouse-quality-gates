"""The headline numbers quoted in the docs match the result files (scripts/check_numbers.py)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_numbers", ROOT / "scripts" / "check_numbers.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look the module up while the class is built
    spec.loader.exec_module(module)
    return module


def test_docs_numbers_match_results() -> None:
    assert _load().problems() == []


def test_a_wrong_number_is_reported(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "README.md").write_text("Runs 999 tests at 12% coverage.\n")
    found = _load().problems(tmp_path)
    assert any("says 999" in line for line in found)
    assert any("says 12" in line for line in found)
