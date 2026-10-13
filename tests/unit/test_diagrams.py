"""The Mermaid blocks in the docs (README, architecture, system design) match docs/diagrams/*.mmd."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]
INIT = re.compile(r"^%%\{init: \{'theme':'base','themeVariables':\{")


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("diagrams", ROOT / "scripts" / "diagrams.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_markdown_blocks_match_sources() -> None:
    assert _load().main(["--check"]) == 0


def test_every_source_uses_the_shared_theme() -> None:
    sources = sorted((ROOT / "docs" / "diagrams").glob("*.mmd"))
    assert len(sources) == 13
    first_lines = {s.read_text(encoding="utf-8").splitlines()[0] for s in sources}
    for source in sources:
        header = source.read_text(encoding="utf-8").splitlines()[1:4]
        keys = [line.split(":")[0] for line in header]
        assert keys == ["%% Title", "%% Purpose", "%% Last updated"], source.name
    assert len(first_lines) == 1
    assert INIT.match(first_lines.pop())


def test_every_source_is_embedded_somewhere() -> None:
    docs = "".join(doc.read_text() for doc in _load().DOCS)
    for source in (ROOT / "docs" / "diagrams").glob("*.mmd"):
        assert f"<!-- diagram:{source.stem} -->" in docs


def test_render_reports_missing_sources(tmp_path: Path) -> None:
    text = "<!-- diagram:nope -->\n```mermaid\nflowchart LR\n```\n<!-- /diagram -->\n"
    new, missing = _load().render(text, tmp_path)
    assert new == text
    assert missing == ["nope"]
