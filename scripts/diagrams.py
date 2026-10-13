"""Keep the Mermaid blocks in the Markdown docs in sync with docs/diagrams/*.mmd.

A block looks like this in README.md or docs/architecture.md:

    <!-- diagram:01-layered-architecture -->
    ```mermaid
    ...contents of docs/diagrams/01-layered-architecture.mmd...
    ```
    <!-- /diagram -->

``python scripts/diagrams.py`` rewrites the blocks from the sources.
``python scripts/diagrams.py --check`` exits 1 if a block is stale or names a missing source.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ROOT / "docs" / "diagrams"
DOCS = (ROOT / "README.md", ROOT / "docs" / "architecture.md", ROOT / "docs" / "system-design.md")
BLOCK = re.compile(
    r"(?P<open><!-- diagram:(?P<name>[\w-]+) -->\n)```mermaid\n.*?```\n(?P<close><!-- /diagram -->)",
    re.S,
)


def render(text: str, sources: Path = SOURCES) -> tuple[str, list[str]]:
    """Return ``text`` with every diagram block refreshed, plus the names of missing sources."""
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        source = sources / f"{match['name']}.mmd"
        if not source.is_file():
            missing.append(match["name"])
            return match[0]
        body = source.read_text(encoding="utf-8").rstrip("\n")
        return f"{match['open']}```mermaid\n{body}\n```\n{match['close']}"

    return BLOCK.sub(replace, text), missing


def main(argv: list[str]) -> int:
    check = "--check" in argv
    stale: list[str] = []
    for doc in DOCS:
        text = doc.read_text(encoding="utf-8")
        new, missing = render(text)
        for name in missing:
            print(f"{doc.relative_to(ROOT)}: no source docs/diagrams/{name}.mmd", file=sys.stderr)
        if missing:
            return 1
        if new != text:
            stale.append(str(doc.relative_to(ROOT)))
            if not check:
                doc.write_text(new, encoding="utf-8")
    if check and stale:
        print(f"stale Mermaid blocks in {', '.join(stale)}; run: make diagrams", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
