"""Write each shared piece of documentation once, and copy it everywhere else it appears.

A source page marks the shared text as a region::

    <!-- region:quickstart -->
    ...
    <!-- /region -->

Any other page includes it by repository-relative path and region name::

    <!-- include:docs/guides/quickstart.md#quickstart -->
    ...copy, rewritten by this script...
    <!-- /include -->

``python scripts/doc_snippets.py`` refreshes every include; ``--check`` exits 1 if a copy is stale, names a
missing region, or a region holds a relative link (it would break when copied to another folder).
``duplicates()`` lists prose written twice by hand outside these blocks; the tests keep it empty.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REGION = re.compile(r"<!-- region:(?P<name>[\w-]+) -->\n(?P<body>.*?)<!-- /region -->", re.S)
INCLUDE = re.compile(
    r"(?P<open><!-- include:(?P<path>[\w./-]+)#(?P<name>[\w-]+) -->\n).*?(?P<close><!-- /include -->)", re.S
)
RELATIVE_LINK = re.compile(r"\]\((?!https?://|mailto:|#)[^)]+\)")
MERMAID = re.compile(r"```mermaid\n.*?```", re.S)
SEPARATOR = re.compile(r"^\|[-:| ]+\|$")
# Pages that may repeat other pages: the changelog, generated results and imported research.
EXCLUDED = ("CHANGELOG.md", "docs/results/", "docs/research/")


def pages(root: Path = ROOT) -> list[Path]:
    """README.md, CONTRIBUTING.md and every docs page except the excluded ones."""
    found = [root / "README.md", root / "CONTRIBUTING.md", *sorted((root / "docs").rglob("*.md"))]
    return [p for p in found if p.exists() and not str(p.relative_to(root)).startswith(EXCLUDED)]


def regions(root: Path = ROOT) -> dict[str, str]:
    """``"path#name" -> body`` for every region in the docs."""
    return {
        f"{p.relative_to(root).as_posix()}#{m['name']}": m["body"]
        for p in pages(root)
        for m in REGION.finditer(p.read_text(encoding="utf-8"))
    }


def render(text: str, found: dict[str, str]) -> tuple[str, list[str]]:
    """Return ``text`` with every include refreshed, plus the keys of includes with no region."""
    missing: list[str] = []

    def replace(m: re.Match[str]) -> str:
        key = f"{m['path']}#{m['name']}"
        if key not in found:
            missing.append(key)
            return m.group(0)
        return m["open"] + found[key] + m["close"]

    return INCLUDE.sub(replace, text), missing


def problems(root: Path = ROOT, fix: bool = False) -> list[str]:
    """Stale or broken includes (rewritten first when ``fix``) and regions with relative links."""
    found = regions(root)
    errors = [
        f"{key}: region has a relative link" for key, body in found.items() if RELATIVE_LINK.search(body)
    ]
    for page in pages(root):
        text = page.read_text(encoding="utf-8")
        new, missing = render(text, found)
        rel = page.relative_to(root).as_posix()
        errors += [f"{rel}: no region {key}" for key in missing]
        if new != text:
            if fix:
                page.write_text(new, encoding="utf-8")
            else:
                errors.append(f"{rel}: stale include (run: python scripts/doc_snippets.py)")
    return errors


def duplicates(root: Path = ROOT, min_length: int = 50) -> dict[str, list[str]]:
    """Lines of at least ``min_length`` characters that appear in more than one page, outside includes."""
    seen: dict[str, set[str]] = defaultdict(set)
    for page in pages(root):
        text = INCLUDE.sub("", MERMAID.sub("", page.read_text(encoding="utf-8")))
        for raw in text.splitlines():
            line = raw.strip()
            if len(line) >= min_length and not SEPARATOR.match(line):
                seen[line].add(page.relative_to(root).as_posix())
    return {line: sorted(where) for line, where in seen.items() if len(where) > 1}


def main(argv: list[str]) -> int:
    check = "--check" in argv
    errors = problems(fix=not check)
    errors += [f"written twice by hand: {where}: {line[:80]}" for line, where in duplicates().items()]
    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
