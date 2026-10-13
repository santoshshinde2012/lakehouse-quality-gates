"""Check relative links and #anchors in the repository's Markdown files.

``python scripts/check_links.py`` prints every broken link (missing file or missing heading anchor) and
exits 1 if there is any. External links (http, https, mailto) are not fetched.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"!?\[(?:[^\]\[]|\[[^\]]*\])*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
FENCE = re.compile(r"^(```|~~~).*?^\1", re.M | re.S)
INLINE_CODE = re.compile(r"`[^`\n]*`")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$", re.M)
EXTERNAL = ("http://", "https://", "mailto:")


def markdown_files(root: Path = ROOT) -> list[Path]:
    """Every tracked-looking Markdown file, skipping virtualenvs, caches and node_modules."""
    skip = {".venv", "node_modules", ".git", ".mypy_cache", ".pytest_cache", ".ruff_cache", "reports"}
    return sorted(p for p in root.rglob("*.md") if not skip & set(p.relative_to(root).parts))


def slug(heading: str) -> str:
    """GitHub's anchor for a heading: lower case, punctuation dropped, spaces to hyphens."""
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # keep link text only
    text = re.sub(r"<[^>]+>", "", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors(path: Path) -> set[str]:
    text = FENCE.sub("", path.read_text(encoding="utf-8"))
    seen: Counter[str] = Counter()
    result: set[str] = set()
    for match in HEADING.finditer(text):
        base = slug(match.group(1))
        result.add(base if not seen[base] else f"{base}-{seen[base]}")
        seen[base] += 1
    return result


def check_file(path: Path, root: Path = ROOT) -> list[str]:
    """Return one message per broken link in ``path``."""
    text = INLINE_CODE.sub("", FENCE.sub("", path.read_text(encoding="utf-8")))
    errors: list[str] = []
    for match in LINK.finditer(text):
        target = match.group(1)
        if target.startswith(EXTERNAL):
            continue
        file_part, _, anchor = target.partition("#")
        dest = path if not file_part else (path.parent / unquote(file_part)).resolve()
        where = f"{path.relative_to(root)}: {target}"
        if not dest.exists():
            errors.append(f"{where} -> missing {file_part}")
        elif anchor and dest.suffix == ".md" and anchor not in anchors(dest):
            errors.append(f"{where} -> no heading #{anchor} in {dest.relative_to(root)}")
    return errors


def main(argv: list[str]) -> int:
    root = Path(argv[0]).resolve() if argv else ROOT
    errors = [e for p in markdown_files(root) for e in check_file(p, root)]
    for error in errors:
        print(error, file=sys.stderr)
    print(f"checked {len(markdown_files(root))} Markdown files: {len(errors)} broken links")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
