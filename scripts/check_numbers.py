"""Check that the headline numbers quoted in the docs match the result files they come from.

Each rule pairs a pattern in README.md and docs/**/*.md with a value read from docs/results. ``--check`` (the
default) prints every mismatch and exits 1; ``make docs-check`` and the unit tests run it.
"""

from __future__ import annotations

import csv
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LATEST = ROOT / "docs" / "results" / "runs" / "2026-10-13-fresh-clone"
BENCH = ROOT / "docs" / "results" / "benchmarks"
# Targets ("≥ 95% coverage") and per-layer lists ("14 / 21 / 11 passed") are skipped.
# Pages that legitimately quote other numbers: history, generated results and imported research.
EXCLUDED = ("CHANGELOG.md", "docs/results/", "docs/research/")


def _test_log() -> str:
    """The newest run folder that holds a test-all.log (folders are named by date)."""
    logs = sorted((ROOT / "docs" / "results" / "runs").glob("*/test-all.log"))
    return logs[-1].read_text(encoding="utf-8")


def tests_passed() -> str:
    match = re.search(r"(\d+) passed", _test_log())
    return match.group(1) if match else "?"


def coverage_percent() -> str:
    match = re.search(r"^TOTAL\s+\d+\s+\d+\s+(\d+)%", _test_log(), re.M)
    return match.group(1) if match else "?"


def checks_total() -> str:
    counts = re.findall(r"retail/\w+: passed \((\d+) checks", _test_log())
    return str(sum(int(c) for c in counts[:3]))


def accuracy() -> str:
    with (BENCH / "accuracy.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return f"{sum(r['verdict_ok'] == 'True' for r in rows)}/{len(rows)}"


def e2e_steps(mode: str) -> Callable[[], str]:
    def count() -> str:
        steps = len(list((LATEST / f"e2e-{mode}").glob("*.log")))
        return f"{steps}/{steps}"

    return count


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    actual: Callable[[], str]


RULES = (
    Rule("test count", re.compile(r"(?<!/ )\b(\d{2,3}) (?:tests|passed)\b"), tests_passed),
    Rule("coverage", re.compile(r"(?<![≥>] )\b(\d{2,3})% (?:line )?coverage"), coverage_percent),
    Rule("checks in total", re.compile(r"\b(\d+) checks in total"), checks_total),
    Rule("accuracy verdicts", re.compile(r"\b(\d+/\d+) verdicts"), accuracy),
    Rule("e2e-live steps", re.compile(r"`e2e-live` (\d+/\d+)"), e2e_steps("live")),
    Rule("e2e-full steps", re.compile(r"`e2e-full` (\d+/\d+)"), e2e_steps("full")),
)


def pages(root: Path = ROOT) -> list[Path]:
    found = [root / "README.md", root / "CONTRIBUTING.md", *sorted((root / "docs").rglob("*.md"))]
    return [p for p in found if p.exists() and not p.relative_to(root).as_posix().startswith(EXCLUDED)]


def problems(root: Path = ROOT) -> list[str]:
    out: list[str] = []
    for rule in RULES:
        actual = rule.actual()
        for page in pages(root):
            for lineno, line in enumerate(page.read_text(encoding="utf-8").splitlines(), 1):
                for match in rule.pattern.finditer(line):
                    if match.group(1) != actual:
                        where = f"{page.relative_to(root).as_posix()}:{lineno}"
                        out.append(f"{where}: {rule.name} says {match.group(1)}, results say {actual}")
    return out


def main() -> int:
    found = problems()
    for line in found:
        print(line)
    print(f"checked {len(RULES)} number rules: {len(found)} mismatches")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
