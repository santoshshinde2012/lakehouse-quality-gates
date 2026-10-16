"""Survey signals and their public sources (survey_sources.csv); pure loading, no plotting."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

SOURCES = Path(__file__).resolve().parent / "survey_sources.csv"
REQUIRED = ("label", "source", "value_pct", "previous_pct", "publisher", "url")


@dataclass(frozen=True)
class SurveySignal:
    label: str
    source: str
    value_pct: int
    previous_pct: int | None
    publisher: str
    url: str


def _percent(text: str) -> int:
    value = int(text)
    if not 0 <= value <= 100:
        raise ValueError(f"percentage out of range: {value}")
    return value


def parse_signals(rows: list[dict[str, str]]) -> list[SurveySignal]:
    """Validated signals: every row has a public https URL and percentages in 0..100."""
    signals = []
    for row in rows:
        missing = [key for key in REQUIRED if key not in row]
        if missing:
            raise ValueError(f"survey row is missing {missing}")
        if not row["url"].startswith("https://"):
            raise ValueError(f"survey source needs a public https URL: {row['label']}")
        previous = row["previous_pct"].strip()
        signals.append(
            SurveySignal(
                label=row["label"],
                source=row["source"],
                value_pct=_percent(row["value_pct"]),
                previous_pct=_percent(previous) if previous else None,
                publisher=row["publisher"],
                url=row["url"],
            )
        )
    return signals


def load_signals(path: Path = SOURCES) -> list[SurveySignal]:
    with path.open(encoding="utf-8", newline="") as fh:
        return parse_signals(list(csv.DictReader(fh)))
