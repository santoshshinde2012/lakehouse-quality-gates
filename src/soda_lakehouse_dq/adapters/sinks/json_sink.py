"""JSON sink: one machine-readable file per gate run."""

from __future__ import annotations

import json
from pathlib import Path

from soda_lakehouse_dq.adapters.sinks.serialize import report_path, report_to_dict
from soda_lakehouse_dq.domain.models import GateReport


class JsonFileSink:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def write(self, report: GateReport) -> None:
        path = report_path(self._directory, report, "json")
        path.write_text(json.dumps(report_to_dict(report), indent=2, default=str) + "\n", encoding="utf-8")
