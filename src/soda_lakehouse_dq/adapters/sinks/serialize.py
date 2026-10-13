"""Shared, pure conversion of a GateReport to plain data (one place, reused by every sink)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from soda_lakehouse_dq.domain.models import GateReport, Outcome


def report_to_dict(report: GateReport) -> dict[str, Any]:
    return {
        "run_id": report.run_id,
        "pipeline": report.pipeline,
        "layer": report.layer.value,
        "engine": report.engine,
        "status": report.status.value,
        "created_at": report.created_at.isoformat(),
        "summary": {o.value: report.total(o) for o in Outcome} | {"total": report.total_checks},
        "reasons": list(report.reasons),
        "contracts": [
            {
                "name": r.contract.name,
                "dataset": r.contract.dataset,
                "source": str(r.contract.source_path),
                "errors": list(r.errors),
                "checks": [
                    {
                        "name": c.name,
                        "type": c.check_type,
                        "column": c.column,
                        "outcome": c.outcome.value,
                        "metrics": c.metrics,
                        "details": list(c.details),
                    }
                    for c in r.checks
                ],
            }
            for r in report.results
        ],
    }


def report_path(directory: Path, report: GateReport, suffix: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{report.run_id}_{report.pipeline}_{report.layer.value}.{suffix}"
