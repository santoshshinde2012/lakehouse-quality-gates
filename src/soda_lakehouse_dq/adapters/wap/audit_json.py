"""Durable snapshot manifests shared by CLI verification, retries and Airflow publication."""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from soda_lakehouse_dq.domain.errors import DataQualityError, DataSourceError
from soda_lakehouse_dq.domain.models import GateReport, GateStatus, Layer
from soda_lakehouse_dq.domain.wap import BranchAudit, validate_branch

_TABLE = re.compile(r"^[a-z0-9_]+\.[a-z0-9_]+$")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate manifest field {key!r}")
        result[key] = value
    return result


def audit_to_dict(audit: BranchAudit) -> dict[str, Any]:
    report = audit.report
    return {
        "version": 1,
        "branch": audit.branch,
        "pipeline": report.pipeline,
        "layer": report.layer.value,
        "run_id": report.run_id,
        "gate": report.status.value,
        "engine": report.engine,
        "created_at": report.created_at.isoformat(),
        "reasons": list(report.reasons),
        "pins": dict(audit.pins),
        "main_before": dict(audit.main_before),
    }


def write_audit(path: Path, audit: BranchAudit) -> None:
    """Replace atomically, so a publisher cannot read a partially written manifest."""
    temporary: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as fh:
            temporary = Path(fh.name)
            # The shared reports bind is written by the DQ user and read by Spark's different uid.
            os.fchmod(fh.fileno(), 0o644)
            json.dump(audit_to_dict(audit), fh, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        temporary.replace(path)
    except OSError as exc:
        raise DataSourceError(f"cannot persist WAP audit at {path}: {exc}") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def load_audit(path: Path) -> BranchAudit:
    try:
        data = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
        if not isinstance(data, dict) or type(data.get("version")) is not int or data["version"] != 1:
            raise ValueError("expected a version 1 audit manifest")
        branch = validate_branch(data["branch"])
        layer = Layer(data["layer"])
        status = GateStatus(data["gate"])
        pins = data["pins"]
        if not isinstance(pins, dict) or not pins:
            raise ValueError("snapshot pins are required")
        for table, pin in pins.items():
            if not isinstance(table, str) or not _TABLE.fullmatch(table):
                raise ValueError(f"invalid table {table!r}")
            if type(pin) is not int or pin <= 0:
                raise ValueError(f"invalid snapshot pin for {table}")
        before = data.get("main_before", {})
        if not isinstance(before, dict) or any(
            table not in pins or (pin is not None and (type(pin) is not int or pin <= 0))
            for table, pin in before.items()
        ):
            raise ValueError("invalid main_before snapshots")
        if any(
            not isinstance(data[key], str) or not data[key].strip()
            for key in ("pipeline", "run_id", "engine")
        ):
            raise ValueError("pipeline, run_id and engine are required")
        reasons = data.get("reasons", [])
        if not isinstance(reasons, list) or any(not isinstance(reason, str) for reason in reasons):
            raise ValueError("reasons must be a list of strings")
        created_at = datetime.fromisoformat(data["created_at"])
        if created_at.tzinfo is None:
            raise ValueError("created_at must include a timezone")
        report = GateReport(
            data["run_id"],
            data["pipeline"],
            layer,
            data["engine"],
            status,
            (),
            tuple(reasons),
            created_at,
        )
        return BranchAudit(branch, report, pins, before)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise DataQualityError(f"invalid WAP audit manifest {path}: {exc}") from exc


class JsonBranchAuditSink:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def write(self, audit: BranchAudit) -> None:
        folder = self._directory / "wap" / validate_branch(audit.branch)
        write_audit(folder / f"{audit.report.layer.value}.audit.json", audit)
