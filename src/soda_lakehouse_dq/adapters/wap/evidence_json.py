"""WAP run evidence: per layer outcome, gate status and snapshot ids, written atomically as JSON."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

from soda_lakehouse_dq.domain.errors import DataSourceError
from soda_lakehouse_dq.domain.wap import BranchState, WapLayerResult

STATE_FIELDS = ("main_before", "main_snapshot", "main_rows", "branch_snapshot", "branch_rows", "refs")


def state_json(state: BranchState | None) -> object:
    if state is None:
        return None
    return {
        "action": state.action,
        "seconds": state.seconds,
        "tables": {
            name: {k: (dict(v) if isinstance(v := getattr(s, k), Mapping) else v) for k in STATE_FIELDS}
            for name, s in state.tables.items()
        },
    }  # fmt: skip


def wap_evidence(results: Sequence[WapLayerResult]) -> list[dict[str, object]]:
    """Per layer: outcome, gate status and the snapshot ids/row counts before and after."""
    return [
        {
            "layer": r.layer.value,
            "branch": r.branch,
            "outcome": r.outcome.value,
            "gate": r.report.status.value,
            "run_id": r.report.run_id,
            "reasons": list(r.report.reasons[:10]),
            "written": state_json(r.written),
            "published": state_json(r.published),
            "audited_pins": dict(r.audited_pins),
        }
        for r in results
    ]


class JsonWapEvidenceWriter:
    """Writes WAP evidence to one JSON file; implements the ``WapEvidenceWriter`` port."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def check(self) -> None:
        """Reject unusable output paths before a WAP run can write or publish data."""
        path = self.path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists() and not path.is_file():
                raise DataSourceError(
                    f"cannot write WAP evidence at {path}: destination is not a regular file"
                )
            with tempfile.TemporaryFile(dir=path.parent):
                pass
        except OSError as exc:
            raise DataSourceError(f"cannot write WAP evidence at {path}: {exc}") from exc

    def write(self, results: Sequence[WapLayerResult]) -> None:
        """Keep previous evidence intact if the final write or replacement fails."""
        path = self.path
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as fh:
                temporary = Path(fh.name)
                os.fchmod(fh.fileno(), 0o644)
                json.dump(wap_evidence(results), fh, indent=2)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            temporary.replace(path)
        except OSError as exc:
            raise DataSourceError(f"cannot persist WAP evidence at {path}: {exc}") from exc
        finally:
            if temporary is not None:
                with contextlib.suppress(OSError):
                    temporary.unlink(missing_ok=True)
