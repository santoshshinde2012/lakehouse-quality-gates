"""Write-audit-publish use cases (ADR 0006).

``RunWapMedallionUseCase`` walks bronze → silver → gold: write the layer to an audit branch, verify the
branch's snapshots with the layer's contracts, then fast-forward ``main`` only if the gate passed. A blocked
gate leaves ``main`` untouched and keeps the branch (with the bad rows) for forensics.
"""

from __future__ import annotations

from collections.abc import Sequence

import structlog

from soda_lakehouse_dq.domain.errors import DataQualityError
from soda_lakehouse_dq.domain.models import GateReport, Layer
from soda_lakehouse_dq.domain.ports import (
    AuditWriter,
    BranchAuditSink,
    BranchGate,
    BranchPublisher,
    Clock,
    ContractRepository,
    SnapshotReader,
)
from soda_lakehouse_dq.domain.wap import BranchAudit, WapLayerResult, WapOutcome, branch_name, validate_branch

log = structlog.get_logger(__name__)


def dataset_table(dataset: str) -> str:
    """``datasource/schema/table`` -> ``schema.table``."""
    parts = dataset.split("/")
    if len(parts) < 2:  # noqa: PLR2004 (schema/table at least)
        raise DataQualityError(f"cannot read schema/table from dataset {dataset!r}")
    return f"{parts[-2]}.{parts[-1]}"


class VerifyBranchUseCase:
    """Gate one layer against what is on ``branch``: every dataset the layer's contracts read is pinned to the
    branch's snapshot when the branch exists on that table; other tables (references) are read from main."""

    def __init__(
        self,
        repository: ContractRepository,
        snapshots: SnapshotReader,
        gate: BranchGate,
        audit_sink: BranchAuditSink | None = None,
    ) -> None:
        self._repository = repository
        self._snapshots = snapshots
        self._gate = gate
        self._audit_sink = audit_sink

    def pins(self, pipeline: str, layer: Layer, branch: str) -> dict[str, int]:
        contracts = self._repository.list_contracts(pipeline, layer)
        primary = {dataset_table(spec.dataset) for spec in contracts}
        tables = sorted({dataset_table(d) for spec in contracts for d in spec.datasets})
        pins = {t: snap for t in tables if (snap := self._snapshots.snapshot(t, branch)) is not None}
        missing = primary - pins.keys()
        if missing:
            raise DataQualityError(
                f"branch {branch!r} missing on primary tables: {', '.join(sorted(missing))}; nothing to audit"
            )
        if not pins:
            raise DataQualityError(f"branch {branch!r}: nothing to audit")
        return pins

    def execute(self, pipeline: str, layer: Layer, branch: str, run_id: str) -> GateReport:
        return self.audit(pipeline, layer, branch, run_id).report

    def audit(self, pipeline: str, layer: Layer, branch: str, run_id: str) -> BranchAudit:
        branch = validate_branch(branch)
        pins = self.pins(pipeline, layer, branch)
        before = {table: self._snapshots.snapshot(table, "main") for table in pins}
        log.info("dq.wap.verify", layer=layer.value, branch=branch, pins=pins)
        audit = BranchAudit(branch, self._gate.verify(pipeline, layer, pins, run_id), pins, before)
        if self._audit_sink is not None:
            self._audit_sink.write(audit)
        return audit


class RunWapMedallionUseCase:
    """Write → audit → publish, layer by layer; stop at the first blocked gate."""

    def __init__(
        self,
        writer: AuditWriter,
        publisher: BranchPublisher,
        verify_branch: VerifyBranchUseCase,
        clock: Clock,
        *,
        drop_blocked: bool = False,
    ) -> None:
        self._writer = writer
        self._publisher = publisher
        self._verify = verify_branch
        self._clock = clock
        self._drop_blocked = drop_blocked

    def execute(
        self,
        pipeline: str,
        layers: Sequence[Layer] = Layer.ordered(),
        *,
        source: str | None = None,
        branch: str | None = None,
    ) -> list[WapLayerResult]:
        run_id = self._clock.new_run_id()
        branch = validate_branch(branch) if branch else branch_name(run_id)
        results: list[WapLayerResult] = []
        for layer in layers:
            written = self._writer.write(layer, branch, source if layer is Layer.BRONZE else None)
            audit = self._verify.audit(pipeline, layer, branch, run_id)
            report = audit.report
            if report.blocked:
                log.error("dq.wap.blocked", layer=layer.value, branch=branch, kept=not self._drop_blocked)
                if self._drop_blocked:
                    self._publisher.drop(layer, branch)
                results.append(
                    WapLayerResult(
                        layer,
                        branch,
                        WapOutcome.BLOCKED,
                        report,
                        written,
                        audited_pins=audit.pins,
                    )
                )
                break
            published = self._publisher.publish(layer, branch, audit.pins, main_before=audit.main_before)
            log.info("dq.wap.published", layer=layer.value, branch=branch)
            results.append(
                WapLayerResult(
                    layer,
                    branch,
                    WapOutcome.PUBLISHED,
                    report,
                    written,
                    published,
                    audit.pins,
                )
            )
        return results
