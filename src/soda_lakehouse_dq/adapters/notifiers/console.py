"""Console notifier: structured log lines (default)."""

from __future__ import annotations

import structlog

from soda_lakehouse_dq.domain.models import GateReport, Outcome


class ConsoleNotifier:
    def __init__(self, logger: structlog.typing.FilteringBoundLogger | None = None) -> None:
        self._log = logger or structlog.get_logger("soda_dq.gate")

    def notify(self, report: GateReport) -> None:
        fields = {
            "pipeline": report.pipeline,
            "layer": report.layer.value,
            "run_id": report.run_id,
            "status": report.status.value,
            "passed": report.total(Outcome.PASSED),
            "warned": report.total(Outcome.WARNED),
            "failed": report.total(Outcome.FAILED),
        }
        if report.blocked:
            self._log.error("dq.gate.blocked", reasons=list(report.reasons), **fields)
        elif report.reasons:
            self._log.warning("dq.gate.passed_with_warnings", reasons=list(report.reasons), **fields)
        else:
            self._log.info("dq.gate.passed", **fields)
