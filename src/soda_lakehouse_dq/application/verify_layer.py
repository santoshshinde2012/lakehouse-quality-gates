"""Use cases: verify one layer's contracts, or walk the medallion layer by layer."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import structlog

from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.domain.errors import GateBlockedError
from soda_lakehouse_dq.domain.models import GateReport, Layer, VariableValue
from soda_lakehouse_dq.domain.ports import (
    Clock,
    ContractRepository,
    ContractVerifier,
    Notifier,
    ResultSink,
)

log = structlog.get_logger(__name__)


class VerifyLayerUseCase:
    """Verify every contract of ``pipeline/layer``, decide, persist and notify.

    Collaborators arrive through the constructor; this class only orchestrates.
    """

    def __init__(
        self,
        repository: ContractRepository,
        verifier: ContractVerifier,
        policy: GatePolicy,
        clock: Clock,
        *,
        sinks: Sequence[ResultSink] = (),
        notifiers: Sequence[Notifier] = (),
    ) -> None:
        self._repository = repository
        self._verifier = verifier
        self._policy = policy
        self._clock = clock
        self._sinks = tuple(sinks)
        self._notifiers = tuple(notifiers)

    def execute(
        self,
        pipeline: str,
        layer: Layer,
        variables: Mapping[str, VariableValue] | None = None,
        run_id: str | None = None,
    ) -> GateReport:
        contracts = self._repository.list_contracts(pipeline, layer)
        log.info("dq.verify.start", pipeline=pipeline, layer=layer.value, contracts=len(contracts))
        results = tuple(self._verifier.verify(contracts, dict(variables or {})))
        decision = self._policy.decide(results)
        report = GateReport(
            run_id=run_id or self._clock.new_run_id(),
            pipeline=pipeline,
            layer=layer,
            engine=self._verifier.engine,
            status=decision.status,
            results=results,
            reasons=decision.reasons,
            created_at=self._clock.now(),
        )
        for sink in self._sinks:
            sink.write(report)
        for notifier in self._notifiers:
            try:
                notifier.notify(report)
            except Exception as exc:  # a broken notifier must never hide the gate result
                log.warning("dq.notify.failed", notifier=type(notifier).__name__, error=str(exc))
        log.info("dq.verify.done", pipeline=pipeline, layer=layer.value, status=report.status.value)
        return report


class RunMedallionUseCase:
    """Gate bronze, then silver, then gold; stop at the first blocked layer."""

    def __init__(self, verify_layer: VerifyLayerUseCase, clock: Clock) -> None:
        self._verify_layer = verify_layer
        self._clock = clock

    def execute(
        self,
        pipeline: str,
        layers: Sequence[Layer] = Layer.ordered(),
        variables: Mapping[str, VariableValue] | None = None,
    ) -> list[GateReport]:
        run_id = self._clock.new_run_id()
        reports: list[GateReport] = []
        for layer in layers:
            report = self._verify_layer.execute(pipeline, layer, variables, run_id=run_id)
            reports.append(report)
            if report.blocked:
                log.error("dq.medallion.blocked", layer=layer.value, run_id=run_id)
                break
        return reports


def raise_if_blocked(report: GateReport) -> GateReport:
    """Turn a blocked report into an exception (used by orchestrators to fail the task)."""
    if report.blocked:
        raise GateBlockedError(
            f"quality gate blocked {report.pipeline}/{report.layer.value}: " + "; ".join(report.reasons[:10]),
            report,
        )
    return report
