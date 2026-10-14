"""``BranchGate`` built from the normal gate: a fresh ``VerifyLayerUseCase`` whose data source reads pinned
snapshots, so the same contracts, policy, sinks and notifiers apply to an audit branch."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from soda_lakehouse_dq.application.verify_layer import VerifyLayerUseCase
from soda_lakehouse_dq.domain.models import GateReport, Layer

UseCaseFactory = Callable[[Mapping[str, int]], VerifyLayerUseCase]


class PinnedSnapshotGate:
    def __init__(self, factory: UseCaseFactory) -> None:
        self._factory = factory

    def verify(self, pipeline: str, layer: Layer, pins: Mapping[str, int], run_id: str) -> GateReport:
        return self._factory(dict(pins)).execute(pipeline, layer, run_id=run_id)
