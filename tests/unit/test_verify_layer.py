from __future__ import annotations

import pytest

from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.application.verify_layer import (
    RunMedallionUseCase,
    VerifyLayerUseCase,
    raise_if_blocked,
)
from soda_lakehouse_dq.domain.errors import DataQualityError, DataSourceError, GateBlockedError
from soda_lakehouse_dq.domain.models import GateStatus, Layer, Outcome
from soda_lakehouse_dq.domain.ports import ContractRepository, ContractVerifier, Notifier, ResultSink
from tests.conftest import FIXED_NOW, FakeRepository, FakeVerifier, RecordingNotifier, RecordingSink


def _use_case(clock, verifier=None, sinks=(), notifiers=()):
    return VerifyLayerUseCase(
        FakeRepository(), verifier or FakeVerifier(), GatePolicy(), clock, sinks=sinks, notifiers=notifiers
    )


def test_fakes_satisfy_ports():
    assert isinstance(FakeRepository(), ContractRepository)
    assert isinstance(FakeVerifier(), ContractVerifier)
    assert isinstance(RecordingSink(), ResultSink)
    assert isinstance(RecordingNotifier(), Notifier)


def test_execute_builds_report_and_fans_out(clock):
    sink, notifier, verifier = RecordingSink(), RecordingNotifier(), FakeVerifier()
    report = _use_case(clock, verifier, [sink], [notifier]).execute("retail", Layer.SILVER, {"K": "v"})
    assert report.status is GateStatus.PASSED
    assert report.run_id == "run-1"
    assert report.engine == "fake"
    assert report.created_at == FIXED_NOW
    assert sink.reports == [report]
    assert notifier.reports == [report]
    assert verifier.calls == [(["retail.silver.orders"], {"K": "v"})]


def test_broken_notifier_does_not_hide_result(clock):
    good = RecordingNotifier()
    report = _use_case(clock, notifiers=[RecordingNotifier(explode=True), good]).execute("retail", Layer.GOLD)
    assert good.reports == [report]


@pytest.mark.parametrize("error", [OSError("report disk is full"), DataSourceError("catalog unavailable")])
def test_failed_report_persistence_prevents_a_successful_return_or_notification(clock, error):
    class FailingSink:
        def write(self, report):
            assert report.status is GateStatus.PASSED
            raise error

    later_sink, notifier = RecordingSink(), RecordingNotifier()
    use_case = _use_case(clock, sinks=[FailingSink(), later_sink], notifiers=[notifier])
    with pytest.raises(DataQualityError) as info:
        use_case.execute("retail", Layer.SILVER, run_id="audit-run")
    if isinstance(error, DataQualityError):
        assert info.value is error
    else:
        assert info.value.__cause__ is error
        assert "audit-run" in str(info.value)
        assert "gate status passed" in str(info.value)
        assert "FailingSink" in str(info.value)
    assert later_sink.reports == []
    assert notifier.reports == []


def test_explicit_run_id_is_kept(clock):
    assert _use_case(clock).execute("retail", Layer.BRONZE, run_id="abc").run_id == "abc"


def test_medallion_stops_at_first_blocked_layer(clock):
    verifier = FakeVerifier({Layer.SILVER: (Outcome.FAILED,)})
    reports = RunMedallionUseCase(_use_case(clock, verifier), clock).execute("retail")
    assert [r.layer for r in reports] == [Layer.BRONZE, Layer.SILVER]
    assert reports[-1].blocked
    assert {r.run_id for r in reports} == {"run-1"}


def test_medallion_runs_all_layers_when_clean(clock):
    reports = RunMedallionUseCase(_use_case(clock), clock).execute("retail")
    assert [r.layer for r in reports] == list(Layer.ordered())


def test_raise_if_blocked(clock):
    clean = _use_case(clock).execute("retail", Layer.GOLD)
    assert raise_if_blocked(clean) is clean
    blocked = _use_case(clock, FakeVerifier({Layer.GOLD: (Outcome.FAILED,)})).execute("retail", Layer.GOLD)
    with pytest.raises(GateBlockedError, match="retail/gold") as info:
        raise_if_blocked(blocked)
    assert info.value.report is blocked
