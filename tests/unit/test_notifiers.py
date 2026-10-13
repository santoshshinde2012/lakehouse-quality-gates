from __future__ import annotations

import json

import pytest
import structlog
from structlog.testing import capture_logs

from soda_lakehouse_dq.adapters.notifiers.console import ConsoleNotifier
from soda_lakehouse_dq.adapters.notifiers.slack import SlackWebhookNotifier, build_payload, urllib_transport
from soda_lakehouse_dq.domain.errors import NotificationError
from soda_lakehouse_dq.domain.models import GateStatus, Outcome
from tests.conftest import make_report, make_result


@pytest.mark.parametrize(
    ("status", "reasons", "event", "level"),
    [
        (GateStatus.PASSED, (), "dq.gate.passed", "info"),
        (GateStatus.PASSED_WITH_WARNINGS, ("w",), "dq.gate.passed_with_warnings", "warning"),
        (GateStatus.BLOCKED, ("f",), "dq.gate.blocked", "error"),
    ],
)
def test_console_notifier_levels(status, reasons, event, level):
    with capture_logs() as logs:
        ConsoleNotifier(structlog.get_logger()).notify(make_report(status, reasons=reasons))
    assert logs[0]["event"] == event
    assert logs[0]["log_level"] == level


class Transport:
    def __init__(self, status=200):
        self.status = status
        self.calls = []

    def __call__(self, url, body):
        self.calls.append((url, json.loads(body)))
        return self.status


def test_slack_skips_clean_pass_by_default():
    transport = Transport()
    SlackWebhookNotifier("https://hooks.slack.test/x", transport).notify(make_report())
    assert transport.calls == []


def test_slack_posts_blocked_report():
    transport = Transport()
    report = make_report(
        GateStatus.BLOCKED, make_result(Outcome.FAILED), reasons=tuple(f"r{i}" for i in range(12))
    )
    SlackWebhookNotifier("https://hooks.slack.test/x", transport).notify(report)
    url, payload = transport.calls[0]
    assert url == "https://hooks.slack.test/x"
    assert ":no_entry:" in payload["text"]
    assert "…and 2 more" in payload["blocks"][2]["text"]["text"]


def test_slack_can_post_every_run_and_raises_on_http_error():
    with pytest.raises(NotificationError, match="HTTP 500"):
        SlackWebhookNotifier("https://h", Transport(500), only_on_problems=False).notify(make_report())


def test_payload_without_reasons_has_two_blocks():
    assert len(build_payload(make_report())["blocks"]) == 2


def test_transport_rejects_plain_http():
    with pytest.raises(ValueError, match="https"):
        urllib_transport("http://example.com", b"{}")
