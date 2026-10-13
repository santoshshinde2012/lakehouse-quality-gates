"""Slack incoming-webhook notifier (stdlib HTTP; the transport is injectable for tests)."""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from typing import Any

from soda_lakehouse_dq.domain.errors import NotificationError
from soda_lakehouse_dq.domain.models import GateReport, GateStatus, Outcome

Transport = Callable[[str, bytes], int]
EMOJI = {
    GateStatus.PASSED: ":white_check_mark:",
    GateStatus.PASSED_WITH_WARNINGS: ":warning:",
    GateStatus.BLOCKED: ":no_entry:",
}
MAX_REASONS = 10


def urllib_transport(url: str, body: bytes, timeout: float = 10.0) -> int:
    if not url.startswith("https://"):
        raise ValueError("Slack webhook URL must use https")
    request = urllib.request.Request(  # noqa: S310 (scheme checked above)
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return int(response.status)


def build_payload(report: GateReport) -> dict[str, Any]:
    title = (
        f"{EMOJI[report.status]} Quality gate *{report.pipeline}/{report.layer.value}*: "
        f"`{report.status.value}`"
    )
    summary = (
        f"passed {report.total(Outcome.PASSED)} · warned {report.total(Outcome.WARNED)} · "
        f"failed {report.total(Outcome.FAILED)} · run `{report.run_id}` · engine `{report.engine}`"
    )
    blocks: list[dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": summary}]},
    ]
    if report.reasons:
        reasons = "\n".join(f"• {r}" for r in report.reasons[:MAX_REASONS])
        if len(report.reasons) > MAX_REASONS:
            reasons += f"\n…and {len(report.reasons) - MAX_REASONS} more"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": reasons}})
    return {"text": title, "blocks": blocks}


class SlackWebhookNotifier:
    """Posts gate results to Slack. ``only_on_problems`` skips clean passes to reduce noise."""

    def __init__(
        self, webhook_url: str, transport: Transport = urllib_transport, only_on_problems: bool = True
    ) -> None:
        self._url = webhook_url
        self._transport = transport
        self._only_on_problems = only_on_problems

    def notify(self, report: GateReport) -> None:
        if self._only_on_problems and report.status is GateStatus.PASSED:
            return
        status = self._transport(self._url, json.dumps(build_payload(report)).encode("utf-8"))
        if status >= 300:  # noqa: PLR2004
            raise NotificationError(f"Slack webhook returned HTTP {status}")
