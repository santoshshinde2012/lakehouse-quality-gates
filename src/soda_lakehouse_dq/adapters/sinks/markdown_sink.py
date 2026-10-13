"""Markdown sink: a human-readable summary (also suitable for $GITHUB_STEP_SUMMARY)."""

from __future__ import annotations

from pathlib import Path

from soda_lakehouse_dq.adapters.sinks.serialize import report_path
from soda_lakehouse_dq.domain.models import GateReport, Outcome

ICONS = {
    Outcome.PASSED: "✅",
    Outcome.WARNED: "⚠️",
    Outcome.FAILED: "❌",
    Outcome.NOT_EVALUATED: "⏸️",
    Outcome.EXCLUDED: "⏭️",
}


def render_markdown(report: GateReport) -> str:
    lines = [
        f"# Quality gate `{report.pipeline}/{report.layer.value}`: **{report.status.value}**",
        "",
        f"- run: `{report.run_id}`  engine: `{report.engine}`  at: {report.created_at.isoformat()}",
        f"- checks: {report.total_checks} (passed {report.total(Outcome.PASSED)}, warned "
        f"{report.total(Outcome.WARNED)}, failed {report.total(Outcome.FAILED)})",
        "",
    ]
    for result in report.results:
        lines += [f"## {result.contract.name} (`{result.contract.dataset}`)", ""]
        lines += ["| | check | column | type | metrics |", "|---|---|---|---|---|"]
        for c in result.checks:
            metrics = "; ".join([", ".join(f"{k}={v}" for k, v in c.metrics.items()), *c.details])
            lines.append(f"| {ICONS[c.outcome]} | {c.name} | {c.column or ''} | {c.check_type} | {metrics} |")
        lines += [f"- error: {e}" for e in result.errors]
        lines.append("")
    if report.reasons:
        lines += ["## Reasons", "", *[f"- {r}" for r in report.reasons], ""]
    return "\n".join(lines)


class MarkdownSink:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def write(self, report: GateReport) -> None:
        path: Path = report_path(self._directory, report, "md")
        path.write_text(render_markdown(report), encoding="utf-8")
