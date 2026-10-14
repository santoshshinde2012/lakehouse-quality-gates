# Observability

> [!NOTE]
> **At a glance.** Every gate run writes JSON, JUnit and Markdown reports and structured logs; optionally one row per
> check or execution failure in the `ops.dq_results` Iceberg table; Slack is told about warnings and blocks.

| Signal | Where | Use |
|---|---|---|
| Structured logs | structlog (`DQ_LOG_JSON=true` for JSON), events `dq.verify.start`, `dq.contract.verify`, `dq.verify.done`, `dq.gate.passed`, `dq.gate.passed_with_warnings`, `dq.gate.blocked`, `dq.medallion.blocked`, `dq.notify.failed` | Debugging, log shipping |
| Gate reports | `reports/*.json`, `*.xml`, `*.md` | Audit trail per run; JUnit renders in CI |
| Iceberg history | `ops.dq_results` (check and execution records with run id, created at, pipeline, layer, engine, gate status, contract, dataset, check name and type, column, outcome) | Trend queries in Trino or DuckDB, e.g. failure rate per check per week |
| Airflow | Task state, logs, durations | Gate latency per run, alert on task failure |
| Alerts | Slack webhook (on warn or block) | Human triage (runbook) |
| CI | Job summary with the Markdown reports, uploaded report artefacts | Contract changes are reviewed with their effect |

Suggested metrics when shipped to a metrics backend: `dq_gate_duration_seconds{layer,engine}`,
`dq_gate_status_total{layer,status}`, `dq_check_failed_total{contract,check}`, derived from the JSON reports
or `ops.dq_results`.

The `ops.dq_results` columns are shown in the [data model](data-flow.md#data-model).

`check_type=execution_error` records contract execution failures even when no checks could run.
An empty blocked gate emits `check_type=gate_decision` with no contract or dataset. These use the existing
history schema; raw error details remain in the JSON report for the same run ID. Exclude operational record
types when calculating a count or failure rate for quality checks.

---

[← Security](security.md) · [Architecture index](README.md) · [Write-audit-publish →](write-audit-publish.md)
