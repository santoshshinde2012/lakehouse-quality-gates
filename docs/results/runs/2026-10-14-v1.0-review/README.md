# Enterprise source review and verification

This run validates the follow-up source changes on `codex/enterprise-verification`, based on
`9cd3ed37870063ed1d4988ce53cd12232c8840f8` plus the reviewed working tree. The final local suite passes **359 tests with 9 live/full cases deselected and 97% line coverage**.
The workflows pass **live 9/9, full 15/15, WAP 11/11**. The source fingerprint identifies the final delivered tree and checksums identify the attachments; the historical evidence folders remain unchanged.

## What was corrected

- WAP now records fixed reference snapshots, including the gold SQL reconciliation dependency. A changing
  declared read set aborts approval; only primary tables can be published.
- Invalid manifests, branch identifiers, and Spark responses fail through structured errors. Spark closes
  its session on failures, and report-write failures cannot return the blocked-data exit code.
- JUnit represents blocked policy decisions. Iceberg history preserves execution-error and empty-gate
  records using the existing schema.
- End-to-end startup failures stop later mutations; selected pytest skips cannot produce a passing run.
  Airflow demo injection requires a real boolean, and its bad WAP run captures before/after `main` state.
- CI keeps coverage and Docker gate reports. Benchmarks disable optional storage/notification side effects,
  reject failed-process measurements, and return failure for accuracy mismatches.

## Attachments

| Evidence | Scope |
|---|---|
| [Local validation](test-all.log), [pytest JUnit](pytest.xml), [line coverage](coverage.xml) | Formatting, lint, strict types, unit/integration tests, contract validation, good/bad fixture gates |
| [Colored CLI check](colored-cli-check.log), [JUnit](colored-cli-pytest.xml) | All 18 WAP CLI tests with forced terminal color, including portable invalid-input error assertions |
| [Light-stack workflow](e2e-live.log) | Real good/bad DuckDB gates and restoration |
| [Full-stack workflow](e2e-full.log) | Spark, DuckDB, Trino, hardened runner, and Airflow good/bad/restore |
| [WAP workflow](e2e-wap.log) | CLI and Airflow publication, blocked candidates, retained approved data |
| [History test](history-check.log), [history JUnit](history-pytest.xml), [storage readback](history-readback.json) | One real Iceberg test: two deliberately synthetic blocked-attempt records, including null contract/dataset fields |
| [Runner controls](runner-security.json), [installed source hashes](runner-source-fingerprint.json), [versions](runner-versions.json) | Actual container hardening and byte-for-byte agreement of all 39 installed package files with host source |
| [First-use bootstrap](bootstrap-check.log), [probe source](bootstrap_probe.py) | Isolated empty-table creation produces a real main snapshot and audit branch with zero visible rows; the temporary namespace is removed |
| [Documentation checks](docs-check.log), [diagram rendering](diagrams-check.log), [pre-commit](pre-commit.log) | Links, Markdown, source synchronization, numerical claims, Mermaid parsing, hooks |

Each end-to-end mode has its own directory containing that invocation's numbered step logs and JUnit XML.
The WAP directory also contains CLI good/bad evidence and the Airflow before/after snapshots.
[Pytest WAP evidence](pytest-wap-evidence/) preserves its original temporary snapshot JSON files. Future
end-to-end runs keep pytest temporary output directly under their own log directories; the temporary-output retention
change has [40 focused regressions](e2e-attachment-check.log) and [JUnit evidence](e2e-attachment-pytest.xml). [Gate reports](gates/),
[saved audit manifests](audit-manifests/), and [verification metadata](verification.json) identify this run. Raw gate reports and manifests preserve their original content.

The bad-run trigger excerpts in `e2e-full/airflow-dq-bad-trigger.txt` and
`e2e-wap/airflow-wap-bad-trigger.txt` are extracted from the corresponding numbered logs, which remain
unchanged. [Task-state validation](airflow-task-check.log) checks that the writer succeeded, the Bronze
quality gate failed, and every downstream task was blocked. The final assertion changes were checked
against these real outputs and regression tests; the Docker workflows were not repeated for this assertion-only change.

## Interpretation and reproduction

Commands are `make test-all`, `AIRFLOW_API_PORT=8090 make e2e-live e2e-full e2e-wap`, and
`uv run pytest tests/live/test_result_history.py -m live` against a running catalog. The local validation
also requests JUnit and coverage XML through `PYTEST_ADDOPTS`; the exact command list is in the verification
metadata. WAP kept the test stack running temporarily for evidence capture; [cleanup](cleanup.log) restores the initial
container state and retains volumes.

The initial full workflow selected the eight existing live/full tests. The new Iceberg history case is
recorded separately, so all current live/full cases have evidence without treating its first skipped
attempt as a pass. The `attempts/` directory preserves a resolved Mermaid rendering error and the history
attempt made after automatic stack teardown, plus a hook run whose captured output changed its own staged log; these are excluded from success counts. The first Linux CI run also exposed two
error-message assertions that depended on terminal coloring; its log and JUnit are preserved in
`attempts/ci-linux-01.log` and `attempts/ci-linux-pytest-01.xml`. The CLI correctly rejected both inputs.
The assertions now ignore ANSI styling, and the local suite was rerun after that portability fix.

No performance workloads were rerun. Archived 46-check timing results remain scoped to their earlier
contracts/publisher; this review verifies the current 53-check implementation. Concurrent-writer, partial
publication, malformed-input, and recovery cases use focused unit regressions; the live workflow demonstrates
actual good/bad publication. Read the [enterprise operating model](../../../guides/enterprise-readiness.md)
for ownership, evidence protection, publication boundaries, and deployment access requirements.

Verify attachment integrity from this directory with `shasum -a 256 -c SHA256SUMS`. Checksums detect
changed or missing files; they do not provide an authenticated signature.
