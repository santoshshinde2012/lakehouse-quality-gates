# Changelog

All notable changes to this project. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Fixed

- Empty contracts block the gate; silver requires an order status, and gold requires revenue and valid,
  non-negative counters. The current retail contracts contain 53 checks (14 bronze, 22 silver, 17 gold).
- Rendered contracts use immutable content-based paths and atomic writes so concurrent engines cannot
  overwrite each other's inputs.
- Write-audit-publish requires every primary table's branch, saves the audited snapshot IDs, publishes
  those exact snapshots, and supports resuming a partially published layer from its audit manifest.
- Relative lakehouse checkout paths resolve correctly through Make. Bad-batch demos require the blocked
  exit code, and end-to-end cleanup preserves containers that were already running. Each invocation writes
  to a separate log directory, preserving prior evidence and preventing stale step counts.
- Trino providers keep configuration local to each verification; missing DuckDB tables and invalid CLI
  settings return configuration errors. Iceberg report schemas remain stable for dataset-only checks.
- Regression tests cover empty contracts, missing values, WAP recovery, rendering isolation, cleanup,
  and failures that previously appeared to pass verification.

## [1.0.0] - 2026-10-14

First complete release: every layer can now be published through write-audit-publish, so a bad batch never reaches
`main` (limitation L1 resolved).

### Added

- **Write-audit-publish** ([ADR 0006](docs/adr/0006-write-audit-publish.md)). Each layer is written to an Iceberg
  branch `audit_<run>`, gated there with the unchanged contracts, and fast-forwarded to `main` only when the gate
  passes. A blocked gate leaves `main` untouched and keeps the branch with the bad rows.
  - `wap/wap_job.py`: our own Spark job (write, publish, drop, status, and a `direct` baseline for benchmarks),
    mounted into `ldl-spark` by `docker/docker-compose.wap.yml`; it reuses the lakehouse SQL files read-only.
  - Ports `AuditWriter`, `BranchPublisher`, `SnapshotReader`, `BranchGate`; adapters `SparkSubmitWap`,
    `PyIcebergSnapshotReader`, `PinnedSnapshotGate`; `DuckDBProvider(pins=...)` reads branch snapshots through
    `dq_audit` views with `AT (VERSION => id)`; use cases `VerifyBranchUseCase` and `RunWapMedallionUseCase`.
  - CLI `soda-dq wap run --layer/--all [--batch good|bad] [--evidence FILE]`, `wap verify`, `wap status`,
    `wap drop`; settings `DQ_WAP_*`.
  - Airflow DAG `lakehouse_retail_medallion_wap` (`wap_write_<layer> >> dq_<layer> >> publish_<layer>`); the
    existing DAGs are unchanged.
  - Make targets `wap-up`, `wap-demo-good`, `wap-demo-bad`, `airflow-wap-trigger`, `airflow-wap-trigger-bad`,
    `e2e-wap` (`scripts/e2e.sh wap`) and `bench-wap`.
  - Tests: unit tests with fakes for every new port, DAG and CLI tests, and the live test `tests/live/test_wap.py`
    that checks snapshot ids and row counts.
- Evidence in `docs/results/runs/2026-10-14-wap/` and a WAP overhead benchmark at 100k and 1M orders, charted in
  `docs/benchmarks/results.md`.
- A fresh-clone regression of every tier, including `e2e-wap`, in `docs/results/runs/2026-10-14-v1.0-fresh-clone/`.

### Changed

- The write-audit-publish page, system design (section 13, L1, L6, L7) and ADR 0004 now describe the
  implementation; README and running-and-testing gain the WAP steps; `check_numbers.py` reads the newest e2e run
  and checks the `e2e-wap` step count.
- The lakehouse compose overlay used by `make airflow-dq-up` includes the WAP overlay.

## [0.5.0] - 2026-10-14

### Added

- **Own end-to-end benchmarks with Mermaid charts.** `docs/benchmarks/results.md` now holds 10 GitHub-rendered Mermaid charts (`xychart-beta` line and bar, `pie`), each with a takeaway, a data table and a link to its raw file: gate latency vs batch size (10k to 5M orders, host DuckDB, Trino, `ldl-soda` container), per-layer latency at 5M, memory and query count, seed-to-gold pipeline, overhead against Spark, Airflow DAG per-task durations for good and bad batches, detection accuracy and the five-tool comparison.
- `scripts/bench_charts.py` generates the charts and the benchmark headline table from the newest data folder; `make docs-check` fails if they are stale and `make diagrams-check` parses every chart with mermaid-cli.
- `benchmarks/airflow_runs.py` (`make bench-airflow`) times good and bad DAG runs and every task.
- `make trino-heap` / `make trino-heap-reset`: a compose override in `benchmarks/trino/` that gives Trino a 4 GiB heap without editing the lakehouse; Trino now passes at 5M orders.
- `docs/benchmarks/analysis.md` rewritten: findings, scaling, bottlenecks, recommendations, threats to validity.
- Raw data in `docs/results/benchmarks/2026-10-14-v0.4.1/` with machine specs, versions and commits.

### Changed

- README "Results at a glance" adds the headline latency chart and the new numbers; `check_numbers.py` reads the newest benchmark folder and checks the DuckDB latency range and the 5M three-gate time.
- The v0.3.0 benchmark set is marked superseded in the results index.

## [0.4.1] - 2026-10-14

### Changed

- **README rewritten** to be short and scannable: tagline, six badges, what it does, the architecture diagram, numbered quick start steps with one command and one expected result each, live and full-stack steps, a testing table, results at a glance and a documentation table. Long feature lists, the roadmap and duplicate explanations moved to the docs.
- **One canonical setup flow:** `docs/guides/quickstart.md` holds the numbered steps as regions that the README copies; running-and-testing and the runbook link to it instead of repeating setup. The runbook's dated walkthrough became a manual walkthrough of every component.
- Documentation hub: a five-step reading path; descriptions updated for v0.4.0 targets and tiers. Stale commands fixed in the contracts guide and troubleshooting.
- **Results reorganised:** `docs/results/README.md` indexes every run newest first with date, commit, machine and the make target to reproduce it. The v0.2.1 component verification moved to `runs/2026-10-13-v0.2-verification/`; early Airflow debugging runs moved to `archive/` with a note; the 1.6 MB coverage HTML was dropped (XML and the test log summary remain).

### Added

- `scripts/check_numbers.py`, run by `make docs-check` and the unit tests: the test count, coverage, total checks, accuracy verdicts and e2e step counts quoted in the README and docs must match the result files.

## [0.4.0] - 2026-10-13

### Added

- **Running and testing guide** (`docs/guides/running-and-testing.md`): prerequisites with versions and Docker resources (including Trino heap advice for 5M orders), setup, the four test tiers, every make target with pass criteria, benchmarks, regenerating docs and results, and troubleshooting.
- `make test-all` (lint, mypy, T0 + T1, validate, demo), `make e2e-live` and `make e2e-full` via `scripts/e2e.sh`: pass/fail summary table, non-zero exit on failure, idempotent, per-step logs in `reports/e2e-<mode>/`, trap teardown of only what it started (`down`, never `-v`), `KEEP_UP=1`. Also `make test-live`, `make test-full`, `make bench-tiny`.
- pytest markers `unit`, `integration`, `live`, `full`; `tests/live/` with live gate tests (good passes, bad blocks every layer) and full-stack tests (Trino gates, runner container and its hardening, Airflow health and DAG). They skip with the reason when the stack is down; CI still runs unit + integration.
- A unit test that every setting is listed in `.env.example`.
- Fresh-clone verification run in `docs/results/runs/2026-10-13-fresh-clone/`.

### Changed

- `.env.example` lists every `DQ_*` setting (previously 10 were missing) and `AIRFLOW_API_PORT`.
- `make help` lists every target, grouped.
- Benchmarks: `BENCH_OUT` / `DQ_BENCH_RESULTS` choose the output folder, `WARM`, `TOOL_SIZES` and `REPEATS` are settable; `benchmarks/tools/requirements.txt` is pinned; `run_tools.py` overwrites its CSV and records `tools_versions.json`.
- The tiny tool comparison harness moved to `benchmarks/tools/tiny/` and is runnable (`make bench-tiny`; no `.venv` of its own, portable timing, relative data paths). Results stay in `docs/research/benchmark-tiny/results/`.
- System design testing tiers now match the markers: T0 static + unit, T1 integration, T2 live light, T3 full.

## [0.3.2] - 2026-10-13

### Changed

- **One home per fact.** `docs/system-design.md` is now the end-to-end narrative (problem, requirements, tool choice, decisions, risks, roadmap) and links to the focused pages for detail; it shrank from 83 KB to 34 KB. Duplicated sections (goals, SLO table, check list, exit codes) now live on one page and are linked from the others.
- Removed the five "moved" stub pages at the `docs/` root; every link points at the new paths.
- Diagrams: the domain model is split into **domain model** (value objects and enums) and a new **ports and adapters** view, both without crossing lines; the gate policy is a compact left-to-right flow; the data model shows every column the schema checks enforce; the hero shows Slack alerts on warn or block; the deployment view shows Airflow's default port 8080. 13 diagrams in total.
- Proofread every page: glossary terms used consistently (*not evaluated*, engine vs runner); corrected the gold-gate overhead at 5M orders (93 % of its build, not more), the object store (RustFS, not MinIO), the sink count (four), the telemetry wording, the structlog event list and the mypy scope. The docs hub opens with a four-step reading path.

### Added

- `scripts/doc_snippets.py` with tests: text that must appear twice (the README quickstart and headline numbers) is written once as a region and copied by `make docs`; `make docs-check`, pre-commit and CI fail on a stale copy or on prose written twice by hand.

## [0.3.1] - 2026-10-13

### Changed

- Documentation restructured around a hub (docs/README.md: who should read what, reading order) and topic folders: `overview/` (use case, glossary), `architecture/` (overview, components, data flow, deployment, security, observability, write-audit-publish, design principles), `guides/` (quickstart, contracts, configuration, runbook, troubleshooting), `benchmarks/` (methodology, results, analysis), `research/` and an ADR index. Old paths keep short "moved" pages.
- docs/system-design.md stays the end-to-end document, now with *At a glance* callouts, links to the focused pages and the glossary.
- The README is shorter: what and why, the architecture diagram, quickstart, headline results and documentation links.
- Diagrams renamed to descriptive files with a header comment (title, purpose, last updated); every embedded diagram has a one-line summary and a *How to read it* list. New: system context and data model (ER). Domain model corrected to the real fields; gate policy now shows the empty-gate rule; sequences are numbered with notes on key decisions.
- Corrected stale facts: Soda Core v4 licence (ELv2, not "proprietary") in the README, `ops.dq_results` columns, image version references.

### Added

- `scripts/check_links.py` (relative links and anchors) with tests, `make docs-check`, markdownlint-cli2 config and pre-commit hooks, and CI steps for links and markdownlint.

## [0.3.0] - 2026-10-13

### Added

- **System design document** (docs/system-design.md): requirements and SLOs, tool selection, architecture, detailed design, sequences, orchestration, deployment, security, observability, measured benchmarks, FMEA, capacity planning, trade-offs, known limitations (including the `CREATE OR REPLACE` gap), the write-audit-publish roadmap, testing tiers, runbook summary and appendix.
- **Benchmarks** on the live lakehouse (benchmarks/, `make bench`, `bench-accuracy`, `bench-spark`, `bench-tools`, `bench-report`): gate latency (cold and warm p50/p95), CPU, peak RSS, SQL query counts and Trino scan bytes per layer and engine at 10k to 5M orders; detection accuracy for 7 defect types at 0.1/1/10 %; gate time against the Spark jobs; a five-tool comparison. Raw CSV/JSON and summary tables in docs/results/benchmarks/.
- `scripts/gen_scale_data.py`: deterministic retail batches of any size with optional defect injection and ground truth.
- Diagrams 08 (write-audit-publish sequence), 09 (gate policy) and 10 (scaling chart, `xychart-beta`); the shared Mermaid theme now styles charts.
- Quality-gate research (landscape, scoring matrix, tiny cross-tool benchmark) in docs/research/.

### Changed

- ADR 0001 amended: Soda Core v4 is licensed under ELv2 (source-available); the decision stands.
- mypy and ruff now also cover benchmarks/ and scripts/.

### Found

- Trino with the lakehouse's default 1 GiB heap cannot evaluate the bronze and silver order checks at 5M orders (`EXCEEDED_LOCAL_MEMORY_LIMIT`); the gate blocks (fail closed). Documented as limitation L8 with mitigations.

## [0.2.3] - 2026-10-13

### Changed

- The README hero is now a system architecture diagram: Orchestration above Sources, Lakehouse platform, Quality gates and Outputs, with short labels, a restrained palette (light zones, one indigo accent, red only for blocked) and no crossings.
- New code architecture (hexagonal) diagram, collapsible in the README and in docs/architecture.md, with the class names moved into a table below it.
- Medallion flow, deployment and Airflow DAG are redrawn as equal stage zones with single-line labels. Every source shares the new theme; docs/diagrams/THEME.md is updated.

## [0.2.2] - 2026-10-13

### Changed

- Diagrams are now Mermaid only. README.md shows the layered architecture, the medallion flow with gates and the deployment inline; docs/architecture.md has all seven views (also the gate sequence, Airflow DAG, domain and ports, and system context).
- Every diagram uses one shared theme (`base` with explicit fills, dark text and borders, one accent per layer, green pass and red fail edges), so it reads on GitHub light and dark. Documented in docs/diagrams/THEME.md.
- `make diagrams` syncs the Markdown copies from docs/diagrams/*.mmd (scripts/diagrams.py) and renders SVGs with mermaid-cli. `make diagrams-check` fails on stale copies and parses every source. The stale-copy check also runs in pytest, so CI enforces it.

### Removed

- The draw.io sources, PNG exports and docs/diagrams/src/build_drawio.py, the draw.io export in `make diagrams`, and the docs/diagrams pre-commit exclusion.

## [0.2.1] - 2026-10-13

### Fixed

- `soda-dq seed`: a relative SAMPLE_DIR was resolved after changing into the scratch directory, so `soda-dq seed fixtures/bad_batch` failed on the host. The path is now resolved first, and a regression test covers it.

## [0.2.0] - 2026-10-13

### Changed

- The project is now `lakehouse-quality-gates`: repository, distribution, Docker image (`lakehouse-quality-gates:0.2.0`) and compose project. The CLI is still `soda-dq`, and the import package is still `soda_lakehouse_dq`.
- The Makefile reads the Airflow host port from `AIRFLOW_API_PORT` (default 8080, the lakehouse default). Use 8090 when 8080 is taken.

### Added

- An `inject_bad_batch` task in `lakehouse_retail_medallion_dq`. It does nothing unless the run's conf has `{"inject_bad_batch": true}`; then it seeds `fixtures/bad_batch`, so `dq_bronze` must block. New make targets: `airflow-dq-trigger-bad` and `airflow-dq-down`.
- A `DQ_DUCKDB_TEMP_DIRECTORY` setting, which the container sets to `/tmp/duckdb`.
- Archived Trino, Docker and Airflow results in `docs/results/`.
- A design-principles document and a SOLID audit: the lakehouse client import lives in one module, a new `NotificationError`, and stricter settings validation.

### Fixed

- `soda-dq seed` inside the read-only container: it now applies the httplib client and the temp directory, and runs DuckDB's Iceberg writer from a scratch working directory (so no stray `./data` folder appears).
- README: the real-lakehouse snippet now exports `DQ_LDL_REPO_PATH` before it is used.

## [0.1.1] - 2026-10-13

Verified end to end on macOS (arm64, Docker Desktop) against local-data-lakehouse.

### Added

- `soda-dq seed SAMPLE_DIR --yes`: rebuilds the live retail Iceberg tables from CSVs (for example `fixtures/bad_batch`) for gate demos.
- Schema check details (missing, unexpected and mistyped columns, order) in reports, gate reasons, JUnit and Markdown.
- `DQ_DUCKDB_HTTPFS_CLIENT` setting (`httplib` inside containers).
- Seven Mermaid architecture diagrams with SVGs in `docs/diagrams/`.

### Fixed

- DuckDB on the Lakekeeper REST catalog: `USE` the first schema (the catalog has no `main` schema), and warm tables with `DESCRIBE` before the schema check (columns load lazily).
- Contracts expect `timestamp with time zone`, which is what Spark and DuckDB write to Iceberg.
- Trino datasource sends `user: soda-dq` (Trino returns 401 without one).
- Docker image: non-editable install, pre-installed `avro` extension (read-only root filesystem), `DQ_CONTAINER_ENGINE` so the host `.env` no longer leaks into the container.
- Airflow overlay: the DQ DAG ships as a second DAG bundle (`soda-dq`) instead of a nested mount into the read-only dags folder.

## [0.1.0] - 2026-10-13

### Added

- Soda Core 4.26.0 data contracts for the retail medallion: `bronze.orders_raw`, `bronze.customers_raw`,
  `silver.orders`, `silver.customers`, `gold.daily_order_metrics` (46 checks).
- Ports-and-adapters package `soda_lakehouse_dq`: gate policy, verify-layer and run-medallion use cases,
  Soda verifier, DuckDB providers (`lakehouse_client.duckdb_connect()` + `from_existing_cursor`, and
  a Docker-free CSV medallion), Trino provider, file contract repository with `{{ dataset_root }}` rendering.
- Sinks: JSON, JUnit XML, Markdown; optional Iceberg `ops.dq_results`. Notifiers: console (structlog),
  Slack incoming webhook.
- Typer CLI `soda-dq` (`verify`, `run`, `validate`, `list`) with exit codes 0/1/2.
- Airflow DAG `lakehouse_retail_medallion_dq` with gate tasks after each layer (original DAG untouched).
- Docker runner image + `ldl-net` compose file + Airflow overlay (socket-proxy allowlist incl. `ldl-soda`).
- `good_batch` / `bad_batch` fixtures; Makefile; uv lock; ruff, mypy --strict, pre-commit; GitHub Actions CI.
- Docs: README, architecture, use case, contracts, runbook, ADRs 0001-0005.

### Notes

- Numeric contract variables passed as strings are coerced (`--set MAX=48` -> 48): Soda 4.26 rejects strings
  for numeric threshold keys.
