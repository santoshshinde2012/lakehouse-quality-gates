# Changelog

All notable changes to this project. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

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
