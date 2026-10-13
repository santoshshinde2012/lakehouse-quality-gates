# Changelog

All notable changes to this project. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

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
