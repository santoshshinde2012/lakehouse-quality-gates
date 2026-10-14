# Configuration and CLI reference

> [!NOTE]
> **At a glance.** CLI commands, three exit codes (`0` pass, `1` blocked, `2` configuration error) and
> `DQ_*` environment variables for everything else.

## Commands

| Command | What it does |
|---|---|
| `soda-dq verify <layer> [-e ENGINE] [-p PIPELINE] [-s K=V]` | Gate one layer (`bronze`, `silver`, `gold`); see [exit codes](#exit-codes) |
| `soda-dq run [-e ENGINE] [-p PIPELINE]` | Gate bronze → silver → gold, stopping at the first blocked layer |
| `soda-dq validate [-p PIPELINE]` | Check the syntax of every contract with Soda, without touching data |
| `soda-dq list [-e ENGINE]` | List contracts and their rendered dataset names |
| `soda-dq seed SAMPLE_DIR --yes` | Demo only: rebuild the live retail Iceberg tables from a CSV batch, e.g. `fixtures/bad_batch` |

`ENGINE` is `duckdb`, `duckdb-local` or `trino`. `--sample-dir` points `duckdb-local` at another CSV batch.
Run `make help` to list every Makefile target.

The `soda-dq wap` commands are `run`, `verify`, `publish`, `status` and `drop`.
`wap verify <layer> --branch <branch> [--evidence FILE]` records exact snapshot IDs;
`wap publish <layer> --audit FILE` publishes or resumes publication from a passing manifest.
See [write-audit-publish](../architecture/write-audit-publish.md) for branch retention and recovery.

## Exit codes

| Code | Meaning | Airflow effect |
|---|---|---|
| `0` | Gate **passed** or **passed with warnings** | Task succeeds; the next layer runs |
| `1` | Gate **blocked** | Task fails; downstream tasks are `upstream_failed` |
| `2` | Configuration, usage, or operational error: missing contracts, unreachable catalog, invalid options, report persistence, or publication failure | Task fails; inspect reports and saved pins because a decision may exist or publication may be partial |

## Settings (`DQ_*`)

All settings are environment variables with the `DQ_` prefix (or entries in `.env`), validated by
pydantic-settings in [`settings.py`](../../src/soda_lakehouse_dq/settings.py).

| Variable | Default | Purpose |
|---|---|---|
| `DQ_ENGINE` | `duckdb` | `duckdb` (Lakekeeper via DuckDB), `duckdb-local` (CSV, no Docker), `trino` |
| `DQ_LDL_REPO_PATH` | `../local-data-lakehouse` | Checkout of local-data-lakehouse (its `lakehouse_client` is reused) |
| `DQ_LAKEKEEPER_URL` | `http://localhost:8181` | Iceberg REST catalog |
| `DQ_LAKEKEEPER_WAREHOUSE` | `lakehouse` | Lakekeeper warehouse |
| `DQ_CATALOG_ALIAS` | `lakehouse` | DuckDB alias for the attached catalog |
| `DQ_TRINO_HOST` / `DQ_TRINO_PORT` | `localhost` / `8088` | Trino coordinator |
| `DQ_DUCKDB_DATASOURCE_NAME` / `DQ_TRINO_DATASOURCE_NAME` | `lakehouse_duckdb` / `lakehouse_trino` | Dataset prefixes used when rendering contracts |
| `DQ_FAIL_ON_WARN` | `false` | Make warnings block |
| `DQ_ALLOW_NOT_EVALUATED` | `false` | Let not-evaluated checks pass |
| `DQ_SINKS` | `["json","junit","markdown"]` | Report formats written to `DQ_REPORTS_DIR` |
| `DQ_ICEBERG_SINK` | `false` | Also append results to Iceberg `ops.dq_results` |
| `DQ_SLACK_WEBHOOK_URL` | unset | Slack incoming webhook (HTTPS only, stored as a secret) |
| `DQ_SLACK_ONLY_ON_PROBLEMS` | `true` | Skip Slack for clean passes |
| `DQ_LOG_JSON` / `DQ_LOG_LEVEL` | `false` / `INFO` | structlog output |
| `DQ_DUCKDB_HTTPFS_CLIENT` | unset | `httplib` inside containers (DNS for `*.localhost`) |
| `DQ_DUCKDB_TEMP_DIRECTORY` | unset | Writable spill directory, e.g. `/tmp/duckdb` on a read-only root filesystem |
| `DQ_CONTRACTS_DIR`, `DQ_DATASOURCES_DIR`, `DQ_REPORTS_DIR`, `DQ_SAMPLE_DIR` | project folders | Path overrides |
| `DQ_WAP_SPARK_CONTAINER` / `DQ_WAP_JOB` | `ldl-spark` / `/opt/soda-wap/wap_job.py` | Spark container and mounted WAP job |
| `DQ_WAP_TIMEOUT_S` | `900` | Timeout for a Spark WAP command, minimum 10 seconds |
| `DQ_WAP_DROP_BLOCKED` | `false` | Delete blocked source branches instead of retaining them for triage |
| `AIRFLOW_API_PORT` (Makefile) | `8080` | Airflow UI/API host port |

`SODA_CORE_TELEMETRY_ENABLED` defaults to `false` (set in the image and at start-up unless you set it yourself). Gate policy settings are explained in the [gate policy](../architecture/data-flow.md#gate-policy-decision-flow).

The supplied Airflow/socket-proxy integration uses the lakehouse's default container names, including
`ldl-soda` and `ldl-spark`. A standalone runner can use `LDL_SODA_CONTAINER`, but changing names for Airflow
also requires matching environment and exact proxy allowlist entries in deployment overlays.
