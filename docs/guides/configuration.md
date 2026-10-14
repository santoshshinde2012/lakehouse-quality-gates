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
| `2` | Configuration or usage error: missing contracts, unreachable catalog, bad `--set`, unknown sink | Task fails; no gate decision was made |

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
| `AIRFLOW_API_PORT` (Makefile) | `8080` | Airflow UI/API host port |

`SODA_CORE_TELEMETRY_ENABLED` defaults to `false` (set in the image and at start-up unless you set it yourself). Gate policy settings are explained in the [gate policy](../architecture/data-flow.md#gate-policy-decision-flow).
