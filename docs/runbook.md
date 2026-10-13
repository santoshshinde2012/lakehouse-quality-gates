# Runbook

## Daily operation

| Task | Command |
|---|---|
| Gate one layer | `uv run soda-dq verify silver` (engine from `DQ_ENGINE`, or `-e duckdb|duckdb-local|trino`) |
| Whole medallion | `uv run soda-dq run` |
| Contract syntax | `make validate` |
| Airflow | `make airflow-dq-up && make airflow-dq-trigger` (UI: http://localhost:$AIRFLOW_API_PORT, default 8080, DAG `lakehouse_retail_medallion_dq`) |
| Container | `make dq-up`, then `docker exec ldl-soda soda-dq run` |

Exit codes: `0` passed (warnings allowed), `1` blocked, `2` configuration or usage error (missing
contracts, unknown sink, bad `--set`).

## A gate blocked: triage

1. Open `reports/<run>_retail_<layer>.md`. **Reasons** lists each failed check as `contract: column: name`
   with its metrics (`invalid_count`, `duplicate_count`, `failed_rows_count`, ...).
2. Query the offending rows in DuckDB, Trino or Spark. Examples:
   - orphan customers: `SELECT * FROM silver.orders o LEFT JOIN silver.customers c USING (customer_id) WHERE c.customer_id IS NULL`
   - duplicates: `SELECT order_id, _source_file, COUNT(*) FROM bronze.orders_raw GROUP BY 1,2 HAVING COUNT(*) > 1`
3. Decide:
   - **Bad data.** Fix the source or quarantine the file, roll the table back with Iceberg time travel if
     needed (`CALL lakehouse.system.rollback_to_snapshot(...)`), then re-run from the failed layer.
   - **The contract is wrong**, for example after an intended schema change. Change the contract in a PR
     that also covers the producer change.
   - **A threshold is too tight.** Override it for one run (`--set MAX_ORDER_AMOUNT=50000`) or change the
     default in the contract.
4. Re-run: in Airflow, clear the failed `dq_<layer>` task. Retries are off on purpose, because data
   problems don't fix themselves.

## Configuration (`DQ_*`, see `.env.example`)

| Variable | Default | Meaning |
|---|---|---|
| `DQ_ENGINE` | `duckdb` | `duckdb`, `duckdb-local`, `trino` |
| `DQ_LDL_REPO_PATH` | `../local-data-lakehouse` | Checkout whose `src/lakehouse_client` is imported |
| `DQ_LAKEKEEPER_URL` / `DQ_LAKEKEEPER_WAREHOUSE` | `http://localhost:8181` / `lakehouse` | Catalog |
| `DQ_TRINO_HOST` / `DQ_TRINO_PORT` | `localhost` / `8088` | Trino (container: `trino:8080`) |
| `DQ_FAIL_ON_WARN` | `false` | Warnings block |
| `DQ_ALLOW_NOT_EVALUATED` | `false` | Unevaluated checks don't block |
| `DQ_SINKS` | `["json","junit","markdown"]` | Report formats |
| `DQ_ICEBERG_SINK` | `false` | Append rows to `ops.dq_results` (needs Lakekeeper) |
| `DQ_SLACK_WEBHOOK_URL` | blank | Slack incoming webhook (https only; blank disables Slack) |
| `DQ_SLACK_ONLY_ON_PROBLEMS` | `true` | Skip Slack for clean passes |
| `DQ_LOG_JSON` / `DQ_LOG_LEVEL` | `false` / `INFO` | structlog output |
| `SODA_CORE_TELEMETRY_ENABLED` | forced `false` | Soda usage telemetry |

## Verified macOS walkthrough (2026-10-13)

```bash
# SODA
cp .env.example .env   # set DQ_LDL_REPO_PATH=/Users/<you>/.../local-data-lakehouse
uv sync && make lint typecheck test validate demo-good demo-bad

# lakehouse (in its repo)
make up-light && .venv/bin/python scripts/light_demo.py retail
# back in SODA: live DuckDB gates (expect 14 / 21 / 11 passed)
.venv/bin/soda-dq run --engine duckdb
.venv/bin/soda-dq seed fixtures/bad_batch --yes && .venv/bin/soda-dq run --engine duckdb   # all three layers block

# full profile with Trino + Spark (lakehouse repo)
make up-full TRINO=1 && make e2e
.venv/bin/soda-dq run --engine trino

# container + Airflow (8080 was taken on this Mac, so the API server uses 8090)
make dq-up
make airflow-dq-up AIRFLOW_API_PORT=8090   # or set AIRFLOW_API_PORT in .env
make airflow-dq-trigger                    # good data: every task succeeds
make airflow-dq-trigger-bad                # fault injection: dq_bronze blocks, downstream upstream_failed
make airflow-dq-trigger                    # restore good data
```

Tear down with `make dq-down` and `make airflow-dq-down` (or `make down` in the lakehouse repo). Never use `-v`, because that deletes the lakehouse volumes.

Notes:
- Tables written by Spark and DuckDB use `timestamp with time zone`; a contract that says `timestamp` fails the schema check.
- After a bad-batch seed, run `make e2e` (lakehouse repo) to restore the good data.
- To rerun one gate in Airflow: `airflow tasks test lakehouse_retail_medallion_dq dq_silver <date>`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `lakehouse_client not found under …/src` | Set `DQ_LDL_REPO_PATH` to the local-data-lakehouse checkout |
| `cannot attach Lakekeeper at http://localhost:8181` | The lakehouse isn't up: `make up-light` in the lakehouse repo |
| `Table … does not exist` | Tables not built yet: `make demo-light`, `make e2e` or the Airflow DAG |
| Schema check fails after a deploy | Intended drift: update the contract's `columns`. Otherwise, roll back |
| `YAML key 'must_be_less_than' expected one of ['Number'], but was str` | A string variable was passed straight to the Soda API. Use the CLI or `coerce_variable` |
| Airflow `dq_*`: `403` from the docker proxy | The overlay isn't applied. Start Airflow with `make airflow-dq-up` (adds `ldl-soda` to the allowlist) |
| Airflow `dq_*`: `No such container: ldl-soda` | `make dq-up` first (needs network `ldl-net`) |
| Freshness fails on old data | Bronze must be ingested within `BRONZE_MAX_AGE_HOURS` (24). For a replay, `--set BRONZE_MAX_AGE_HOURS=720` |
