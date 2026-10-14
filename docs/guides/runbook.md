# Runbook

> [!IMPORTANT]
> A **blocked** gate means the data did not meet its contract, or the gate could not prove it did. Do not
> re-run it unchanged: read the report, fix the data, the contract or the threshold, then clear the task.

## Daily operation

| Task | Command |
|---|---|
| Gate one layer | `uv run soda-dq verify silver` (engine from `DQ_ENGINE`, or `-e` with `duckdb`, `duckdb-local` or `trino`) |
| Whole medallion | `uv run soda-dq run` |
| Contract syntax | `make validate` |
| Airflow | `make airflow-dq-up && make airflow-dq-trigger` (UI: `http://localhost:$AIRFLOW_API_PORT`, default 8080, DAG `lakehouse_retail_medallion_dq`) |
| Container | `make dq-up`, then `docker exec ldl-soda soda-dq run` |

Exit codes and their effect in Airflow: [configuration](configuration.md#exit-codes).

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
   problems do not fix themselves.

## Configuration

Every `DQ_*` setting is listed in [configuration](configuration.md).

## Manual walkthrough

`make e2e-live` and `make e2e-full` script all of this (see [running and testing](running-and-testing.md)). To drive
each piece yourself, after the [quickstart](quickstart.md) setup and with `DQ_LDL_REPO_PATH` in `.env`:

```bash
# lakehouse repo: light profile and the retail tables
make up-light && .venv/bin/python scripts/light_demo.py retail
# this repo: live DuckDB gates (expect 14 / 22 / 17 passed)
uv run soda-dq run --engine duckdb
uv run soda-dq seed fixtures/bad_batch --yes && uv run soda-dq run --engine duckdb   # blocks at bronze
uv run soda-dq seed fixtures/good_batch --yes                                        # restore

# lakehouse repo: full profile with Trino and Spark
make up-full TRINO=1 && make e2e
# this repo
uv run soda-dq run --engine trino
make dq-up && docker exec ldl-soda soda-dq run
make airflow-dq-up            # add AIRFLOW_API_PORT=8090 if 8080 is taken (or set it in .env)
make airflow-dq-trigger       # good data: every task succeeds
make airflow-dq-trigger-bad   # fault injection: dq_bronze blocks, downstream upstream_failed
make airflow-dq-trigger       # restore good data
```

The verified output of each command is in [results](../results/README.md).

Tear down with `make dq-down` and `make airflow-dq-down` (or `make down` in the lakehouse repo).

> [!WARNING]
> Never use `docker compose down -v`: it deletes the lakehouse volumes.

Notes:

- Tables written by Spark and DuckDB use `timestamp with time zone`; a contract that says `timestamp` fails the schema check.
- After a bad-batch seed, restore the good data with `uv run soda-dq seed fixtures/good_batch --yes` (or `make e2e` in the lakehouse repo).
- To rerun one gate in Airflow: `airflow tasks test lakehouse_retail_medallion_dq dq_silver <date>`.

## Troubleshooting

Symptoms and fixes: [troubleshooting](troubleshooting.md).
