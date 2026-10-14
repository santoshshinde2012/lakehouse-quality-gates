# Write-audit-publish run, 2026-10-14

Live proof that a good batch is published to `main` and a bad batch never reaches it. Lakehouse full profile with
`docker/docker-compose.wap.yml` (Spark 4.1.3, Iceberg 1.12, Lakekeeper), gates on host DuckDB 1.5.6, Airflow 3.
Times are IST (UTC+5:30).

## CLI: good batch, then bad batch

`uv run soda-dq wap run --all --batch good --evidence cli-good.json` (10:58 IST, exit 0), then
`... --batch bad --evidence cli-bad.json` (11:00 IST, exit 1). Snapshot ids from [`cli-good.json`](cli-good.json)
and [`cli-bad.json`](cli-bad.json):

| Table | `main` before | Audited branch snapshot | `main` after good | Rows (branch = main) |
|---|---|---|---|---|
| bronze.orders_raw | 6960657473168426969 | 6313071372401083325 | 6313071372401083325 | 22 = 22 |
| bronze.customers_raw | 1660378615478777296 | 5352607284838812996 | 5352607284838812996 | 10 = 10 |
| silver.orders | 4981470092929694725 | 5507301427094681899 | 5507301427094681899 | 19 = 19 |
| silver.customers | 6378550253007205579 | 2923366684003229905 | 2923366684003229905 | 10 = 10 |
| gold.daily_order_metrics | 2389062495651120009 | 7658322153456504503 | 7658322153456504503 | 2 = 2 |

All three gates passed (14, 21 and 11 checks); `main` of every table moved to exactly the snapshot the gate
verified, with the same row count.

Bad batch (`fixtures/bad_batch`) on branch `audit_20261014t052958z_19ce388e`:

| Table | `main` after bad batch | Rows on `main` | Branch snapshot (kept) | Rows on branch |
|---|---|---|---|---|
| bronze.orders_raw | 6313071372401083325 (unchanged) | 22 | 6160888266221912068 | 29 |
| bronze.customers_raw | 5352607284838812996 (unchanged) | 10 | 4347840016123168408 | 12 |

The bronze gate blocked (`No order duplicated inside one source file`, `Every order has a customer`); silver and
gold were never written. `soda-dq run -e duckdb` on `main` afterwards passed every layer: `main` still holds the
good data.

## Airflow: `lakehouse_retail_medallion_wap`

- Good run (`make airflow-wap-trigger`, 11:04 IST): every task succeeded; `main` of `bronze.orders_raw` moved to
  2937753052092199088 and `bronze.customers_raw` to 2315605737112221829
  ([`airflow-bad-main-before.json`](airflow-bad-main-before.json)).
- Bad run (`make airflow-wap-trigger-bad`, conf `{"bad_batch": true}`, 11:08 IST): `wap_write_bronze` succeeded,
  `dq_bronze` failed, `publish_bronze` and every later task `upstream_failed`
  ([`airflow-bad-tasks.txt`](airflow-bad-tasks.txt)). `main` after the run has the same snapshot ids and row counts as before
  (2937753052092199088 with 22 rows, 2315605737112221829 with 10 rows;
  [`airflow-bad-main-after.json`](airflow-bad-main-after.json)), and the run's branch `audit_20261014t053852` is
  kept with 29 and 12 rows ([`airflow-bad-branch.json`](airflow-bad-branch.json)).

## Live test

`uv run pytest -m full -k wap` (`tests/live/test_wap.py`): 1 passed in 166 s. It repeats both CLI runs and asserts
the same properties: after the good batch every `main` snapshot equals the audited branch snapshot with equal row
counts; after the bad batch `main` snapshot ids and rows are unchanged, the branch exists with more rows, and the
gates on `main` pass.

## Reproduce

`make wap-up`, then `make wap-demo-good wap-demo-bad`, or everything with `make e2e-wap`.
