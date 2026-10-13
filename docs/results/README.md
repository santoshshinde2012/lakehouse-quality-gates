# Verification results (v0.2.1)

These are real artefacts from runs on macOS (arm64, Docker Desktop) on 2026-10-13, using Soda Core 4.26.0 and Python 3.12.
Every file in this folder was produced by the commands below; only this README was written by hand.
Live runs used local-data-lakehouse (Lakekeeper Iceberg REST, RustFS, Spark 4.1.3, Trino 483, Airflow 3).

## Summary

| Check | Command | Result | Evidence |
|---|---|---|---|
| Lint | `ruff format --check . && ruff check .` | ✅ clean | [lint.log](logs/lint.log) |
| Types | `mypy` (strict) | ✅ no issues | [typecheck.log](logs/typecheck.log) |
| Tests | `pytest --cov` | ✅ 96 passed | [pytest.log](logs/pytest.log) · [JUnit](tests/pytest-junit.xml) |
| Coverage | html + xml | ✅ 99% (825 statements, 10 missed) | [HTML](coverage/html/index.html) · [XML](coverage/coverage.xml) |
| Contract syntax | `soda-dq validate` | ✅ 5/5 | [validate.log](logs/validate.log) |
| Good batch, no Docker | `make demo-good` | ✅ 3/3 gates passed | [log](logs/demo-good.log) |
| Bad batch, no Docker | `make demo-bad` | ✅ 3/3 gates blocked | [log](logs/demo-bad.log) |
| Live DuckDB, good (Spark-written data) | `make e2e` then `soda-dq run -e duckdb` | ✅ 46/46 passed | [e2e](logs/live-spark-e2e.log) · [run](logs/live-duckdb-good.log) |
| Live DuckDB, bad | `soda-dq seed fixtures/bad_batch --yes` then `run` | ✅ blocked at bronze | [seed](logs/live-seed-bad.log) · [run](logs/live-duckdb-bad.log) |
| Trino, good | `soda-dq run -e trino` | ✅ 46/46 passed | [run](logs/trino-good.log) |
| Trino, bad | same, after the bad seed | ✅ blocked at bronze | [run](logs/trino-bad.log) |
| Docker image | `make dq-up` | ✅ `lakehouse-quality-gates:0.2.0` (built just before the 0.2.1 bump; the image contents are identical apart from the seed fix), user `dq`, read-only root, `cap_drop: ALL`, healthy | [build](docker/build-and-up.log) · [image](docker/image.txt) |
| `ldl-soda`, bad then good | `docker exec ldl-soda soda-dq seed … && soda-dq run` | ✅ bad blocked at bronze; good passed 46/46 | [bad](docker/ldl-soda-seed-bad-run.log) · [good](docker/ldl-soda-run-good.log) |
| Airflow, good data | `make airflow-dq-trigger` (port 8090) | ✅ all 9 tasks succeeded | [run](airflow/dag-run-good.log) · [tasks](airflow/task-states-good.txt) |
| Airflow, bad data | `make airflow-dq-trigger-bad` | ✅ `dq_bronze` failed; every downstream task `upstream_failed` | [run](airflow/dag-run-bad.log) · [tasks](airflow/task-states-bad.txt) |
| DAG import errors | `airflow dags list-import-errors` | ✅ none | [import-errors.txt](airflow/import-errors.txt) |

`soda-dq run` stops at the first blocked layer, so live bad runs end at bronze. The per-layer bad results are
in the no-Docker bad batch below, which verifies each layer on its own.

## Good batch (duckdb-local)

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `passed` | 14 | 0 | 0 | [json](gates/good/20261013T093946Z-bba130b8_retail_bronze.json) · [md](gates/good/20261013T093946Z-bba130b8_retail_bronze.md) · [junit](gates/good/20261013T093946Z-bba130b8_retail_bronze.xml) |
| silver | `passed` | 21 | 0 | 0 | [json](gates/good/20261013T093946Z-bba130b8_retail_silver.json) · [md](gates/good/20261013T093946Z-bba130b8_retail_silver.md) · [junit](gates/good/20261013T093946Z-bba130b8_retail_silver.xml) |
| gold | `passed` | 11 | 0 | 0 | [json](gates/good/20261013T093946Z-bba130b8_retail_gold.json) · [md](gates/good/20261013T093946Z-bba130b8_retail_gold.md) · [junit](gates/good/20261013T093946Z-bba130b8_retail_gold.xml) |

## Bad batch (duckdb-local): every gate blocks

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `blocked` | 10 | 2 | 2 | [json](gates/bad/20261013T093947Z-583cad67_retail_bronze.json) · [md](gates/bad/20261013T093947Z-583cad67_retail_bronze.md) · [junit](gates/bad/20261013T093947Z-583cad67_retail_bronze.xml) |
| silver | `blocked` | 14 | 1 | 6 | [json](gates/bad/20261013T093948Z-20ee869b_retail_silver.json) · [md](gates/bad/20261013T093948Z-20ee869b_retail_silver.md) · [junit](gates/bad/20261013T093948Z-20ee869b_retail_silver.xml) |
| gold | `blocked` | 9 | 1 | 1 | [json](gates/bad/20261013T093948Z-7de931af_retail_gold.json) · [md](gates/bad/20261013T093948Z-7de931af_retail_gold.md) · [junit](gates/bad/20261013T093948Z-7de931af_retail_gold.xml) |

| Layer | Why it blocked |
|---|---|
| bronze | failed: No order duplicated inside one source file |
| bronze | failed: customer_id: Every order has a customer |
| bronze | warned: status: Status is a known value (case-insensitive) |
| bronze | warned: amount: Amount is not negative (enforced in silver) |
| silver | failed: email: Email is well-formed |
| silver | failed: email: Email unique |
| silver | failed: No order timestamped in the future |
| silver | failed: customer_id: Customer id present |
| silver | failed: customer_id: Customer exists in silver.customers |
| silver | failed: amount: Amount is not negative |
| silver | warned: amount: Amount below the outlier ceiling |
| gold | failed: No metrics for future days |
| gold | warned: avg_order_value: Average order value is plausible |

## Live lakehouse: DuckDB → Lakekeeper Iceberg

Good:

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `passed` | 14 | 0 | 0 | [json](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_bronze.json) · [md](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_bronze.md) · [junit](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_bronze.xml) |
| silver | `passed` | 21 | 0 | 0 | [json](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_silver.json) · [md](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_silver.md) · [junit](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_silver.xml) |
| gold | `passed` | 11 | 0 | 0 | [json](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_gold.json) · [md](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_gold.md) · [junit](gates/live-duckdb-good/20261013T093027Z-ab4c8f52_retail_gold.xml) |

Bad:

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `blocked` | 10 | 2 | 2 | [json](gates/live-duckdb-bad/20261013T093817Z-a7c9cb34_retail_bronze.json) · [md](gates/live-duckdb-bad/20261013T093817Z-a7c9cb34_retail_bronze.md) · [junit](gates/live-duckdb-bad/20261013T093817Z-a7c9cb34_retail_bronze.xml) |

## Live lakehouse: Trino 483

Good:

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `passed` | 14 | 0 | 0 | [json](gates/trino-good/20261013T093031Z-a93abd20_retail_bronze.json) · [md](gates/trino-good/20261013T093031Z-a93abd20_retail_bronze.md) · [junit](gates/trino-good/20261013T093031Z-a93abd20_retail_bronze.xml) |
| silver | `passed` | 21 | 0 | 0 | [json](gates/trino-good/20261013T093031Z-a93abd20_retail_silver.json) · [md](gates/trino-good/20261013T093031Z-a93abd20_retail_silver.md) · [junit](gates/trino-good/20261013T093031Z-a93abd20_retail_silver.xml) |
| gold | `passed` | 11 | 0 | 0 | [json](gates/trino-good/20261013T093031Z-a93abd20_retail_gold.json) · [md](gates/trino-good/20261013T093031Z-a93abd20_retail_gold.md) · [junit](gates/trino-good/20261013T093031Z-a93abd20_retail_gold.xml) |

Bad:

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `blocked` | 10 | 2 | 2 | [json](gates/trino-bad/20261013T093818Z-cbd0abbe_retail_bronze.json) · [md](gates/trino-bad/20261013T093818Z-cbd0abbe_retail_bronze.md) · [junit](gates/trino-bad/20261013T093818Z-cbd0abbe_retail_bronze.xml) |

## Airflow DAG `lakehouse_retail_medallion_dq`

Good run `manual__ldl_20261013T093306`:

| Task | State |
|---|---|
| `land_smoke` | success |
| `bronze` | success |
| `inject_bad_batch` | success |
| `dq_bronze` | success |
| `silver` | success |
| `dq_silver` | success |
| `gold` | success |
| `dq_gold` | success |
| `query_timetravel` | success |

Bad run `manual__ldl_20261013T093637` (conf `{"inject_bad_batch": true}`):

| Task | State |
|---|---|
| `land_smoke` | success |
| `bronze` | success |
| `inject_bad_batch` | success |
| `dq_bronze` | failed |
| `silver` | upstream_failed |
| `dq_silver` | upstream_failed |
| `gold` | upstream_failed |
| `dq_gold` | upstream_failed |
| `query_timetravel` | upstream_failed |

All runs are listed in [dag-runs.txt](airflow/dag-runs.txt).

## Reproduce

```bash
make lint typecheck test validate demo-good demo-bad
# lakehouse repo: make up-full TRINO=1 && make e2e
DQ_ENGINE=duckdb soda-dq run && DQ_ENGINE=trino soda-dq run
soda-dq seed fixtures/bad_batch --yes   # then run again: blocked; restore with seed fixtures/good_batch --yes
make dq-up && docker exec ldl-soda soda-dq run
make airflow-dq-up AIRFLOW_API_PORT=8090 && make airflow-dq-trigger && make airflow-dq-trigger-bad
```
