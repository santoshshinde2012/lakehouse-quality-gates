# Verification results

Real artefacts from a run on macOS (arm64, Docker Desktop) on 2026-10-13, Soda Core 4.26.0, Python 3.12.
Everything in this folder was produced by the commands below. Nothing was edited by hand except this README.

## Summary

| Check | Command | Result | Log |
|---|---|---|---|
| Lint | `ruff format --check . && ruff check .` | ✅ clean | [lint.log](logs/lint.log) |
| Types | `mypy` (strict) | ✅ no issues in 31 files | [typecheck.log](logs/typecheck.log) |
| Tests | `pytest --cov` | ✅ 94 passed | [pytest.log](logs/pytest.log) · [JUnit](tests/pytest-junit.xml) |
| Coverage | `--cov-report html xml` | ✅ 99% (815 statements, 10 missed) | [HTML](coverage/html/index.html) · [XML](coverage/coverage.xml) |
| Contract syntax | `soda-dq validate` | ✅ 5/5 contracts valid | [validate.log](logs/validate.log) |
| Good batch | `make demo-good` (duckdb-local) | ✅ 3/3 gates passed | [demo-good.log](logs/demo-good.log) |
| Bad batch | `make demo-bad` (duckdb-local) | ✅ 3/3 gates blocked (exit 1, as intended) | [demo-bad.log](logs/demo-bad.log) |
| Live lakehouse | `make up-light` + `light_demo.py retail`, then `soda-dq run -e duckdb` | ✅ 3/3 gates passed on Iceberg via Lakekeeper | [seed](logs/live-light-seed.log) · [run](logs/live-duckdb.log) |

## Good batch (duckdb-local)

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `passed` | 14 | 0 | 0 | [json](gates/good/20261013T062549Z-f1e1f033_retail_bronze.json) · [md](gates/good/20261013T062549Z-f1e1f033_retail_bronze.md) · [junit](gates/good/20261013T062549Z-f1e1f033_retail_bronze.xml) |
| silver | `passed` | 21 | 0 | 0 | [json](gates/good/20261013T062549Z-f1e1f033_retail_silver.json) · [md](gates/good/20261013T062549Z-f1e1f033_retail_silver.md) · [junit](gates/good/20261013T062549Z-f1e1f033_retail_silver.xml) |
| gold | `passed` | 11 | 0 | 0 | [json](gates/good/20261013T062549Z-f1e1f033_retail_gold.json) · [md](gates/good/20261013T062549Z-f1e1f033_retail_gold.md) · [junit](gates/good/20261013T062549Z-f1e1f033_retail_gold.xml) |

## Bad batch (duckdb-local): each gate blocks

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `blocked` | 10 | 2 | 2 | [json](gates/bad/20261013T062550Z-c8e8b182_retail_bronze.json) · [md](gates/bad/20261013T062550Z-c8e8b182_retail_bronze.md) · [junit](gates/bad/20261013T062550Z-c8e8b182_retail_bronze.xml) |
| silver | `blocked` | 14 | 1 | 6 | [json](gates/bad/20261013T062551Z-193b005c_retail_silver.json) · [md](gates/bad/20261013T062551Z-193b005c_retail_silver.md) · [junit](gates/bad/20261013T062551Z-193b005c_retail_silver.xml) |
| gold | `blocked` | 9 | 1 | 1 | [json](gates/bad/20261013T062552Z-c9b3230b_retail_gold.json) · [md](gates/bad/20261013T062552Z-c9b3230b_retail_gold.md) · [junit](gates/bad/20261013T062552Z-c9b3230b_retail_gold.xml) |

Why each layer blocked (from the gate reports):

| Layer | Reason |
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

## Live lakehouse (DuckDB attached to Lakekeeper's Iceberg REST catalog)

| Layer | Gate | Passed | Warned | Failed | Reports |
|---|---|---|---|---|---|
| bronze | `passed` | 14 | 0 | 0 | [json](gates/live-duckdb/20261013T062614Z-34353c63_retail_bronze.json) · [md](gates/live-duckdb/20261013T062614Z-34353c63_retail_bronze.md) · [junit](gates/live-duckdb/20261013T062614Z-34353c63_retail_bronze.xml) |
| silver | `passed` | 21 | 0 | 0 | [json](gates/live-duckdb/20261013T062614Z-34353c63_retail_silver.json) · [md](gates/live-duckdb/20261013T062614Z-34353c63_retail_silver.md) · [junit](gates/live-duckdb/20261013T062614Z-34353c63_retail_silver.xml) |
| gold | `passed` | 11 | 0 | 0 | [json](gates/live-duckdb/20261013T062614Z-34353c63_retail_gold.json) · [md](gates/live-duckdb/20261013T062614Z-34353c63_retail_gold.md) · [junit](gates/live-duckdb/20261013T062614Z-34353c63_retail_gold.xml) |

Also verified by hand earlier the same day, but not archived here: the Trino engine (46/46 passed on Spark-written data, and it blocked the bad batch), the `ldl-soda` Docker image, and the Airflow DAG `lakehouse_retail_medallion_dq` (all 8 tasks succeeded; the gate tasks fail on bad data). See [the runbook](../runbook.md).

## Reproduce

```bash
make lint typecheck test validate demo-good demo-bad
# live: in local-data-lakehouse run `make up-light`, then `.venv/bin/python scripts/light_demo.py retail`
DQ_ENGINE=duckdb soda-dq run
```
