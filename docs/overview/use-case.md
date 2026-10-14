# Use case: contract-gated medallion

> [!NOTE]
> **At a glance.** Bad retail batches used to flow from bronze to gold unchecked. A Soda contract gate after each
> layer now stops the run at the first layer whose contract fails, and says exactly why.

## Problem

The retail pipeline in local-data-lakehouse promotes data `bronze → silver → gold` with Spark. Each layer has
an implicit contract: silver has one row per `order_id` and only known statuses, gold has one row per day
and revenue that is never negative. Nothing *checks* those contracts. If a bad batch arrives (a negative
amount, an orphan customer, a malformed email, a timestamp in the future), the pipeline carries it through
to the gold metrics consumers read.

## Solution

Each layer gets Soda data contracts, and a gate verifies them before the next layer is built (terms in the [glossary](glossary.md); every check in the [contracts guide](../guides/contracts.md)):

| Gate | Guarantees (fail) | Signals (warn) |
|---|---|---|
| `dq_bronze` | Schema unchanged, batch not empty, ingested within 24 h, no duplicate order in one file, every order has an id, a customer, a timestamp and an amount | Unknown raw status, negative amount (both cleaned or enforced in silver) |
| `dq_silver` | Schema unchanged, `order_id` and `customer_id` unique, customer exists (referential integrity), canonical status, amount ≥ 0, no future timestamps, well-formed and unique emails | Amount above the outlier ceiling (`MAX_ORDER_AMOUNT`) |
| `dq_gold` | Schema unchanged, one row per day, no future days, orders ≥ 1, revenue and AOV ≥ 0, cancelled + refunded ≤ orders, `SUM(orders)` equals the silver row count | AOV above `MAX_AVG_ORDER_VALUE` |

What happens when a gate fails:

- the gate task fails and the run stops, so the next layer is not built from bad data;
- the console and, if configured, Slack say which checks failed and why;
- `reports/` gets JSON (machine-readable), JUnit XML (CI test view) and Markdown (for humans, and the
  GitHub step summary).

Bronze **warns** where silver **fails** because bronze is the raw record. The job of the bronze gate is to catch
*structural* breakage early: schema drift, empty files, missing keys. Value rules are enforced once the
data has been cleaned.

## Demo walkthrough (`make demo`)

1. **Good batch.** `fixtures/good_batch` holds the repo's own sample CSVs. All 53 checks pass in all three
   layers.
2. **Bad batch.** `fixtures/bad_batch` adds `orders_day3.csv` and two customers with injected defects (see
   its README). Running each layer separately shows defence in depth:

   | Layer | Blocked by | Warnings |
   |---|---|---|
   | bronze | a duplicate order inside one file; a missing `customer_id` | unknown status, negative amount |
   | silver | malformed email, duplicate email, future timestamp, missing and orphan customer, negative amount | outlier amount |
   | gold | a metrics row for 2099 | implausible AOV |

   `soda-dq run` on the bad batch stops at bronze. Airflow behaves the same way: silver and gold never run.

## Limits and next steps

- In the gated DAG the Spark jobs use `CREATE OR REPLACE`, so a blocked silver gate stops gold from being built,
  but silver itself has already been overwritten. The write-audit-publish DAG and `soda-dq wap run` isolate even
  the failing layer: each layer stays on an Iceberg audit branch until its gate passes, and `main` keeps the last
  good data. See [write-audit-publish](../architecture/write-audit-publish.md).
- Soda's dedicated group-by and reconciliation check types, and percentage thresholds on `failed_rows`, need a
  paid Soda licence. These contracts stay inside the free Soda Core: the gold-to-silver reconciliation is a SQL
  `metric` check.
