# Data flow

> [!NOTE]
> **At a glance.** Spark builds a layer, then its gate runs: the contracts are rendered, Soda verifies them through
> DuckDB or Trino, and `GatePolicy` decides. Exit 1 stops the Airflow run; exit 0 lets the next layer build.

## Medallion flow with quality gates

One gate after each layer; only a passing gate lets the next layer run.

<!-- diagram:medallion-gates -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Medallion flow with quality gates
%% Purpose: One gate after each layer; a failed gate stops promotion
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
flowchart LR
  subgraph BRZ["Bronze"]
    direction TB
    B[("bronze tables")]:::store
    GB("Bronze gate<br/><small>14 checks</small>"):::ours
    X1(["Blocked"]):::blocked
    B --> GB
    GB -. fail .-> X1
  end
  subgraph SLV["Silver"]
    direction TB
    S[("silver tables")]:::store
    GS("Silver gate<br/><small>21 checks</small>"):::ours
    X2(["Blocked"]):::blocked
    S --> GS
    GS -. fail .-> X2
  end
  subgraph GLD["Gold"]
    direction TB
    G[("gold tables")]:::store
    GG("Gold gate<br/><small>11 checks</small>"):::ours
    X3(["Blocked"]):::blocked
    G --> GG
    GG -. fail .-> X3
  end
  OK(["Published"]):::pass
  BRZ -->|pass| SLV
  SLV -->|pass| GLD
  GLD -->|pass| OK
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  style BRZ fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style SLV fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style GLD fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  linkStyle 1,3,5 stroke:#dc2626,stroke-width:1.5px
  linkStyle 6,7,8 stroke:#16a34a,stroke-width:2px
```
<!-- /diagram -->

**How to read it:**

- Green arrows are **pass**: the next layer is built.
- Red dotted arrows are **fail**: the run is **blocked** at that layer.
- The gates run 14 (bronze), 21 (silver) and 11 (gold) checks: 46 in total.
- A **warn** does not block unless `DQ_FAIL_ON_WARN=true`.

## Data model

The retail Iceberg tables the gates check, and the `ops.dq_results` table the Iceberg sink writes.

<!-- diagram:data-model -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Data model
%% Purpose: The retail Iceberg tables the gates check and the ops.dq_results table they write
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
erDiagram
  BRONZE_CUSTOMERS_RAW ||--o{ SILVER_CUSTOMERS : "cleaned into"
  BRONZE_ORDERS_RAW ||--o{ SILVER_ORDERS : "cleaned into"
  SILVER_CUSTOMERS ||--o{ SILVER_ORDERS : "customer_id"
  SILVER_ORDERS }o--|| GOLD_DAILY_ORDER_METRICS : "aggregated by day"
  BRONZE_ORDERS_RAW {
    varchar order_id
    varchar customer_id
    timestamptz order_ts
    varchar status
    double amount
    varchar _source_file
  }
  BRONZE_CUSTOMERS_RAW {
    varchar customer_id
    varchar name
    varchar email
    varchar city
    date signup_date
    varchar _source_file
  }
  SILVER_ORDERS {
    varchar order_id PK
    varchar customer_id FK
    timestamptz order_ts
    varchar status
    double amount
  }
  SILVER_CUSTOMERS {
    varchar customer_id PK
    varchar name
    varchar email
    varchar city
    date signup_date
  }
  GOLD_DAILY_ORDER_METRICS {
    date order_date PK
    bigint orders
    double revenue
    double avg_order_value
    bigint cancelled_orders
    bigint refunded_orders
  }
  DQ_RESULTS {
    varchar run_id
    varchar layer
    varchar gate_status
    varchar contract
    varchar check_name
    varchar outcome
  }
```
<!-- /diagram -->

**How to read it:**

- Bronze keeps raw values plus lineage columns (`_source_file`, `_ingested_at`); silver is cleaned and typed; gold is one row per day.
- Crow's feet mean one-to-many. `silver.orders.customer_id` must exist in `silver.customers` (a `valid_reference_data` check).
- Gold reconciles with silver: `SUM(orders)` must equal the silver row count.
- `DQ_RESULTS` (`ops.dq_results`) is only written with `DQ_ICEBERG_SINK=true`, one row per check. Column lists are abridged.

## What each gate checks

One YAML file per dataset under `contracts/retail/<layer>/`. Bronze keeps raw values, so value problems
**warn** there and are **enforced** in silver; structural problems fail at bronze.

| Layer | Dataset | Check | Type | Level |
|---|---|---|---|---|
| bronze | `orders_raw` | Schema is unchanged | schema | fail |
| bronze | `orders_raw` | Batch is not empty | row_count ≥ 1 | fail |
| bronze | `orders_raw` | Ingested within `BRONZE_MAX_AGE_HOURS` (24) | freshness on `_ingested_at` | fail |
| bronze | `orders_raw` | No order duplicated inside one source file | duplicate (`order_id`, `_source_file`) | fail |
| bronze | `orders_raw` | Every order has an id, a customer, a timestamp, an amount | missing × 4 | fail |
| bronze | `orders_raw` | Status is a known value (case-insensitive) | invalid, `LOWER(TRIM(status))` | warn |
| bronze | `orders_raw` | Amount is not negative | invalid, `valid_min: 0` | warn |
| bronze | `customers_raw` | Schema, not empty, every customer has an id | schema, row_count, missing | fail |
| bronze | `customers_raw` | Email is present | missing | warn |
| silver | `orders` | Schema, not empty | schema, row_count | fail |
| silver | `orders` | No order timestamped in the future | failed_rows | fail |
| silver | `orders` | Order id present and unique after dedup | missing, duplicate | fail |
| silver | `orders` | Customer exists in `silver.customers` | invalid, `valid_reference_data` | fail |
| silver | `orders` | Status is canonical; amount present and not negative | invalid, missing | fail |
| silver | `orders` | Amount below `MAX_ORDER_AMOUNT` (10,000) | invalid, `valid_max` | warn |
| silver | `customers` | Schema, not empty, id present and unique, name, email present, well-formed and unique, signup date present | schema, row_count, missing, duplicate, regex | fail |
| gold | `daily_order_metrics` | Schema, at least one day, one row per day, order date present | schema, row_count, duplicate, missing | fail |
| gold | `daily_order_metrics` | Cancelled + refunded never exceed orders; no future days | failed_rows | fail |
| gold | `daily_order_metrics` | Order count reconciles with `silver.orders` | metric (SQL), must be 0 | fail |
| gold | `daily_order_metrics` | Orders ≥ 1, revenue and average order value not negative | invalid | fail |
| gold | `daily_order_metrics` | Average order value ≤ `MAX_AVG_ORDER_VALUE` (1,000) | invalid | warn |

Totals: 14 bronze, 21 silver and 11 gold checks (46). Full text in [contracts.md](../guides/contracts.md).

## Gate policy decision flow

How `GatePolicy` turns the check outcomes of one layer into *passed*, *passed with warnings* or *blocked*.

<!-- diagram:gate-policy -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Gate policy decision flow
%% Purpose: How GatePolicy turns check outcomes into passed, passed with warnings or blocked
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
flowchart TB
  IN(["Contract results<br/><small>for one layer</small>"]):::actor
  Z{"No contracts<br/>verified?"}:::node
  E{"Execution<br/>error?"}:::node
  F{"Any check<br/>failed?"}:::node
  N{"Any check<br/>not evaluated?"}:::node
  W{"Any check<br/>warned?"}:::node
  B(["Blocked<br/><small>exit 1</small>"]):::blocked
  PW(["Passed with warnings<br/><small>exit 0</small>"]):::ours
  P(["Passed<br/><small>exit 0</small>"]):::pass
  IN --> Z
  Z -->|no| E
  E -->|no| F
  F -->|no| N
  N -->|no| W
  W -->|no| P
  W -->|yes| PW
  Z -->|yes| B
  E -->|yes| B
  F -->|yes| B
  N -->|"yes, unless<br/>DQ_ALLOW_NOT_EVALUATED"| B
  PW -. "DQ_FAIL_ON_WARN=true" .-> B
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  linkStyle 5 stroke:#16a34a,stroke-width:2px
  linkStyle 6 stroke:#4f46e5,stroke-width:1.5px
  linkStyle 7,8,9,10,11 stroke:#dc2626,stroke-width:1.5px
```
<!-- /diagram -->

**How to read it:**

- Questions are asked top to bottom; any *yes* on a red path **blocks** (exit code 1).
- The gate **fails closed**: no contracts, an execution error or an unevaluated check all block by default.
- The only overrides are `DQ_ALLOW_NOT_EVALUATED` and `DQ_FAIL_ON_WARN` (plus `allow_empty`, in code only).
- The policy evaluates every rule and reports **all** reasons, not just the first one.

| Situation | Default | Override |
|---|---|---|
| A check **failed** | block | none |
| A contract raised an **execution error** (SQL error, missing table) | block | none (fail closed) |
| A check is **not evaluated** (e.g. a metric could not be computed) | block | `DQ_ALLOW_NOT_EVALUATED=true` |
| A check **warned** | pass with warnings | `DQ_FAIL_ON_WARN=true` blocks |
| The layer has **no contracts** | block | `allow_empty` in code only |

Reasons are collected in a stable order (errors, failures, not evaluated, warnings) and printed, written to
every report and sent to notifiers.

## Gate run sequence

What happens, step by step, when one layer is gated (here: silver).

<!-- diagram:gate-run-sequence -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Gate run sequence
%% Purpose: What happens, in order, when one layer is gated
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
sequenceDiagram
  autonumber
  participant U as Airflow task
  participant C as soda-dq CLI
  participant V as VerifyLayerUseCase
  participant R as FileContractRepository
  participant S as SodaContractVerifier
  participant L as Lakekeeper (Iceberg)
  participant P as GatePolicy
  participant K as Sinks and notifiers
  U->>C: soda-dq verify silver
  C->>+V: execute(retail, silver)
  V->>R: list_contracts(retail, silver)
  R-->>V: ContractSpec list
  V->>+S: verify(contracts)
  S->>L: open DuckDB or Trino, read tables
  L-->>S: rows aggregated by SQL
  S-->>-V: ContractResult list
  V->>P: decide(results)
  Note over P: Fail closed: an error or an<br/>unevaluated check also blocks
  P-->>V: GateDecision
  V->>K: write(report), notify(report)
  V-->>-C: GateReport
  alt blocked
    C-->>U: exit 1, task fails, downstream skipped
  else passed or passed with warnings
    C-->>U: exit 0, next layer runs
  end
```
<!-- /diagram -->

**How to read it:**

- Numbers give the order of calls. Solid arrows are calls; dashed arrows are returns.
- Only `SodaContractVerifier` imports Soda, and only the data-source provider knows the engine (DuckDB or Trino).
- The note marks the key decision: the gate fails closed.
- The `alt` box shows the two exits: 1 (blocked) and 0 (passed, with or without warnings).

Soda issues one metadata query (schema) and one batched aggregate query per dataset, plus extra queries for
reference, failed-rows and SQL-metric checks. Measured per layer: bronze 4, silver 5, gold 3 queries
(identical on DuckDB and Trino; Trino's own query log confirms the counts, the [benchmarks](../benchmarks/results.md)).

## Airflow DAG `lakehouse_retail_medallion_dq`

The tasks of the gated DAG and how a failed gate stops the run.

<!-- diagram:airflow-dag -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Airflow DAG lakehouse_retail_medallion_dq
%% Purpose: Spark tasks and gate tasks in the gated DAG
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
flowchart LR
  subgraph LOAD["Load"]
    direction TB
    ST[["land_smoke"]]:::node
    BR[["bronze"]]:::node
    IN[["inject_bad_batch<br/><small>no-op by default</small>"]]:::node
    ST --> BR --> IN
  end
  subgraph QB["Bronze gate"]
    direction TB
    DB("dq_bronze"):::ours
    F1(["Run failed"]):::blocked
    DB -. exit 1 .-> F1
  end
  subgraph QS["Silver"]
    direction TB
    SI[["silver"]]:::node
    DS("dq_silver"):::ours
    F2(["Run failed"]):::blocked
    SI --> DS
    DS -. exit 1 .-> F2
  end
  subgraph QG["Gold"]
    direction TB
    GO[["gold"]]:::node
    DG("dq_gold"):::ours
    F3(["Run failed"]):::blocked
    GO --> DG
    DG -. exit 1 .-> F3
  end
  EN(["query_timetravel"]):::pass
  LOAD --> QB
  QB -->|exit 0| QS
  QS -->|exit 0| QG
  QG -->|exit 0| EN
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  style LOAD fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style QB fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style QS fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style QG fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  linkStyle 2,4,6 stroke:#dc2626,stroke-width:1.5px
  linkStyle 8,9,10 stroke:#16a34a,stroke-width:2px
```
<!-- /diagram -->

**How to read it:**

- White double-edged boxes are Spark tasks; indigo boxes are gate tasks (`docker exec ldl-soda soda-dq verify <layer>`).
- Exit 0 lets the next group run; exit 1 fails the task and every downstream task becomes `upstream_failed`.
- Gate tasks have `retries=0`: data problems do not fix themselves.
- `inject_bad_batch` does nothing unless the run is triggered with `{"inject_bad_batch": true}`.

| Concern | Design |
|---|---|
| Gate task | `BashOperator`: `docker exec ldl-soda soda-dq verify <layer>`, through the lakehouse's Docker socket proxy (exec only) |
| Retries | `retries=0` on gates: data problems do not fix themselves. Spark tasks keep the lakehouse defaults |
| Failure propagation | Exit 1 fails the task; downstream tasks become `upstream_failed` (verified live, see [results](../results/README.md)) |
| Idempotency | Gates are read-only and safe to re-run. Spark layers are `CREATE OR REPLACE`, so a re-run rebuilds the layer from the one below |
| Re-run after a fix | Clear the failed `dq_<layer>` task (or the Spark task before it) in the Airflow UI |
| Backfills | The DAG is unscheduled (manual trigger, `catchup=False`), like the original. Backfills are re-runs of the whole medallion; per-partition backfills need WAP + partition-scoped contracts ([write-audit-publish](write-audit-publish.md)) |
| Demo fault injection | `inject_bad_batch` is a no-op unless the run conf has `inject_bad_batch: true`; the Jinja renders to the literal words `yes`/`no`, so no conf value reaches the shell |
| Concurrency | One DAG run at a time is assumed: layers are rebuilt in place |

---

[← Components](components.md) · [Architecture index](README.md) · [Deployment →](deployment.md)
