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
  direction LR
  BRONZE_CUSTOMERS_RAW ||--o| SILVER_CUSTOMERS : "cleaned into"
  BRONZE_ORDERS_RAW ||--o| SILVER_ORDERS : "cleaned into"
  SILVER_CUSTOMERS ||--o{ SILVER_ORDERS : "customer_id"
  SILVER_ORDERS }o--|| GOLD_DAILY_ORDER_METRICS : "aggregated by day"
  BRONZE_ORDERS_RAW {
    varchar order_id
    varchar customer_id
    timestamptz order_ts
    varchar status
    double amount
    varchar _source_file
    timestamptz _ingested_at
  }
  BRONZE_CUSTOMERS_RAW {
    varchar customer_id
    varchar name
    varchar email
    varchar city
    date signup_date
    varchar _source_file
    timestamptz _ingested_at
  }
  SILVER_ORDERS {
    varchar order_id PK
    varchar customer_id FK
    timestamptz order_ts
    varchar status
    double amount
    varchar _source_file
    timestamptz _ingested_at
  }
  SILVER_CUSTOMERS {
    varchar customer_id PK
    varchar name
    varchar email
    varchar city
    date signup_date
    varchar _source_file
    timestamptz _ingested_at
  }
  GOLD_DAILY_ORDER_METRICS {
    date order_date PK
    bigint orders
    double revenue
    double avg_order_value
    bigint cancelled_orders
    bigint refunded_orders
  }
  OPS_DQ_RESULTS {
    varchar run_id
    timestamptz created_at
    varchar pipeline
    varchar layer
    varchar engine
    varchar gate_status
    varchar contract
    varchar dataset
    varchar check_name
    varchar check_type
    varchar column_name
    varchar outcome
  }
```
<!-- /diagram -->

**How to read it:**

- Bronze keeps raw values; silver is cleaned, typed and deduplicated; both carry the lineage columns `_source_file` and `_ingested_at`. Gold is one row per day.
- Each bronze row becomes at most one silver row (`|o`). A crow's foot means *many*: one customer has many orders, and many orders aggregate into one day. `silver.orders.customer_id` must exist in `silver.customers` (a `valid_reference_data` check).
- Gold reconciles with silver: `SUM(orders)` must equal the silver row count.
- `OPS_DQ_RESULTS` (`ops.dq_results`) is written only with `DQ_ICEBERG_SINK=true`, one row per check.
- Columns and types are the ones the contracts' schema checks enforce.

## What each gate checks

One contract per dataset under `contracts/retail/<layer>/`: 14 checks in the bronze gate, 21 in silver and 11 in
gold. Bronze keeps raw values, so value problems **warn** there and are **enforced** in silver; structural problems
(schema drift, an empty batch, missing keys, duplicates within a file) fail at bronze. Every check, with its type
and level, is listed in the [contracts guide](../guides/contracts.md#checks-per-contract).

## Gate policy decision flow

How `GatePolicy` turns the check outcomes of one layer into *passed*, *passed with warnings* or *blocked*.

<!-- diagram:gate-policy -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Gate policy decision flow
%% Purpose: How GatePolicy turns check outcomes into passed, passed with warnings or blocked
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
flowchart LR
  IN(["Contract results<br/><small>one layer</small>"]):::actor
  Q{"Any blocking<br/>condition?"}:::node
  W{"Any check<br/>warned?"}:::node
  FW{"DQ_FAIL_ON_WARN<br/>= true?"}:::node
  B(["Blocked<br/><small>exit 1</small>"]):::blocked
  PW(["Passed with warnings<br/><small>exit 0</small>"]):::ours
  P(["Passed<br/><small>exit 0</small>"]):::pass
  C["<b>Blocking conditions</b><br/>no contracts verified<br/>a contract execution error<br/>a failed check<br/>a not-evaluated check<br/><small>unless DQ_ALLOW_NOT_EVALUATED=true</small>"]:::store
  IN --> Q
  Q -->|no| W
  W -->|no| P
  W -->|yes| FW
  FW -->|no| PW
  Q -->|yes| B
  FW -->|yes| B
  C -.- Q
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  linkStyle 2 stroke:#16a34a,stroke-width:2px
  linkStyle 4 stroke:#4f46e5,stroke-width:1.5px
  linkStyle 5,6 stroke:#dc2626,stroke-width:1.5px
```
<!-- /diagram -->

**How to read it:**

- Read left to right. Any of the blocking conditions in the grey box **blocks** (exit code 1).
- The gate **fails closed**: no contracts, an execution error or a not-evaluated check all block by default.
- Warnings pass unless `DQ_FAIL_ON_WARN=true`. The only other override is `DQ_ALLOW_NOT_EVALUATED=true` (plus `allow_empty`, in code only).
- The policy evaluates every rule and reports **all** reasons, not just the first one.

| Situation | Default | Override |
|---|---|---|
| A check **failed** | block | none |
| A contract raised an **execution error** (SQL error, missing table) | block | none (fail closed) |
| A check is **not evaluated** (e.g. a metric could not be computed) | block | `DQ_ALLOW_NOT_EVALUATED=true` |
| A check **warned** | passed with warnings | `DQ_FAIL_ON_WARN=true` blocks |
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
  Note over P: Fail closed: an error or a<br/>not-evaluated check also blocks
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
reference, failed-rows and SQL-metric checks. Measured per layer: bronze 4, silver 5, gold 3 queries, identical on
DuckDB and Trino and confirmed by Trino's own query log ([methodology](../benchmarks/methodology.md)).

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
  subgraph LOAD["Load and bronze"]
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
  subgraph QS["Silver build and gate"]
    direction TB
    SI[["silver"]]:::node
    DS("dq_silver"):::ours
    F2(["Run failed"]):::blocked
    SI --> DS
    DS -. exit 1 .-> F2
  end
  subgraph QG["Gold build and gate"]
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
