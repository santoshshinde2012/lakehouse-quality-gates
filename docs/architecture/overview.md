# Architecture overview

> [!NOTE]
> **At a glance.** `soda-dq` runs Soda Core v4 contracts against the Iceberg tables right after each layer is built
> and decides **pass** or **block**. Airflow runs it as a task between the Spark jobs. It reads the lakehouse; it
> changes none of the lakehouse's files.

## Context

### System context

lakehouse-quality-gates and the people and systems around it.

<!-- diagram:system-context -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: System context
%% Purpose: Who and what interacts with lakehouse-quality-gates
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
flowchart LR
  ENG(["Data engineer<br/><small>writes contracts, triages</small>"]):::actor
  USR(["Data consumers<br/><small>dashboards, SQL</small>"]):::actor
  subgraph CTX["lakehouse-quality-gates"]
    direction TB
    DQ["soda-dq<br/><small>Soda contracts + gate policy</small>"]:::ours
  end
  AF["Airflow 3<br/><small>gated DAG</small>"]:::node
  LH[("local-data-lakehouse<br/><small>Spark, Iceberg, Lakekeeper, Trino</small>")]:::store
  SL(["Slack<br/><small>alerts</small>"]):::actor
  CI["GitHub Actions<br/><small>lint, tests, demo gates</small>"]:::node
  ENG -->|edits contracts| DQ
  AF -->|1. run Spark job| LH
  AF -->|2. run gate| DQ
  DQ -->|reads tables| LH
  DQ -->|warn or block| SL
  CI -->|validates on push| DQ
  USR -->|query gold| LH
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  style CTX fill:#f5f7ff,stroke:#a5b4fc,stroke-width:1.5px,color:#3730a3
```
<!-- /diagram -->

**How to read it:**

- The indigo box is this project; everything else is external.
- Arrows point from the caller to the system it uses; the label says what for.
- Airflow does two things per layer, in order: **1.** run the Spark job, **2.** run the gate.
- Consumers only ever read gold; the gate is what decides whether new gold gets built.

## System architecture

Where the quality gates sit between the lakehouse layers, and what every gate run produces.

<!-- diagram:system-architecture -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: System architecture
%% Purpose: Where the quality gates sit between the lakehouse layers and what they produce
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
flowchart TB
  subgraph ORCH["Orchestration"]
    direction LR
    AF(["Airflow 3<br/><small>gated DAG</small>"]):::actor
  end
  subgraph FLOW["Data pipeline"]
    direction LR
    subgraph SRC["Sources"]
      direction TB
      CSV[("Good batch<br/><small>clean sample CSV</small>")]:::store
      BAD[("Bad batch<br/><small>injected defects</small>")]:::store
      CSV ~~~ BAD
    end
    subgraph LAKE["Lakehouse platform"]
      direction TB
      JOB[["Spark jobs<br/><small>medallion transforms</small>"]]:::node
      TBL[("Iceberg tables<br/><small>medallion on RustFS</small>")]:::store
      CAT["Lakekeeper<br/><small>REST catalog</small>"]:::node
      JOB -->|write| TBL
      TBL --- CAT
    end
    subgraph DQ["Quality gates"]
      direction TB
      SODA["Soda contracts<br/><small>46 checks</small>"]:::ours
      GATE("Gate decision<br/><small>pass or block</small>"):::ours
      BLK(["Run blocked<br/><small>downstream skipped</small>"]):::blocked
      SODA --> GATE
      GATE -. fail .-> BLK
    end
    subgraph OUT["Outputs"]
      direction TB
      REP[("Gate reports<br/><small>JSON, JUnit, MD</small>")]:::store
      DQR[("ops.dq_results<br/><small>Iceberg table, optional</small>")]:::store
      SLK(["Slack alerts<br/><small>on warn or block</small>"]):::actor
      REP ~~~ DQR ~~~ SLK
    end
    SRC -->|ingest| LAKE
    LAKE -->|read| DQ
    DQ -->|publish| OUT
  end
  ORCH -. schedules every step .-> FLOW
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  style ORCH fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style FLOW fill:#ffffff,stroke:#e2e8f0,stroke-width:1px,stroke-dasharray:4 4,color:#64748b
  style SRC fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style LAKE fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style DQ fill:#f5f7ff,stroke:#a5b4fc,stroke-width:1.5px,color:#3730a3
  style OUT fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  linkStyle 4 stroke:#dc2626,stroke-width:1.5px
```
<!-- /diagram -->

**How to read it:**

- Read left to right, following the data: **Sources → Lakehouse platform → Quality gates → Outputs**.
- Indigo is this project (`soda-dq`); grey zones belong to `local-data-lakehouse` or are outputs.
- The red dotted path is a **block**: the gate fails, the Airflow task fails, nothing downstream runs.
- Airflow (top) schedules every step: the Spark job of a layer, then that layer's gate.

| Zone | Responsibility |
|---|---|
| Orchestration | Airflow 3 runs `lakehouse_retail_medallion_dq`: a Spark task per layer, then a gate task |
| Sources | CSV batches (`fixtures/good_batch`, `fixtures/bad_batch`, or generated at scale by `scripts/gen_scale_data.py`) |
| Lakehouse platform | `local-data-lakehouse`: Spark 4.1 writes Iceberg 1.12 tables to RustFS (S3) through the Lakekeeper REST catalog |
| Quality gates | `soda-dq`: renders contracts, verifies them through an engine, applies the gate policy |
| Outputs | Reports per gate, optional `ops.dq_results` Iceberg table, Slack alerts, Airflow task state |

## Goals and non-goals

The goals (G1 to G7), how each is measured, and what is out of scope are in
[system design § 2](../system-design.md#2-problem-statement-goals-and-non-goals).

---

[← Docs hub](../README.md) · [Architecture index](README.md) · [Components →](components.md)
