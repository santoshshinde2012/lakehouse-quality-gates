# Architecture

Every diagram here is a native Mermaid block that GitHub renders inline. The sources are `.mmd` files in
[`diagrams/`](diagrams/) and share one restrained theme, documented in [diagrams/THEME.md](diagrams/THEME.md):
light zones, indigo for this project, green for pass and red for blocked. Edit a source, then run
`make diagrams` to sync the copies below; `make diagrams-check` catches drift.

## 1. System architecture

<!-- diagram:01-system-architecture -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
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
      DQR[("dq_results<br/><small>Iceberg table</small>")]:::store
      SLK(["Slack alerts<br/><small>on block</small>"]):::actor
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

| Zone | What it is |
|---|---|
| Orchestration | Airflow 3 runs the gated DAG `lakehouse_retail_medallion_dq` (section 5) |
| Sources | `fixtures/good_batch` and `fixtures/bad_batch` CSVs, or the lakehouse's own sample data |
| Lakehouse platform | local-data-lakehouse: Spark jobs write Iceberg tables to RustFS (S3) through the Lakekeeper REST catalog (PostgreSQL-backed) |
| Quality gates | `soda-dq`: Soda Core 4.26 contracts (14 bronze, 21 silver and 11 gold checks), read through DuckDB or Trino, and `GatePolicy` decides pass or block |
| Outputs | JSON, JUnit and Markdown reports in `reports/`, the optional `ops.dq_results` Iceberg table, Slack alerts on block |

## 2. Code architecture (hexagonal)

<!-- diagram:02-code-architecture -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
flowchart LR
  subgraph IN["Inbound adapters"]
    direction TB
    CLI["soda-dq CLI"]:::node
    AIR["Airflow task"]:::node
    DOC["Docker runner"]:::node
    CLI ~~~ AIR ~~~ DOC
  end
  subgraph CORE["Application core"]
    direction TB
    subgraph APP["Application"]
      direction TB
      UC["Use cases"]:::ours
      POL["Gate policy"]:::ours
      UC ~~~ POL
    end
    subgraph DOM["Domain"]
      direction TB
      MOD["Models"]:::ours
      PRT["Ports"]:::ours
      MOD ~~~ PRT
    end
    APP -->|uses| DOM
  end
  subgraph OUTB["Outbound adapters"]
    direction TB
    VER["Soda verifier"]:::node
    ENG["DuckDB, Trino"]:::node
    SNK["Result sinks"]:::node
    NOT["Notifiers"]:::node
    VER ~~~ ENG ~~~ SNK ~~~ NOT
  end
  IN -->|call| CORE
  CORE -. ports implemented by .-> OUTB
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  style IN fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style OUTB fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style CORE fill:#f5f7ff,stroke:#a5b4fc,stroke-width:1.5px,color:#3730a3
  style APP fill:#ffffff,stroke:#a5b4fc,stroke-width:1px,color:#3730a3
  style DOM fill:#ffffff,stroke:#a5b4fc,stroke-width:1px,color:#3730a3
```
<!-- /diagram -->

| Zone | Contents (real names) |
|---|---|
| Inbound adapters | `soda-dq` Typer CLI (`cli.py`), the Airflow DAG `lakehouse_retail_medallion_dq`, the Docker runner `ldl-soda`. `container.py` (`build_container`) is the single composition root that wires the adapters into the use cases |
| Application | `RunMedallionUseCase`, `VerifyLayerUseCase`, `GatePolicy` → `GateDecision`, `raise_if_blocked` |
| Domain | Models: `Layer`, `Outcome`, `GateStatus`, `ContractSpec`, `CheckResult`, `ContractResult`, `GateReport`. Ports (`Protocol`): `ContractRepository`, `ContractVerifier`, `ResultSink`, `Notifier`, `Clock`. Errors: `DataQualityError` and subclasses. Standard library only |
| Outbound adapters | `SodaContractVerifier` with `DuckDBProvider` / `TrinoProvider`, `FileContractRepository`, sinks (`JsonFileSink`, `JUnitXmlSink`, `MarkdownSink`, `IcebergSink`), notifiers (`ConsoleNotifier`, `SlackWebhookNotifier`), `SystemClock` |

**The dependency rule.** Dependencies point inward. The domain has no third-party imports. The application
receives `ContractVerifier`, `ResultSink` and the other ports through its constructor (dependency inversion),
and `container.py` passes in the concrete adapters. Three cross-cutting concerns span every layer:
**observability** (structlog events and reports), **configuration** (pydantic-settings, `DQ_*`) and
**security** (vended S3 credentials, a non-root read-only container, `SecretStr` for the Slack URL).

## 3. Medallion flow with quality gates

<!-- diagram:03-medallion-flow -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
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

## 4. Deployment: Docker network `ldl-net`

<!-- diagram:04-deployment -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
flowchart LR
  subgraph HOST["Host"]
    direction TB
    BRW(["Browser"]):::actor
    CLI(["soda-dq CLI"]):::actor
    BRW ~~~ CLI
  end
  subgraph NET["Docker network ldl-net"]
    direction LR
    subgraph ORCH["Orchestration"]
      direction TB
      AF["Airflow :8090"]:::node
      PX["Docker proxy"]:::node
      AF --> PX
    end
    subgraph RUN["Compute"]
      direction TB
      SP[["ldl-spark"]]:::node
      SO[["ldl-soda"]]:::ours
      TR[["ldl-trino :8088"]]:::node
      SP ~~~ SO ~~~ TR
    end
    subgraph STORE["Catalog and storage"]
      direction TB
      LK["Lakekeeper :8181"]:::node
      PG[("PostgreSQL")]:::store
      OS[("RustFS :9000")]:::store
      LK ~~~ PG ~~~ OS
    end
    ORCH -->|docker exec| RUN
    RUN -->|Iceberg REST + S3| STORE
  end
  HOST -->|published ports| NET
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  style HOST fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style NET fill:#ffffff,stroke:#94a3b8,stroke-width:1px,stroke-dasharray:4 4,color:#334155
  style ORCH fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style RUN fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style STORE fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
```
<!-- /diagram -->

| Container | Host port | Role |
|---|---|---|
| Airflow api-server, scheduler, dag-processor, docker-proxy | 8090 (`AIRFLOW_API_PORT`) | Runs the DAG; `docker exec` goes through the proxy |
| `ldl-spark` | n/a | bronze, silver and gold jobs |
| `ldl-soda` | n/a | `lakehouse-quality-gates:0.2.3`: non-root, read-only root filesystem, `cap_drop: ALL` |
| `ldl-trino` | 8088 | Optional engine (full profile) |
| `ldl-lakekeeper` | 8181 | Iceberg REST catalog |
| `ldl-postgres` | n/a | Catalog database |
| `ldl-objectstore` | 9000 | RustFS, `s3://lake/warehouse` |

## 5. Airflow DAG `lakehouse_retail_medallion_dq`

<!-- diagram:06-airflow-dag -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
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

`inject_bad_batch` is a no-op unless the run is triggered with `{"inject_bad_batch": true}`
(`make airflow-dq-trigger-bad`). Then `dq_bronze` exits 1 and every downstream task is `upstream_failed`.

## 6. Gate run sequence

<!-- diagram:05-gate-sequence -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
sequenceDiagram
  autonumber
  participant U as Airflow task / engineer
  participant C as soda-dq CLI
  participant V as VerifyLayerUseCase
  participant R as FileContractRepository
  participant S as SodaContractVerifier
  participant D as DuckDBProvider
  participant L as Lakekeeper (Iceberg)
  participant P as GatePolicy
  participant K as Sinks, Notifiers
  rect rgb(248, 250, 252)
    U->>C: soda-dq verify silver
    C->>+V: execute(retail, silver, variables)
    V->>R: list_contracts(retail, silver)
    R-->>V: ContractSpec[]
    V->>+S: verify(contracts, variables)
    S->>D: open()
    D->>L: ATTACH catalog
    S->>S: verify_contract_locally()
    S-->>-V: ContractResult[]
    V->>P: decide(results)
    P-->>V: GateDecision
    V->>K: write(report), notify(report)
    V-->>-C: GateReport
    alt blocked
      rect rgb(254, 242, 242)
        C-->>U: exit 1, task fails, downstream skipped
      end
    else passed or warned
      rect rgb(240, 253, 244)
        C-->>U: exit 0, promote layer
      end
    end
  end
```
<!-- /diagram -->

## 7. Domain model and ports

<!-- diagram:07-domain-ports -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
classDiagram
  direction LR
  namespace domain {
    class ContractSpec {
      +pipeline
      +layer
      +name
      +dataset
      +path
      +datasets()
    }
    class CheckResult {
      +name
      +outcome
      +details
    }
    class ContractResult {
      +contract
      +checks
      +outcome
    }
    class GateReport {
      +run_id
      +layer
      +decision
      +results
    }
    class ContractRepository {
      <<Protocol>>
      +list_contracts(pipeline, layer)
    }
    class ContractVerifier {
      <<Protocol>>
      +verify(contracts, variables)
    }
    class ResultSink {
      <<Protocol>>
      +write(report)
    }
    class Notifier {
      <<Protocol>>
      +notify(report)
    }
    class Clock {
      <<Protocol>>
      +now()
    }
  }
  namespace application {
    class GateDecision {
      +status
      +reasons
    }
    class GatePolicy {
      +fail_on_warn
      +decide(results) GateDecision
    }
    class VerifyLayerUseCase {
      +execute(pipeline, layer, variables) GateReport
    }
  }
  namespace adapters {
    class SodaContractVerifier
    class DataSourceProvider {
      <<Protocol>>
      +open() SodaDataSources
    }
    class DuckDBProvider
    class TrinoProvider
    class FileContractRepository
    class JsonFileSink
    class SlackWebhookNotifier
  }
  ContractResult o-- CheckResult
  ContractResult --> ContractSpec
  GateReport o-- ContractResult
  GatePolicy ..> GateDecision
  VerifyLayerUseCase --> ContractRepository
  VerifyLayerUseCase --> ContractVerifier
  VerifyLayerUseCase --> GatePolicy
  VerifyLayerUseCase --> ResultSink
  VerifyLayerUseCase --> Notifier
  VerifyLayerUseCase --> Clock
  SodaContractVerifier ..|> ContractVerifier
  SodaContractVerifier --> DataSourceProvider
  DuckDBProvider ..|> DataSourceProvider
  TrinoProvider ..|> DataSourceProvider
  FileContractRepository ..|> ContractRepository
  JsonFileSink ..|> ResultSink
  SlackWebhookNotifier ..|> Notifier
  style ContractSpec fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style CheckResult fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style ContractResult fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style GateReport fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style ContractRepository fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style ContractVerifier fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style ResultSink fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style Notifier fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style Clock fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style GateDecision fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style GatePolicy fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style VerifyLayerUseCase fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b
  style SodaContractVerifier fill:#ffffff,stroke:#94a3b8,color:#0f172a
  style DataSourceProvider fill:#ffffff,stroke:#94a3b8,color:#0f172a
  style DuckDBProvider fill:#ffffff,stroke:#94a3b8,color:#0f172a
  style TrinoProvider fill:#ffffff,stroke:#94a3b8,color:#0f172a
  style FileContractRepository fill:#ffffff,stroke:#94a3b8,color:#0f172a
  style JsonFileSink fill:#ffffff,stroke:#94a3b8,color:#0f172a
  style SlackWebhookNotifier fill:#ffffff,stroke:#94a3b8,color:#0f172a
```
<!-- /diagram -->

## Layering rules

| Layer | Package | May import |
|---|---|---|
| Domain | `domain/` (models, ports, errors) | stdlib only |
| Application | `application/` | domain, structlog |
| Adapters | `adapters/` | domain, Soda, DuckDB, PyYAML, PyIceberg (lazily) |
| Composition | `container.py`, `cli.py`, `settings.py` | everything. This is the only place that instantiates concrete classes |

How SOLID applies here:
- **S**ingle responsibility. The policy decides, the use case orchestrates, sinks persist, notifiers
  notify, the verifier only translates between Soda and the domain, and the repository only renders
  contracts.
- **O**pen/closed. A new sink, notifier or engine is a new adapter plus one line in `container.py`. The
  use cases don't change.
- **L**iskov. Every fake in `tests/conftest.py` stands in for its real adapter, and `isinstance`
  checks against the `runtime_checkable` ports confirm the shapes match.
- **I**nterface segregation. Each port has one method; no adapter implements methods it doesn't use.
- **D**ependency inversion. The application depends on `Protocol`s, and Soda types never leave
  `adapters/soda_verifier.py`.

## Engines

| `DQ_ENGINE` | Data source | Dataset name | When to use |
|---|---|---|---|
| `duckdb` (default) | `lakehouse_client.duckdb_connect()` → Lakekeeper, Soda `from_existing_cursor` | `lakehouse_duckdb/<schema>/<table>` | Light or full profile. No JVM |
| `duckdb-local` | In-memory DuckDB built from CSVs by the same SQL as `sql/retail/*.sql` | same | CI, demos, writing contracts |
| `trino` | `datasources/trino.yml` (soda-trino) | `lakehouse_trino/lakehouse/<schema>/<table>` | Full profile with `TRINO=1` |

Soda's DuckDB dialect has a single namespace level (the schema), so the provider runs `USE lakehouse`. After
that, `silver.orders` resolves inside the attached Iceberg catalog, and Soda's `information_schema` lookups
(used by the schema check) match on schema name. See ADR 0002.
