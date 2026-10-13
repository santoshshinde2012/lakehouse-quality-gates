# Architecture

Every diagram here is a native Mermaid block that GitHub renders inline. The sources are in
[`diagrams/`](diagrams/) as `.mmd` files and share one theme, documented in
[diagrams/THEME.md](diagrams/THEME.md): one accent per layer (sky entry points, slate composition root,
indigo application, violet domain, teal adapters, amber data platform), green for pass and red for blocked.
Edit a source, then run `make diagrams` to sync the copies below; `make diagrams-check` catches drift.

## 1. Layered reference architecture

<!-- diagram:01-layered-architecture -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
flowchart TB
  subgraph ENTRY["1 · Entry points"]
    direction LR
    E1["soda-dq CLI<br/><small>cli.py · Typer</small>"]:::entry
    E2["Airflow DAG<br/><small>lakehouse_retail_medallion_dq</small>"]:::entry
    E3["Docker runner<br/><small>ldl-soda</small>"]:::entry
    E4["Makefile · CI<br/><small>make verify / demo</small>"]:::entry
    E1 ~~~ E2 ~~~ E3 ~~~ E4
  end
  subgraph ROOT["Composition root · container.py"]
    direction LR
    R1["Settings<br/><small>pydantic-settings · DQ_*</small>"]:::root
    R2["build_container()"]:::root
    R3["build_provider() · build_sinks()<br/>build_notifiers()"]:::root
    R1 ~~~ R2 ~~~ R3
  end
  subgraph APP["2 · Application"]
    direction LR
    A1["RunMedallionUseCase"]:::app
    A2["VerifyLayerUseCase"]:::app
    A3["GatePolicy → GateDecision"]:::app
    A4["raise_if_blocked()"]:::app
    A1 ~~~ A2 ~~~ A3 ~~~ A4
  end
  subgraph DOM["3 · Domain · stdlib only"]
    direction LR
    D1["Models<br/><small>ContractSpec · CheckResult<br/>ContractResult · GateReport</small>"]:::domain
    D2["Ports «Protocol»<br/><small>ContractRepository · ContractVerifier<br/>ResultSink · Notifier · Clock</small>"]:::domain
    D3["Errors<br/><small>GateBlockedError · DataSourceError</small>"]:::domain
    D1 ~~~ D2 ~~~ D3
  end
  subgraph ADP["4 · Adapters · implement the ports"]
    direction LR
    P1["SodaContractVerifier<br/><small>DuckDBProvider · TrinoProvider</small>"]:::adapter
    P2["FileContractRepository"]:::adapter
    P3["Sinks<br/><small>JsonFileSink · JUnitXmlSink<br/>MarkdownSink · IcebergSink</small>"]:::adapter
    P4["Notifiers<br/><small>ConsoleNotifier · SlackWebhookNotifier</small>"]:::adapter
    P5["SystemClock"]:::adapter
    P1 ~~~ P2 ~~~ P3 ~~~ P4 ~~~ P5
  end
  subgraph PLAT["5 · Data platform · local-data-lakehouse"]
    direction LR
    L1["Soda Core 4.26"]:::platform
    L2["DuckDB · Trino"]:::platform
    L3["Lakekeeper<br/><small>Iceberg REST</small>"]:::platform
    L4[("RustFS S3<br/><small>bronze · silver · gold</small>")]:::platform
    L5["Slack · reports/"]:::platform
    L1 ~~~ L2 ~~~ L3 ~~~ L4 ~~~ L5
  end
  subgraph XC["Cross-cutting · every layer"]
    direction LR
    X1["Observability<br/><small>structlog · JSON / JUnit / Markdown</small>"]:::xcut
    X2["Configuration<br/><small>Settings · .env · DQ_*</small>"]:::xcut
    X3["Security<br/><small>non-root read-only image · SecretStr</small>"]:::xcut
    X1 ~~~ X2 ~~~ X3
  end
  ENTRY ==>|calls| ROOT
  ROOT ==>|wires use cases| APP
  APP ==>|depends on models + ports| DOM
  DOM ==>|ports implemented by| ADP
  ADP ==>|talks to| PLAT
  PLAT ~~~ XC
  classDef entry fill:#e0f2fe,stroke:#0284c7,stroke-width:1.5px,color:#0c4a6e
  classDef root fill:#e2e8f0,stroke:#475569,stroke-width:1.5px,color:#0f172a
  classDef app fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef domain fill:#ede9fe,stroke:#7c3aed,stroke-width:1.5px,color:#2e1065
  classDef adapter fill:#ccfbf1,stroke:#0d9488,stroke-width:1.5px,color:#134e4a
  classDef platform fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#451a03
  classDef xcut fill:#f1f5f9,stroke:#64748b,stroke-width:1.5px,stroke-dasharray:4 3,color:#1e293b
  classDef pass fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  classDef fail fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#7f1d1d
  style ENTRY fill:#f0f9ff,stroke:#0284c7,stroke-width:1.5px,color:#0c4a6e
  style ROOT fill:#f8fafc,stroke:#475569,stroke-width:1.5px,color:#0f172a
  style APP fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  style DOM fill:#f5f3ff,stroke:#7c3aed,stroke-width:1.5px,color:#2e1065
  style ADP fill:#f0fdfa,stroke:#0d9488,stroke-width:1.5px,color:#134e4a
  style PLAT fill:#fffbeb,stroke:#d97706,stroke-width:1.5px,color:#451a03
  style XC fill:#f8fafc,stroke:#64748b,stroke-width:1.5px,stroke-dasharray:6 4,color:#1e293b
  linkStyle 20,21,22,23,24 stroke:#475569,stroke-width:2px
```
<!-- /diagram -->

| # | Layer | Contents (real names) | May depend on |
|---|---|---|---|
| 1 | **Entry points** | `soda-dq` Typer CLI (`cli.py`), Airflow DAG `lakehouse_retail_medallion_dq` (`airflow/dags/`), Docker runner `ldl-soda` | the application layer, through `container.py` |
| – | **Composition root** | `container.py`: `build_container`, `build_provider`, `build_sinks`, `build_notifiers` | every layer. It is the only place that instantiates adapters |
| 2 | **Application** | `RunMedallionUseCase`, `VerifyLayerUseCase`, `GatePolicy` → `GateDecision` | domain |
| 3 | **Domain** | models (`Layer`, `Outcome`, `GateStatus`, `ContractSpec`, `CheckResult`, `ContractResult`, `GateReport`), ports (`ContractRepository`, `ContractVerifier`, `ResultSink`, `Notifier`, `Clock`), errors (`DataQualityError` + subclasses) | the standard library only |
| 4 | **Adapters** | `SodaContractVerifier`, `FileContractRepository`, `DuckDBProvider` / `TrinoProvider`, sinks (`JsonFileSink`, `JUnitXmlSink`, `MarkdownSink`, `IcebergSink`), notifiers (`ConsoleNotifier`, `SlackWebhookNotifier`), `SystemClock` | domain (they implement its ports), plus third-party libraries |
| 5 | **Data platform** | local-data-lakehouse: DuckDB, Trino and Spark, the Lakekeeper REST catalog, Iceberg tables (bronze, silver, gold), RustFS (S3), PostgreSQL | none (external systems) |

**The dependency rule.** Source code only depends on layers below it, and the domain has no
third-party imports. The application layer never imports Soda, DuckDB or Slack: it receives
`ContractVerifier`, `ResultSink` and the other ports through its constructor (dependency inversion), and
`container.py` passes in the concrete adapters. Three cross-cutting concerns span every layer:
**observability** (structlog events and reports), **configuration** (pydantic-settings, `DQ_*`) and
**security** (vended S3 credentials, a non-root read-only container, `SecretStr` for the Slack URL).

## 2. Medallion data flow with quality gates

<!-- diagram:02-medallion-flow -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
flowchart LR
  SRC[("Raw CSV<br/><small>good or bad batch</small>")]:::platform
  subgraph BRZ["Bronze"]
    direction TB
    B["bronze.orders<br/>bronze.customers"]:::platform
    GB{{"Bronze gate<br/><small>14 checks</small>"}}:::app
  end
  subgraph SLV["Silver"]
    direction TB
    S["silver.orders<br/>silver.customers"]:::platform
    GS{{"Silver gate<br/><small>21 checks</small>"}}:::app
  end
  subgraph GLD["Gold"]
    direction TB
    G["gold.daily_order_metrics"]:::platform
    GG{{"Gold gate<br/><small>11 checks</small>"}}:::app
  end
  OK(["Published<br/><small>BI · time travel</small>"]):::pass
  X(["Blocked<br/><small>exit 1 · task fails · Slack alert<br/>downstream not run</small>"]):::fail
  SRC -->|ingest| B
  B --> GB
  GB -->|pass| S
  S --> GS
  GS -->|pass| G
  G --> GG
  GG -->|pass| OK
  GB -->|fail| X
  GS -->|fail| X
  GG -->|fail| X
  classDef entry fill:#e0f2fe,stroke:#0284c7,stroke-width:1.5px,color:#0c4a6e
  classDef root fill:#e2e8f0,stroke:#475569,stroke-width:1.5px,color:#0f172a
  classDef app fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef domain fill:#ede9fe,stroke:#7c3aed,stroke-width:1.5px,color:#2e1065
  classDef adapter fill:#ccfbf1,stroke:#0d9488,stroke-width:1.5px,color:#134e4a
  classDef platform fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#451a03
  classDef xcut fill:#f1f5f9,stroke:#64748b,stroke-width:1.5px,stroke-dasharray:4 3,color:#1e293b
  classDef pass fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  classDef fail fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#7f1d1d
  style BRZ fill:#fdf4ec,stroke:#b45309,stroke-width:1.5px,color:#451a03
  style SLV fill:#f1f5f9,stroke:#64748b,stroke-width:1.5px,color:#0f172a
  style GLD fill:#fefce8,stroke:#ca8a04,stroke-width:1.5px,color:#422006
  linkStyle 2,4,6 stroke:#16a34a,stroke-width:2.5px
  linkStyle 7,8,9 stroke:#dc2626,stroke-width:2px,stroke-dasharray:6 4
```
<!-- /diagram -->

## 3. Deployment: Docker network `ldl-net`

<!-- diagram:03-deployment -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
flowchart TB
  subgraph HOST["Developer machine · Docker Desktop"]
    direction TB
    subgraph CLIENTS["Host clients"]
      direction LR
      BRW["Browser<br/><small>localhost:8090 · AIRFLOW_API_PORT</small>"]:::entry
      VENV["SODA/.venv · soda-dq<br/><small>localhost:8181 · localhost:9000</small>"]:::entry
    end
    subgraph NET["Docker network · ldl-net"]
      direction TB
      subgraph ORCH["Orchestration"]
        direction LR
        AF["airflow api-server :8090<br/><small>scheduler · dag-processor</small>"]:::app
        DP["docker-proxy<br/><small>exec only</small>"]:::root
      end
      subgraph RUN["Compute"]
        direction LR
        SP["ldl-spark<br/><small>bronze · silver · gold jobs</small>"]:::platform
        SO["ldl-soda<br/><small>lakehouse-quality-gates:0.2.2<br/>non-root · read-only · cap_drop ALL</small>"]:::adapter
        TR["ldl-trino :8088<br/><small>full profile</small>"]:::platform
      end
      subgraph STORE["Catalog and storage"]
        direction LR
        LK["ldl-lakekeeper :8181<br/><small>Iceberg REST</small>"]:::domain
        PG[("ldl-postgres<br/><small>catalog DB</small>")]:::domain
        OS[("ldl-objectstore :9000<br/><small>RustFS · s3://lake</small>")]:::domain
      end
    end
  end
  BRW --> AF
  AF -->|docker exec| DP
  DP --> SP
  DP --> SO
  SP -->|write Iceberg| LK
  SO -->|read Iceberg| LK
  SO -. "engine=trino" .-> TR
  TR --> LK
  VENV -->|REST + S3| LK
  LK --> PG
  LK --> OS
  classDef entry fill:#e0f2fe,stroke:#0284c7,stroke-width:1.5px,color:#0c4a6e
  classDef root fill:#e2e8f0,stroke:#475569,stroke-width:1.5px,color:#0f172a
  classDef app fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef domain fill:#ede9fe,stroke:#7c3aed,stroke-width:1.5px,color:#2e1065
  classDef adapter fill:#ccfbf1,stroke:#0d9488,stroke-width:1.5px,color:#134e4a
  classDef platform fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#451a03
  classDef xcut fill:#f1f5f9,stroke:#64748b,stroke-width:1.5px,stroke-dasharray:4 3,color:#1e293b
  classDef pass fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  classDef fail fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#7f1d1d
  style HOST fill:#ffffff,stroke:#94a3b8,stroke-width:1.5px,color:#0f172a
  style CLIENTS fill:#f0f9ff,stroke:#0284c7,stroke-width:1px,color:#0c4a6e
  style NET fill:#f8fafc,stroke:#475569,stroke-width:1.5px,stroke-dasharray:6 4,color:#0f172a
  style ORCH fill:#eef2ff,stroke:#4f46e5,stroke-width:1px,color:#1e1b4b
  style RUN fill:#fffbeb,stroke:#d97706,stroke-width:1px,color:#451a03
  style STORE fill:#f5f3ff,stroke:#7c3aed,stroke-width:1px,color:#2e1065
```
<!-- /diagram -->

Host ports were verified on macOS. Airflow's port is set by `AIRFLOW_API_PORT` (8090 in the runbook).

## 4. Gate run sequence

<!-- diagram:04-gate-sequence -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
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
  participant K as Sinks · Notifiers
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
    V->>K: write(report) · notify(report)
    V-->>-C: GateReport
    alt blocked
      rect rgb(254, 226, 226)
        C-->>U: exit 1 · task fails, downstream skipped
      end
    else passed or warned
      rect rgb(220, 252, 231)
        C-->>U: exit 0 · promote layer
      end
    end
  end
```
<!-- /diagram -->

## 5. Airflow DAG `lakehouse_retail_medallion_dq`

<!-- diagram:05-airflow-dag -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
flowchart LR
  ST["land_smoke<br/><small>ldl-spark</small>"]:::platform
  BR["bronze"]:::platform
  IN["inject_bad_batch<br/><small>no-op unless<br/>conf inject_bad_batch=true</small>"]:::xcut
  DB{{"dq_bronze<br/><small>ldl-soda</small>"}}:::app
  SI["silver"]:::platform
  DS{{"dq_silver"}}:::app
  GO["gold"]:::platform
  DG{{"dq_gold"}}:::app
  EN(["query_timetravel"]):::pass
  FX(["DAG run failed<br/><small>downstream: upstream_failed</small>"]):::fail
  ST --> BR
  BR --> IN
  IN --> DB
  DB -->|exit 0| SI
  SI --> DS
  DS -->|exit 0| GO
  GO --> DG
  DG -->|exit 0| EN
  DB -->|exit 1| FX
  DS -->|exit 1| FX
  DG -->|exit 1| FX
  classDef entry fill:#e0f2fe,stroke:#0284c7,stroke-width:1.5px,color:#0c4a6e
  classDef root fill:#e2e8f0,stroke:#475569,stroke-width:1.5px,color:#0f172a
  classDef app fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef domain fill:#ede9fe,stroke:#7c3aed,stroke-width:1.5px,color:#2e1065
  classDef adapter fill:#ccfbf1,stroke:#0d9488,stroke-width:1.5px,color:#134e4a
  classDef platform fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#451a03
  classDef xcut fill:#f1f5f9,stroke:#64748b,stroke-width:1.5px,stroke-dasharray:4 3,color:#1e293b
  classDef pass fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  classDef fail fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#7f1d1d
  linkStyle 3,5,7 stroke:#16a34a,stroke-width:2.5px
  linkStyle 8,9,10 stroke:#dc2626,stroke-width:2px,stroke-dasharray:6 4
```
<!-- /diagram -->

`inject_bad_batch` is a no-op unless the run is triggered with `{"inject_bad_batch": true}`
(`make airflow-dq-trigger-bad`); then `dq_bronze` exits 1 and every downstream task is `upstream_failed`.

## 6. Domain model and ports

<!-- diagram:06-domain-ports -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
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
  style ContractSpec fill:#ede9fe,stroke:#7c3aed,color:#2e1065
  style CheckResult fill:#ede9fe,stroke:#7c3aed,color:#2e1065
  style ContractResult fill:#ede9fe,stroke:#7c3aed,color:#2e1065
  style GateReport fill:#ede9fe,stroke:#7c3aed,color:#2e1065
  style ContractRepository fill:#f5f3ff,stroke:#7c3aed,color:#2e1065
  style ContractVerifier fill:#f5f3ff,stroke:#7c3aed,color:#2e1065
  style ResultSink fill:#f5f3ff,stroke:#7c3aed,color:#2e1065
  style Notifier fill:#f5f3ff,stroke:#7c3aed,color:#2e1065
  style Clock fill:#f5f3ff,stroke:#7c3aed,color:#2e1065
  style GateDecision fill:#e0e7ff,stroke:#4f46e5,color:#1e1b4b
  style GatePolicy fill:#e0e7ff,stroke:#4f46e5,color:#1e1b4b
  style VerifyLayerUseCase fill:#e0e7ff,stroke:#4f46e5,color:#1e1b4b
  style SodaContractVerifier fill:#ccfbf1,stroke:#0d9488,color:#134e4a
  style DataSourceProvider fill:#ccfbf1,stroke:#0d9488,color:#134e4a
  style DuckDBProvider fill:#ccfbf1,stroke:#0d9488,color:#134e4a
  style TrinoProvider fill:#ccfbf1,stroke:#0d9488,color:#134e4a
  style FileContractRepository fill:#ccfbf1,stroke:#0d9488,color:#134e4a
  style JsonFileSink fill:#ccfbf1,stroke:#0d9488,color:#134e4a
  style SlackWebhookNotifier fill:#ccfbf1,stroke:#0d9488,color:#134e4a
```
<!-- /diagram -->

## 7. System context

<!-- diagram:07-system-context -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
flowchart LR
  ENG(["Data engineer"]):::entry
  subgraph DQ["lakehouse-quality-gates"]
    direction TB
    CLI["soda-dq<br/><small>Soda Core 4.26 data contracts</small>"]:::app
    REP[("reports/<br/><small>JSON · JUnit · Markdown</small>")]:::adapter
  end
  subgraph LDL["local-data-lakehouse · referenced by path, unchanged"]
    direction TB
    AF["Airflow 3"]:::platform
    SP["Spark jobs<br/><small>bronze · silver · gold</small>"]:::platform
    LK["Lakekeeper<br/><small>Iceberg REST catalog</small>"]:::platform
    TR["Trino<br/><small>optional engine</small>"]:::platform
    OS[("RustFS<br/><small>s3://lake/warehouse</small>")]:::platform
  end
  SL(["Slack"]):::entry
  ENG -->|make verify · seed| CLI
  AF -->|docker exec ldl-spark| SP
  AF -->|docker exec ldl-soda| CLI
  SP -->|writes Iceberg| LK
  CLI -->|DuckDB iceberg ext| LK
  CLI -. "engine=trino" .-> TR
  TR --> LK
  LK --> OS
  CLI --> REP
  CLI -. "webhook on block" .-> SL
  classDef entry fill:#e0f2fe,stroke:#0284c7,stroke-width:1.5px,color:#0c4a6e
  classDef root fill:#e2e8f0,stroke:#475569,stroke-width:1.5px,color:#0f172a
  classDef app fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef domain fill:#ede9fe,stroke:#7c3aed,stroke-width:1.5px,color:#2e1065
  classDef adapter fill:#ccfbf1,stroke:#0d9488,stroke-width:1.5px,color:#134e4a
  classDef platform fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#451a03
  classDef xcut fill:#f1f5f9,stroke:#64748b,stroke-width:1.5px,stroke-dasharray:4 3,color:#1e293b
  classDef pass fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  classDef fail fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#7f1d1d
  style DQ fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  style LDL fill:#fffbeb,stroke:#d97706,stroke-width:1.5px,color:#451a03
  linkStyle 9 stroke:#dc2626,stroke-width:1.5px,stroke-dasharray:6 4
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
