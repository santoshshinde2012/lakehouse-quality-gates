<div align="center">

# lakehouse-quality-gates

**Data contracts as quality gates for an open lakehouse: bad data never reaches gold.**

[Soda Core v4](https://docs.soda.io/) contracts run between bronze, silver and gold on Apache Iceberg,
using DuckDB or Trino, and are orchestrated by Airflow, for
[local-data-lakehouse](https://github.com/santoshshinde2012/local-data-lakehouse).

[![CI](https://github.com/santoshshinde2012/lakehouse-quality-gates/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/santoshshinde2012/lakehouse-quality-gates/actions/workflows/ci.yml)
[![Release](https://img.shields.io/badge/release-v0.2.2-2563eb)](https://github.com/santoshshinde2012/lakehouse-quality-gates/releases/tag/v0.2.2)
[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-16a34a.svg)](LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![mypy: strict](https://img.shields.io/badge/mypy-strict-2A6DB2)](https://mypy-lang.org/)
[![Coverage 99%](https://img.shields.io/badge/coverage-99%25-16a34a)](docs/results/README.md)
[![Soda Core 4.26](https://img.shields.io/badge/Soda%20Core-4.26-00B388)](https://docs.soda.io/)

[Why](#why) · [Features](#key-features) · [Architecture](#architecture) · [Quickstart](#quickstart) ·
[Usage](#usage) · [Configuration](#configuration) · [Results](#verified-results) · [Docs](#documentation) · [Roadmap](#roadmap)

</div>

---

## Why

**The problem.** In a medallion lakehouse, every layer trusts the one before it. A duplicated order, an
orphan customer id or a negative amount that lands in bronze is faithfully transformed into silver and
aggregated into gold, and the dashboards are wrong before anyone notices. Checks that run *after*
publishing only tell you about the damage.

**The solution.** Put a **gate** between the layers. Each layer has a declarative
[Soda data contract](docs/contracts.md) covering schema, completeness, validity, uniqueness, freshness,
referential integrity and reconciliation. The gate runs right after the layer is built. If the contract
fails, the Airflow task fails and the next layer is never built from bad data. Warnings are recorded but
do not block.

## Key features

- 🛡️ **5 contracts, 46 checks** over the retail medallion: schema, row count, freshness, missing,
  invalid (sets, regex, ranges, referential integrity), duplicate, `failed_rows` and a silver-to-gold
  `metric` reconciliation.
- ⚙️ **Three engines, one set of contracts.** DuckDB attaches to the Lakekeeper Iceberg REST catalog
  (default). Trino 483 is optional. `duckdb-local` builds the same medallion from CSV files with no Docker.
- 🚦 **Explicit gate policy.** A failure, an execution error or an unevaluated check blocks. A warning is
  logged. `DQ_FAIL_ON_WARN=true` makes warnings block too.
- 🧾 **Reports and alerts.** JSON, JUnit XML and Markdown for every run, an optional Iceberg table
  `ops.dq_results`, console output, and Slack alerts on problems.
- 🌀 **Airflow-native.** A gated DAG, `lakehouse_retail_medallion_dq`, sits next to the lakehouse's original
  DAG, which is left untouched. An `inject_bad_batch` switch shows a gate blocking for real.
- 🐳 **Hardened runner container.** Non-root, read-only root filesystem, `cap_drop: ALL`, digest-pinned base images.
- 🧱 **Clean architecture.** Ports and adapters with a single composition root. ruff, `mypy --strict`,
  100 tests at 99% coverage.

## Architecture

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

The system has five layers, and dependencies only point **downward**. The entry points (CLI, Airflow,
Docker) call use cases in the application layer, which work purely against domain models and ports.
Adapters implement those ports on top of Soda Core, DuckDB, Trino, files and Slack, and they reach the
Iceberg lakehouse underneath. `container.py` is the single composition root that wires everything together.

### Medallion flow with quality gates

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

### Deployment

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

More views are in **[docs/architecture.md](docs/architecture.md)**: gate run sequence, Airflow DAG, domain
model and ports, and system context. Every diagram is native Mermaid that GitHub renders inline; the
sources and the shared theme are in [`docs/diagrams/`](docs/diagrams/) ([THEME.md](docs/diagrams/THEME.md)).
Edit a `.mmd` file, then run `make diagrams` to sync the copies in the Markdown.

## Quickstart

No Docker is needed. Requires [uv](https://docs.astral.sh/uv/) and takes about a minute:

```bash
git clone https://github.com/santoshshinde2012/lakehouse-quality-gates.git && cd lakehouse-quality-gates
uv sync --all-extras            # Python 3.12 venv: Soda Core 4.26.0, DuckDB 1.5.6, dev tools
make validate                   # Soda parses every contract
make demo                       # good batch: all gates pass; bad batch: every gate blocks
open reports/*_retail_silver.md # what was caught, and why (use xdg-open on Linux)
```

<details>
<summary><b>Against the running lakehouse (DuckDB, Trino)</b></summary>

```bash
cp .env.example .env                             # set DQ_LDL_REPO_PATH to your local-data-lakehouse checkout
export DQ_LDL_REPO_PATH=../local-data-lakehouse  # same path, for the next line
(cd "$DQ_LDL_REPO_PATH" && make up-light && make demo-light)   # tables exist in Lakekeeper
DQ_ENGINE=duckdb uv run soda-dq run                            # host DuckDB -> Iceberg REST
# Trino (lakehouse: make up-full TRINO=1):
DQ_ENGINE=trino uv run soda-dq run
```
</details>

<details>
<summary><b>Docker runner and Airflow</b></summary>

```bash
(cd "$DQ_LDL_REPO_PATH" && make up-full)
make dq-up                                  # build lakehouse-quality-gates:0.2.2, start ldl-soda on ldl-net
make airflow-dq-up AIRFLOW_API_PORT=8090    # lakehouse Airflow + the gated DAG (default port 8080)
make airflow-dq-trigger                     # good data: every task succeeds
make airflow-dq-trigger-bad                 # inject_bad_batch: dq_bronze fails, downstream is upstream_failed
make dq-down && make airflow-dq-down        # stop; volumes are kept
```
</details>

## Usage

| Command | What it does |
|---|---|
| `soda-dq verify <layer> [-e ENGINE] [-p PIPELINE] [-s K=V]` | Gate one layer (`bronze`, `silver`, `gold`). Exit **0** passed, **1** blocked, **2** configuration error |
| `soda-dq run [-e ENGINE] [-p PIPELINE]` | Gate bronze → silver → gold, stopping at the first blocked layer |
| `soda-dq validate [-p PIPELINE]` | Check the syntax of every contract with Soda, without touching data |
| `soda-dq list [-e ENGINE]` | List contracts and their rendered dataset names |
| `soda-dq seed SAMPLE_DIR --yes` | Demo only: rebuild the live retail Iceberg tables from a CSV batch, e.g. `fixtures/bad_batch` |

`ENGINE` is `duckdb`, `duckdb-local` or `trino`. `--sample-dir` points `duckdb-local` at another CSV batch.
Run `make help` to list every Makefile target.

## Configuration

All settings are environment variables with the `DQ_` prefix (or entries in `.env`), validated by
pydantic-settings in [`settings.py`](src/soda_lakehouse_dq/settings.py).

| Variable | Default | Purpose |
|---|---|---|
| `DQ_ENGINE` | `duckdb` | `duckdb` (Lakekeeper via DuckDB), `duckdb-local` (CSV, no Docker), `trino` |
| `DQ_LDL_REPO_PATH` | `../local-data-lakehouse` | Checkout of local-data-lakehouse (its `lakehouse_client` is reused) |
| `DQ_LAKEKEEPER_URL` | `http://localhost:8181` | Iceberg REST catalog |
| `DQ_LAKEKEEPER_WAREHOUSE` | `lakehouse` | Lakekeeper warehouse |
| `DQ_CATALOG_ALIAS` | `lakehouse` | DuckDB alias for the attached catalog |
| `DQ_TRINO_HOST` / `DQ_TRINO_PORT` | `localhost` / `8088` | Trino coordinator |
| `DQ_FAIL_ON_WARN` | `false` | Make warnings block |
| `DQ_ALLOW_NOT_EVALUATED` | `false` | Let unevaluated checks pass |
| `DQ_SINKS` | `["json","junit","markdown"]` | Report formats written to `DQ_REPORTS_DIR` |
| `DQ_ICEBERG_SINK` | `false` | Also append results to Iceberg `ops.dq_results` |
| `DQ_SLACK_WEBHOOK_URL` | unset | Slack incoming webhook (HTTPS only, stored as a secret) |
| `DQ_SLACK_ONLY_ON_PROBLEMS` | `true` | Skip Slack for clean passes |
| `DQ_LOG_JSON` / `DQ_LOG_LEVEL` | `false` / `INFO` | structlog output |
| `DQ_DUCKDB_HTTPFS_CLIENT` | unset | `httplib` inside containers (DNS for `*.localhost`) |
| `DQ_DUCKDB_TEMP_DIRECTORY` | unset | Writable spill directory, e.g. `/tmp/duckdb` on a read-only root filesystem |
| `DQ_CONTRACTS_DIR`, `DQ_DATASOURCES_DIR`, `DQ_REPORTS_DIR`, `DQ_SAMPLE_DIR` | project folders | Path overrides |
| `AIRFLOW_API_PORT` (Makefile) | `8080` | Airflow UI/API host port |

## Contracts

Contracts are plain Soda v4 YAML. `{{ dataset_root }}` is rendered per engine. Here is an excerpt of
[`contracts/retail/silver/orders.yml`](contracts/retail/silver/orders.yml):

```yaml
dataset: "{{ dataset_root }}/silver/orders"

checks:
  - schema:
      name: Silver orders schema is unchanged
  - failed_rows:
      name: No order timestamped in the future
      expression: order_ts > CURRENT_TIMESTAMP

columns:
  - name: customer_id
    data_type: varchar
    checks:
      - missing:
          name: Customer id present
      - invalid:
          name: Customer exists in silver.customers
          valid_reference_data:
            dataset: "{{ dataset_root }}/silver/customers"
            column: customer_id
```

[docs/contracts.md](docs/contracts.md) explains every check and why it sits in its layer.

## Project structure

```text
contracts/retail/{bronze,silver,gold}/*.yml   Soda v4 data contracts
datasources/trino.yml                         Soda data source for Trino
src/soda_lakehouse_dq/
├── domain/        models, ports (Protocols), errors (stdlib only)
├── application/   GatePolicy, VerifyLayerUseCase, RunMedallionUseCase
├── adapters/      Soda verifier, DuckDB/Trino providers, contracts, sinks, notifiers
├── container.py   composition root
├── cli.py         Typer CLI (soda-dq)
└── settings.py    pydantic-settings (DQ_*)
airflow/dags/      lakehouse_retail_medallion_dq.py, dq_operators.py
docker/            Dockerfile, docker-compose.dq.yml, docker-compose.airflow-dq.yml
fixtures/          good_batch, bad_batch (injected defects)
tests/             unit (fakes for every port) + integration (real Soda on DuckDB)
docs/              architecture, design principles, contracts, runbook, ADRs, results
```

> **Names.** The repository and distribution are `lakehouse-quality-gates`, the CLI is `soda-dq`, and the
> import package is `soda_lakehouse_dq` (kept to avoid churn). The Docker image is
> `lakehouse-quality-gates:<version>`.

## Design principles

The code is split into ports and adapters. The **domain** has no third-party imports, the
**application** layer orchestrates through `Protocol` ports, **adapters** wrap Soda, DuckDB, Trino, the file
system and Slack, and `container.py` is the only place that wires concrete classes together. Each SOLID
principle is mapped to real classes in **[docs/design-principles.md](docs/design-principles.md)**, and the
decisions are recorded as [ADRs](docs/adr/).

## Verified results

Verified end to end on macOS on 2026-10-13. Every log and report is in **[docs/results](docs/results/README.md)**.

| Check | Result |
|---|---|
| ruff · mypy --strict | ✅ clean |
| pytest | ✅ 96 passed, 99% coverage |
| Contracts (`soda-dq validate`) | ✅ 5/5 |
| Good / bad batch (no Docker) | ✅ 3/3 gates passed / 3/3 blocked |
| Live DuckDB → Lakekeeper Iceberg | ✅ 46/46 passed · bad batch blocked |
| Trino 483 | ✅ 46/46 passed · bad batch blocked |
| Docker `ldl-soda` (non-root, read-only) | ✅ good passed · bad blocked |
| Airflow DAG | ✅ good: 9/9 tasks succeeded · bad: `dq_bronze` failed, downstream `upstream_failed` |

## Documentation

| Doc | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Seven diagrams: context, containers, medallion gates, sequence, DAG, deployment, classes |
| [Design principles](docs/design-principles.md) | SOLID mapped to classes, plus typing, errors, logging, config and security |
| [Use case](docs/use-case.md) | The retail scenario end to end |
| [Contracts](docs/contracts.md) | Every check, its layer and its rationale |
| [Runbook](docs/runbook.md) | Daily operation, gate triage, the verified macOS walkthrough, troubleshooting |
| [ADRs](docs/adr/) | Licence, DuckDB cursor reuse, dataset token, separate DAG, ports and adapters |
| [Results](docs/results/README.md) | Verification artefacts |
| [Changelog](CHANGELOG.md) | Release history |

## Roadmap

- [ ] **Write-Audit-Publish with Iceberg branches.** Write each layer to an audit branch, gate it, then
  fast-forward `main`, so readers never see unaudited snapshots.
- [ ] Gate the lakehouse's second pipeline (renewal features).
- [ ] OpenLineage events from gate runs, so quality is visible next to lineage.
- [ ] Contract drift checks in CI against the live schema.
- [ ] Dashboard over `ops.dq_results` (trend of failures per check).

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md). In short: `uv sync --all-extras && uv run pre-commit install`,
then make sure `make check validate demo` passes, which is exactly what CI runs.

## License

[MIT](LICENSE) © 2026 Santosh Shinde.

> Soda Core 4.x is **proprietary**: free to use, but not OSI-licensed. This project accepts that trade-off on
> purpose (see [ADR 0001](docs/adr/0001-soda-core-v4-proprietary-licence.md)). Soda Cloud is not used, and
> telemetry is switched off.

## Acknowledgements

Built on [Soda Core](https://github.com/sodadata/soda-core), [Apache Iceberg](https://iceberg.apache.org/),
[Lakekeeper](https://github.com/lakekeeper/lakekeeper), [DuckDB](https://duckdb.org/),
[Trino](https://trino.io/), [Apache Spark](https://spark.apache.org/), [Apache Airflow](https://airflow.apache.org/),
[RustFS](https://github.com/rustfs/rustfs) and [uv](https://github.com/astral-sh/uv). The README structure
takes cues from dbt, Great Expectations, Dagster and soda-core.
