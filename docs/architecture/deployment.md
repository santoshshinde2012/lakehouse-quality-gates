# Deployment

> [!NOTE]
> **At a glance.** One hardened container, `ldl-soda`, joins the lakehouse's Docker network `ldl-net`. Airflow
> starts gates in it with `docker exec`; the gate reads Iceberg through Lakekeeper with vended credentials.

## Topology

The containers, their host ports and how they connect.

<!-- diagram:deployment -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Deployment on Docker network ldl-net
%% Purpose: Containers, ports and how they talk to each other
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
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
      AF["Airflow :8080"]:::node
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

**How to read it:**

- The dashed boundary is the Docker network `ldl-net`; `:NNNN` are host ports (defaults).
- `ldl-soda` (indigo) is this project; every other container belongs to `local-data-lakehouse`.
- Airflow reaches containers only through the Docker socket proxy, which allows `exec` and little else.
- `ldl-trino` runs only in the lakehouse's full profile with `TRINO=1`.

## Containers

| Component | Image / version | Host port | Notes |
|---|---|---|---|
| Airflow api-server, scheduler, dag-processor | Apache Airflow 3 | 8080 by default (`AIRFLOW_API_PORT`) | Mounts our DAG through our own compose overlay |
| docker-proxy | socket proxy | none | Allowlist widened to `exec` for `ldl-soda` |
| `ldl-spark` | Spark 4.1.3, Iceberg 1.12 | none | Runs the jobs |
| `ldl-soda` | `lakehouse-quality-gates:<version>` | none | Non-root `dq`, read-only root FS, `cap_drop: ALL`, `no-new-privileges`, tmpfs `/tmp` |
| `ldl-trino` | Trino 483 | 8088 | Full profile only |
| `ldl-lakekeeper` | Lakekeeper | 8181 | Iceberg REST catalog, vends S3 credentials |
| `ldl-postgres` | PostgreSQL | none | Catalog database |
| `ldl-objectstore` | RustFS | 9000 | `s3://lake/warehouse` |

## Ways to run a gate

| Where | Engine | Command | Needs |
|---|---|---|---|
| Laptop, no Docker | `duckdb-local` | `make demo` | uv |
| Host against the lakehouse | `duckdb` or `trino` | `DQ_ENGINE=duckdb uv run soda-dq run` | lakehouse light or full profile |
| Runner container | `duckdb` | `make dq-up`, then `docker exec ldl-soda soda-dq run` | lakehouse full profile |
| Airflow | `duckdb` in `ldl-soda` | `make airflow-dq-up && make airflow-dq-trigger` | the above plus the Airflow overlay |

Step-by-step commands: [quickstart](../guides/quickstart.md) and [runbook](../guides/runbook.md).

---

[← Data flow](data-flow.md) · [Architecture index](README.md) · [Security →](security.md)
