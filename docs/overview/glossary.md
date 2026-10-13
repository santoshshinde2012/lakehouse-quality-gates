# Glossary

Terms are used with exactly these meanings across the code, the reports and the docs.

## Core terms

| Term | Meaning |
|---|---|
| **Layer** | One medallion stage: **bronze** (raw records as ingested), **silver** (cleaned, typed, deduplicated), **gold** (business aggregates). In code: `Layer` |
| **Contract** | One Soda Core v4 YAML file that declares the schema and the checks for one dataset, e.g. `contracts/retail/silver/orders.yml`. In code: `ContractSpec` |
| **Check** | One assertion inside a contract (schema, row_count, missing, invalid, duplicate, freshness, failed_rows, metric). In code: `CheckResult` |
| **Gate** | All contracts of one layer, verified together right after the layer is built, plus the policy that decides the result. One gate per layer: `dq_bronze`, `dq_silver`, `dq_gold` |
| **Gate policy** | The rules that turn check outcomes into a gate status. In code: `GatePolicy` |
| **Gate report** | Everything one gate run produced: run id, layer, engine, status, per-check results and reasons. In code: `GateReport` |

## Outcomes and statuses

A **check outcome** belongs to one check; a **gate status** belongs to the whole layer.

| Term | Level | Meaning |
|---|---|---|
| **pass** (`passed`) | check | The check's condition holds |
| **warn** (`warned`) | check | A check with `level: warn` did not hold. Recorded and alerted, but does not block by default |
| **fail** (`failed`) | check | A check with the default `level: fail` did not hold |
| **not evaluated** (`not_evaluated`) | check | The check could not be computed (e.g. the engine ran out of memory) |
| **passed** | gate | No failure, error or unevaluated check, and no warning. Exit code 0 |
| **passed with warnings** | gate | Only warnings. Exit code 0; the next layer runs |
| **block** (`blocked`) | gate | At least one failure, error or unevaluated check, or no contracts at all. Exit code 1; the Airflow task fails and downstream tasks do not run |
| **Fail closed** | gate | A gate that could not prove the data is good blocks. Errors and unevaluated checks count as failures |

## Platform terms

| Term | Meaning |
|---|---|
| **Medallion** | The bronze → silver → gold layering of the lakehouse |
| **Engine** | What executes the check SQL: `duckdb` (host DuckDB on the Iceberg catalog), `duckdb-local` (in-memory DuckDB from CSVs), `trino`. Chosen with `DQ_ENGINE` |
| **Runner** | The `ldl-soda` container that runs `soda-dq` next to the lakehouse |
| **Sink** | Where a gate report is written: JSON, JUnit XML, Markdown, the `ops.dq_results` Iceberg table. Port: `ResultSink` |
| **Notifier** | Who is told about a gate result: console, Slack. Port: `Notifier` |
| **Port / adapter** | A port is a small `Protocol` in the domain; an adapter implements it with a real library (ports and adapters, ADR 0005) |
| **Iceberg snapshot / branch** | A snapshot is one committed version of a table; a branch is a named pointer to snapshots, `main` being the default |
| **WAP** | Write-audit-publish: write a batch to a branch, gate it, then fast-forward `main` (proposed, see [write-audit-publish](../architecture/write-audit-publish.md)) |
| **Vended credentials** | Short-lived S3 credentials that the Lakekeeper REST catalog hands out per table |
| **`dataset_root` token** | The `{{ dataset_root }}` placeholder in a contract's `dataset:` key, rendered per engine (ADR 0003) |
| **Run id** | Unique id of one gate run, shared by all its reports |

## Benchmark and licence terms

| Term | Meaning |
|---|---|
| **p50 / p95** | Median / 95th percentile. With five runs, p95 (nearest rank) is the slowest run |
| **Cold / warm run** | Cold: the first gate run after the data was written. Warm: the next five |
| **RSS** | Resident set size: physical memory used by a process |
| **ELv2** | Elastic License 2.0, the source-available licence of Soda Core v4 |
| **ODCS** | Open Data Contract Standard (Bitol, LF AI & Data) |
