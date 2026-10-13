# Diagrams

Every diagram is a Mermaid source file in this folder (13 in total). The pages embed a copy that GitHub renders inline;
`make diagrams` refreshes the copies and `make diagrams-check` (also run by the tests) fails on drift.
All sources share one init line and palette, described in [THEME.md](THEME.md).

| Source | Title | Type | What it shows | Shown in |
|---|---|---|---|---|
| [`system-context.mmd`](system-context.mmd) | System context | flowchart | lakehouse-quality-gates and the people and systems around it. | [overview.md](../architecture/overview.md) |
| [`system-architecture.mmd`](system-architecture.mmd) | System architecture | flowchart | Where the quality gates sit between the lakehouse layers, and what every gate run produces. | [README.md](../../README.md), [overview.md](../architecture/overview.md), [system-design.md](../system-design.md) |
| [`code-architecture.mmd`](code-architecture.mmd) | Code architecture (hexagonal) | flowchart | How the `soda-dq` code is layered as ports and adapters. | [components.md](../architecture/components.md) |
| [`domain-model.mmd`](domain-model.mmd) | Domain model | class | The value objects a gate run produces and the enumerations they use. | [components.md](../architecture/components.md) |
| [`ports-adapters.mmd`](ports-adapters.mmd) | Ports and adapters | flowchart | Which adapter implements each domain port, and what the use case depends on. | [components.md](../architecture/components.md) |
| [`data-model.mmd`](data-model.mmd) | Data model | ER | The retail Iceberg tables the gates check, and the `ops.dq_results` table the Iceberg sink writes. | [data-flow.md](../architecture/data-flow.md) |
| [`medallion-gates.mmd`](medallion-gates.mmd) | Medallion flow with quality gates | flowchart | One gate after each layer; only a passing gate lets the next layer run. | [data-flow.md](../architecture/data-flow.md), [system-design.md](../system-design.md) |
| [`gate-policy.mmd`](gate-policy.mmd) | Gate policy decision flow | flowchart | How `GatePolicy` turns the check outcomes of one layer into *passed*, *passed with warnings* or *blocked*. | [data-flow.md](../architecture/data-flow.md) |
| [`gate-run-sequence.mmd`](gate-run-sequence.mmd) | Gate run sequence | sequence | What happens, step by step, when one layer is gated (here: silver). | [data-flow.md](../architecture/data-flow.md) |
| [`airflow-dag.mmd`](airflow-dag.mmd) | Airflow DAG `lakehouse_retail_medallion_dq` | flowchart | The tasks of the gated DAG and how a failed gate stops the run. | [data-flow.md](../architecture/data-flow.md) |
| [`deployment.mmd`](deployment.mmd) | Deployment on Docker network `ldl-net` | flowchart | The containers, their host ports and how they connect. | [deployment.md](../architecture/deployment.md) |
| [`wap-sequence.mmd`](wap-sequence.mmd) | Write-audit-publish (proposed) | sequence | How Iceberg branches would keep a layer hidden from readers until its gate passes. | [write-audit-publish.md](../architecture/write-audit-publish.md) |
| [`benchmark-scaling.mmd`](benchmark-scaling.mmd) | Scaling with batch size (measured) | xychart | Seconds for all three layers as the batch grows from 10k to 5M orders. | [results.md](../benchmarks/results.md) |

## Editing a diagram

1. Edit `docs/diagrams/<name>.mmd`. Keep the first line (the shared init) and the header comment
   (`%% Title`, `%% Purpose`, `%% Last updated`).
2. Run `make diagrams` to sync every Markdown copy and render SVGs locally (`docs/diagrams/svg/`, not committed).
3. Run `make diagrams-check`; commit the `.mmd` and the updated Markdown together.

In a page, a diagram always has a heading, one sentence on what it shows, the synced block between
`<!-- diagram:<name> -->` and `<!-- /diagram -->`, and a short *How to read it* list.
