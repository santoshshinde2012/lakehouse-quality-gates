# Documentation

Everything about **lakehouse-quality-gates**: Soda Core v4 data contracts that act as quality gates between
the bronze, silver and gold layers of [local-data-lakehouse](https://github.com/santoshshinde2012/local-data-lakehouse).

> [!TIP]
> New here? Read the [use case](overview/use-case.md), run the [quickstart](guides/quickstart.md), then skim the
> [architecture overview](architecture/overview.md). The [glossary](overview/glossary.md) defines every term we use.

## Who should read what

| You are | Start with | Then |
|---|---|---|
| New to the project | [Use case](overview/use-case.md) | [Quickstart](guides/quickstart.md), [Glossary](overview/glossary.md) |
| Data engineer writing checks | [Contracts guide](guides/contracts.md) | [Data flow](architecture/data-flow.md), [Runbook](guides/runbook.md) |
| On call, a gate just blocked | [Runbook](guides/runbook.md#a-gate-blocked-triage) | [Troubleshooting](guides/troubleshooting.md) |
| Architect or reviewer | [System design](system-design.md) | [ADRs](adr/README.md), [Research](research/README.md) |
| Developer changing the code | [Components](architecture/components.md) | [Design principles](architecture/design-principles.md), [CONTRIBUTING](../CONTRIBUTING.md) |
| Platform or SRE | [Deployment](architecture/deployment.md) | [Security](architecture/security.md), [Observability](architecture/observability.md), [Benchmarks](benchmarks/README.md) |

## All documents

### Overview

| Document | What it covers |
|---|---|
| [Use case](overview/use-case.md) | The retail problem, what each gate guarantees, and the good and bad batch demo |
| [Glossary](overview/glossary.md) | Gate, contract, layer, check, block, warn and every other term, defined once |

### Architecture

| Document | What it covers |
|---|---|
| [System design](system-design.md) | The end-to-end design document (20 sections): requirements, design, benchmarks, risks, roadmap |
| [Architecture index](architecture/README.md) | Reading order for the focused pages below |
| [Overview](architecture/overview.md) | System context and system architecture |
| [Components](architecture/components.md) | Code architecture, domain model, modules, engines, sinks and errors |
| [Data flow](architecture/data-flow.md) | Medallion gates, gate policy, gate run sequence, Airflow DAG, data model |
| [Deployment](architecture/deployment.md) | Containers, ports, profiles and the runner image |
| [Security](architecture/security.md) | Credentials, container hardening, secrets, licence |
| [Observability](architecture/observability.md) | Logs, reports, `ops.dq_results`, alerts |
| [Write-audit-publish](architecture/write-audit-publish.md) | Proposed: Iceberg branches so readers never see unaudited data |
| [Design principles](architecture/design-principles.md) | SOLID mapped to real classes, typing, errors, configuration |

### Guides

| Document | What it covers |
|---|---|
| [Quickstart](guides/quickstart.md) | Run the gates in a minute without Docker, then against the live lakehouse |
| [Contracts](guides/contracts.md) | Contract conventions, every check per layer, and how to add one |
| [Configuration](guides/configuration.md) | CLI commands, exit codes and every `DQ_*` setting |
| [Runbook](guides/runbook.md) | Daily operation, triaging a blocked gate, the verified macOS walkthrough |
| [Troubleshooting](guides/troubleshooting.md) | Symptoms and fixes |

### Benchmarks

| Document | What it covers |
|---|---|
| [Benchmarks index](benchmarks/README.md) | Headline numbers and where the raw data lives |
| [Methodology](benchmarks/methodology.md) | Machine, data, engines, runs and how every metric is taken |
| [Results](benchmarks/results.md) | Latency, resources, accuracy, overhead and the tool comparison |
| [Analysis](benchmarks/analysis.md) | What the numbers mean: SLO verdicts, capacity planning, caveats |

### Decisions, research and diagrams

| Document | What it covers |
|---|---|
| [ADRs](adr/README.md) | Architecture decision records |
| [Research](research/README.md) | The quality-gate tool landscape and scoring |
| [Diagrams](diagrams/README.md) | Catalogue of every Mermaid source and the shared [theme](diagrams/THEME.md) |
| [Verification results](results/README.md) | Logs, reports and coverage from the verified runs |
| [Changelog](../CHANGELOG.md) | Release history |

## Conventions

- Every diagram is Mermaid, kept as one source file in [`diagrams/`](diagrams/README.md) and synced into the pages by
  `make diagrams`. `make diagrams-check` and the tests fail if a copy is stale.
- Terms follow the [glossary](overview/glossary.md). Numbers come from [`results/`](results/README.md); pages link to the raw data.
- `make docs-check` verifies every relative link and anchor; markdownlint runs in pre-commit and CI.
