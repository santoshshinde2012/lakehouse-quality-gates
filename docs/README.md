# Documentation

Everything about **lakehouse-quality-gates**: Soda Core v4 data contracts that act as quality gates between
the bronze, silver and gold layers of [local-data-lakehouse](https://github.com/santoshshinde2012/local-data-lakehouse).

## Reading path

1. [Use case](overview/use-case.md): the problem and what a gate guarantees (5 min).
2. [Quickstart](guides/quickstart.md): set up and run the good and bad batch, no Docker (10 min).
3. [Architecture overview](architecture/overview.md): the system on one page (5 min).
4. [Running and testing](guides/running-and-testing.md): the test tiers, the live stack and the benchmarks (10 min).
5. [System design](system-design.md): requirements, decisions and risks, linking every detail page (15 min).

Terms are defined once in the [glossary](overview/glossary.md); measured evidence is in [results](results/README.md).

## Who should read what

| You are | Start with | Then |
|---|---|---|
| New to the project | [Use case](overview/use-case.md) | [Quickstart](guides/quickstart.md), [Glossary](overview/glossary.md) |
| Contributor running the tests | [Running and testing](guides/running-and-testing.md) | [Contributing](../CONTRIBUTING.md), [Benchmarks](benchmarks/results.md) |
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
| [System design](system-design.md) | The end-to-end narrative: problem, requirements, tool choice, decisions, risks, roadmap. Links to the pages below for detail |
| [Architecture index](architecture/README.md) | Reading order for the focused pages below |
| [Overview](architecture/overview.md) | System context and system architecture |
| [Components](architecture/components.md) | Code architecture, modules, domain model, ports and adapters, engines, sinks, errors |
| [Data flow](architecture/data-flow.md) | Medallion gates, gate policy, gate run sequence, Airflow DAG, data model |
| [Deployment](architecture/deployment.md) | Containers, ports, profiles and the runner image |
| [Security](architecture/security.md) | Credentials, container hardening, secrets, licence |
| [Observability](architecture/observability.md) | Logs, reports, `ops.dq_results`, alerts |
| [Write-audit-publish](architecture/write-audit-publish.md) | Implemented: each layer on an Iceberg audit branch, gated, then fast-forwarded to `main` |
| [Design principles](architecture/design-principles.md) | SOLID mapped to real classes, typing, errors, configuration |

### Guides

| Document | What it covers |
|---|---|
| [Quickstart](guides/quickstart.md) | The canonical setup: numbered steps for the local demo, the live stack and the full stack |
| [End-to-end setup](guides/end-to-end.md) | One ordered path on macOS or Linux: services, gates, WAP, Airflow, benchmarks, charts, teardown, with service screenshots |
| [Running and testing](guides/running-and-testing.md) | Prerequisites, setup, the four test tiers, every make target, benchmarks, regenerating docs |
| [Contracts](guides/contracts.md) | Contract conventions, every check per layer, and how to add one |
| [Configuration](guides/configuration.md) | CLI commands, exit codes and every `DQ_*` setting |
| [Runbook](guides/runbook.md) | Daily operation, triaging a blocked gate, a manual walkthrough of every component |
| [Enterprise operating model](guides/enterprise-readiness.md) | Quality ownership, publication boundaries, evidence trust, access, recovery, and rollout decisions |
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
| [Verification results](results/README.md) | Every verified run, newest first, with date, commit, machine and the make target to reproduce it |
| [Changelog](../CHANGELOG.md) | Release history |

## Conventions

- Each fact lives on one page. The system design summarises and links; the focused pages hold the detail.
- Text that must appear twice (the README quick start steps and the headline numbers) is written once as a region and copied
  by `make docs`; the tests fail if a copy is stale or if a line of prose is written twice by hand.
- Every diagram is Mermaid, kept as one source file in [`diagrams/`](diagrams/README.md) and synced into the pages by
  `make diagrams`. `make diagrams-check` and the tests fail if a copy is stale.
- Terms follow the [glossary](overview/glossary.md). Numbers come from [`results/`](results/README.md); pages link to the raw data.
- `make docs-check` verifies every relative link and anchor; markdownlint runs in pre-commit and CI.
