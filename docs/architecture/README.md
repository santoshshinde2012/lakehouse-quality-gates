# Architecture

These pages hold the detail behind the [system design](../system-design.md), one topic per page. Read them in
order the first time; each page stands on its own afterwards.

| # | Page | Read it to learn |
|---|---|---|
| 1 | [Overview](overview.md) | What the system is, who uses it and where the gates sit |
| 2 | [Components](components.md) | How the code is organised: modules, domain model, ports and adapters |
| 3 | [Data flow](data-flow.md) | What happens during a gate run and in the Airflow DAG |
| 4 | [Deployment](deployment.md) | Which containers run where and how they connect |
| 5 | [Security](security.md) | Credentials, hardening and licence |
| 6 | [Observability](observability.md) | How to see what the gates did |
| 7 | [Write-audit-publish](write-audit-publish.md) | Audit branches, the gate on the branch and the fast-forward to `main` |
| 8 | [Design principles](design-principles.md) | The engineering rules behind the code |

Diagrams on these pages: [catalogue](../diagrams/README.md).
