# ADR 0001: Use Soda Core 4.26 despite its proprietary licence

- Status: accepted (approved by Santosh Shinde, 2026-10-13)

## Context

local-data-lakehouse has a rule that every dependency is OSI open source (`requirements.in`). Soda's current
line, Soda Core 4.x (`soda-core`, `soda-duckdb`, `soda-trino`, 4.26.0 released 2026-10-06), is published on
PyPI with `License: Proprietary`, and its GitHub repository reports "Other". Soda Core 3.x (SodaCL scans) was
Apache-2.0 but is the deprecated line. v4 brings data contracts, the Python API
`verify_contract_locally`, DuckDB `from_existing_cursor`, warn/fail thresholds and variables.

## Decision

Use Soda Core **4.26.0**, with versions pinned exactly in `pyproject.toml` and `uv.lock`. Keep it in this
**separate project**, so the lakehouse repo's OSI rule stays intact. Don't use Soda Cloud. Set telemetry
off by default (`SODA_CORE_TELEMETRY_ENABLED=false`).

## Consequences

- We get modern contracts and an API we can call from code. The trade-off is a licence review whenever
  this project is redistributed. Our own code stays MIT.
- Licensed-only features (group-by, reconciliation checks, percentage thresholds on failed_rows, Cloud
  notifications and history) are not used. Equivalents are built from free checks plus our own sinks and
  notifiers.
- All Soda calls sit behind `ContractVerifier` (`adapters/soda_verifier.py`). Swapping to Soda v3, Great
  Expectations or plain SQL would touch one adapter, not the use cases.

## Amendment (2026-10-13, v0.3.0)

The licence research for the system design (docs/research/quality-gates-landscape.md) found that Soda Core
v4 is published under the **Elastic License 2.0 (ELv2)**, a source-available licence, rather than an
unnamed proprietary licence. ELv2 allows internal use, modification and redistribution with notices; it
forbids offering the software as a hosted or managed service and circumventing licence keys. This project
does neither, so the decision stands. The OSI-licensed fallback (ODCS v3.2 contracts tested by
datacontract-cli, Pointblank for failed rows, behind the same `ContractVerifier` port) is described and
benchmarked in docs/system-design.md sections 4 and 12.6.
