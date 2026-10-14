# Enterprise operating model

This repository provides a verified quality-gate implementation and a local lakehouse reference deployment.
Adopting it for a data product also needs decisions about ownership, publication boundaries, access, and
recovery. A passing gate proves the rules that were evaluated against its recorded snapshots.

## Define the quality agreement

The data product owner approves the quality dimensions and the impact of a block. Producers implement the
rules, stewards review their meaning, consumers define tolerable defects and freshness, and the platform
team operates the gate. Reuse shared rules for keys, schema, freshness, and references; set thresholds for
the product's use and criticality. A tool supplies metrics, while accountable people approve the policy.

Review contract and producer changes together. Record the owner, rule, threshold, warning/block decision,
and escalation route. Runtime `--set` overrides and permissive gate settings need the same approval and
recording process in the deployment; this CLI does not implement an approval workflow.

## Controls and deployment decisions

| Area | Implemented here | Decision required for an enterprise deployment |
|---|---|---|
| Publication | WAP audits immutable snapshot IDs, saves a manifest, and advances primary tables through private refs | Route every producer through the agreed publication path; restrict direct writes to `main` |
| Read consistency | Candidate and declared reference snapshots are available through WAP audit views | Declare each SQL dependency and use `schema.table`; catalog-qualified paths and external readers bypass those views |
| Multiple tables | Per-table publication can resume from saved pins | Accept partial publication visibility, or add a consumer release boundary outside this implementation |
| Concurrent writers | The WAP DAG permits one active run; divergent `main` blocks fast-forward | Coordinate CLI, other DAGs, and external writers; a DAG setting is not a platform-wide lock |
| Evidence | JSON, Markdown, JUnit, structured logs, and atomic WAP manifests | Protect evidence from modification; ship it to durable storage with a defined retention period |
| Credentials | Catalog-vended storage credentials and a hardened gate container | Configure catalog authentication, TLS, least privilege, and separate audit/publish identities in the target environment |
| Recovery | Blocked branches remain for inspection; a passing manifest supports publication retries | Name an on-call owner, set branch/snapshot retention, and practise interrupted publication recovery |
| Performance | Archived laptop benchmarks include machine, versions, sample counts, and raw data | Measure current contracts, publisher, and workload on the intended deployment before setting capacity or latency commitments |

The saved audit manifest is trusted local input, not a signed authorization token. A principal able to alter
it can alter a publication request. Limit write access to the evidence directory and publication commands;
external identity and authorization controls belong to the deployment. Publication reuses the saved
audit-time decision; it does not reevaluate the current contracts or policy. Retain the contract revision
and configuration alongside operational reports when defining the approval record.

## Operate a failure

For a blocked gate, inspect its report and branch before changing data or thresholds. For a publication
error, compare each table's current `main` with the saved audit pins: some tables may already have advanced.
Resume from the same manifest after resolving the cause; do not rerun the writer and assume it recreates
the audited data. Keep the saved manifest and snapshots available for the entire recovery window.

Use the [WAP recovery commands](../architecture/write-audit-publish.md#design-notes) and the
[runbook](runbook.md). Alerts are best effort; configure scheduler task-failure alerts and an owner as well.
Report files can contain business metrics and diagnostic details, so apply the data product's access policy
to reports and alert destinations.

## Review evidence before rollout

Use the [verification index](../results/README.md) to inspect raw results, test scope, and benchmark versions.
Require passing local tests, real good/bad gates, both orchestration paths, and snapshot evidence showing
that a blocked candidate did not move `main`. Test failure and replay behavior as well as the happy path.
Historical timings used 46 checks and the earlier publisher; the current 53-check contracts and publication
hardening have not been performance-benchmarked. Synthetic defect coverage does not establish quality for
rules that the contracts do not express.
