# ADR 0006: Write-audit-publish with Iceberg audit branches

- Status: accepted, implemented in v1.0.0
- Supersedes: the "write-audit-publish later" follow-up of [ADR 0004](0004-separate-gated-dag.md)

## Context

The gated DAG ([ADR 0004](0004-separate-gated-dag.md)) runs each gate *after* Spark has replaced the layer with
`CREATE OR REPLACE`. A blocked gate stops the next layer, but readers of the checked layer already see the bad
batch (limitation L1). The lakehouse's Spark jobs and tracked files must not change (G6).

## Decision

1. **Write to a branch, from our own job.** `wap/wap_job.py` (this repo, mounted read-only into `ldl-spark` by
   `docker/docker-compose.wap.yml`) builds each layer on the branch `audit_<run>`: `ALTER TABLE t CREATE BRANCH`
   from main's current snapshot, then `DELETE FROM t.branch_audit_<run>` and an append. It reuses the lakehouse's
   SQL files read-only: `bronze_ddl.sql` for the bronze tables and the `SELECT` of every `CREATE OR REPLACE TABLE`
   in `silver_orders.sql` and `gold_daily_metrics.sql`. We use explicit `t.branch_<name>` identifiers instead of
   `spark.wap.branch`, so no table property (`write.wap.enabled`) has to change.
2. **Audit the branch with DuckDB.** `PyIcebergSnapshotReader` reads each table's branch snapshot id from the
   REST catalog (metadata only). `DuckDBProvider(pins=...)` attaches an in-memory `dq_audit` catalog with one view
   per table: pinned tables read `AT (VERSION => <snapshot id>)` (DuckDB 1.5 iceberg extension), every other table
   reads main. `USE dq_audit` makes the *unchanged* contracts resolve to the branch. Verified live: counts and
   schema match the branch, not main.
   - Trino `FOR VERSION AS OF 'branch'` also works, but Soda builds the SQL from the dataset name, so a clause
     cannot be injected; DuckDB views solve that without touching contracts. WAP therefore requires
     `DQ_ENGINE=duckdb`.
3. **Publish with Spark.** `CALL lakehouse.system.fast_forward(table => t, branch => 'main', to => audit_<run>)`,
   then `DROP BRANCH`. OSS Trino 483 cannot fast-forward (L7).
4. **On block, do nothing to main.** The branch is kept with the bad rows for triage (`soda-dq wap status`,
   `soda-dq wap drop`); `DQ_WAP_DROP_BLOCKED=true` drops it instead.
5. **Hexagonal wiring.** New ports `AuditWriter`, `BranchPublisher`, `SnapshotReader`, `BranchGate`; adapters
   `SparkSubmitWap` (`docker exec ldl-spark spark-submit`), `PyIcebergSnapshotReader`, `PinnedSnapshotGate`; use
   cases `VerifyBranchUseCase` and `RunWapMedallionUseCase`; CLI `soda-dq wap run|verify|status|drop`; Airflow DAG
   `lakehouse_retail_medallion_wap` (the other DAGs are unchanged).

## Consequences

- Readers of `main` never see a batch that failed its gate: proven live, with snapshot ids before and after, in
  [the WAP run](../results/runs/2026-10-14-wap/README.md). L1 is resolved for the WAP DAG and CLI.
- Each layer costs two extra Spark submissions (write and publish run as separate jobs so the gate can run in
  between). At 1M orders this adds 3.9 to 4.5 s of Spark time per layer, the gate adds under 0.7 s, and the wall clock for a layer grows from about 22 s to about 41 s, mostly spark-submit start-up; see [benchmarks](../benchmarks/results.md#write-audit-publish-overhead).
- There is no atomic multi-table publish on this catalog: tables of a layer are fast-forwarded one after another
  (a failure between two fast-forwards leaves the layer half published; rerun the publish step).
- Fast-forward needs main to be an ancestor of the branch, so the DAG runs one at a time (`max_active_runs=1`).
- `DELETE` + append on the branch replaces the layer like `CREATE OR REPLACE`, but keeps the table's schema; a
  schema change needs a one-off migration on main.
