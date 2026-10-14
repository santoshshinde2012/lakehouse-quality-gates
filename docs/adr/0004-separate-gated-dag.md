# ADR 0004: A separate Airflow DAG; gates fail tasks; write-audit-publish later

- Status: accepted

## Decision

- Add `lakehouse_retail_medallion_dq` and leave `lakehouse_retail_medallion` exactly as it is. Reuse its
  `spark_submit_task` and add `soda_gate_task(layer)`, which runs `docker exec ldl-soda soda-dq verify
  <layer>` (exit 1 fails the task, `retries=0`).
- Mount the DAG and widen the socket-proxy allowlist through our own compose overlay
  (`docker/docker-compose.airflow-dq.yml`), applied after the repo's files. No repo file changes.
- Warnings are logged, not fatal (`DQ_FAIL_ON_WARN` can change this).

## Consequences and follow-up

> [!NOTE]
> The follow-up below is done: [ADR 0006](0006-write-audit-publish.md) adds write-audit-publish in a third DAG,
> `lakehouse_retail_medallion_wap`. This DAG is kept unchanged.

The Spark jobs write with `CREATE OR REPLACE`, so a gate stops *promotion to the next layer* but cannot
undo the layer it checked. A follow-up should use Iceberg branches for write-audit-publish (write to an
`audit_<run_id>` branch, gate, then `fast_forward` main) so consumers never see unaudited data.
