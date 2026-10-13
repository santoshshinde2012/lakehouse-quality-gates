# Troubleshooting

Find the symptom, apply the fix. For a gate that **blocked** on real data, follow the [runbook triage](runbook.md#a-gate-blocked-triage) instead.

| Symptom | Cause and fix |
|---|---|
| `lakehouse_client not found under …/src` | Set `DQ_LDL_REPO_PATH` to the local-data-lakehouse checkout |
| `cannot attach Lakekeeper at http://localhost:8181` | The lakehouse is not up: `make up-light` in the lakehouse repo |
| `Table … does not exist` | Tables not built yet: `make demo-light`, `make e2e` or the Airflow DAG |
| Schema check fails after a deploy | Intended drift: update the contract's `columns`. Otherwise, roll back |
| `YAML key 'must_be_less_than' expected one of ['Number'], but was str` | A string variable was passed straight to the Soda API. Use the CLI or `coerce_variable` |
| Airflow `dq_*`: `403` from the docker proxy | The overlay is not applied. Start Airflow with `make airflow-dq-up` (adds `ldl-soda` to the allowlist) |
| Airflow `dq_*`: `No such container: ldl-soda` | `make dq-up` first (needs network `ldl-net`) |
| Freshness fails on old data | Bronze must be ingested within `BRONZE_MAX_AGE_HOURS` (24). For a replay, `--set BRONZE_MAX_AGE_HOURS=720` |
| Trino gates block at very large batches with `EXCEEDED_LOCAL_MEMORY_LIMIT` in the reasons | Trino's default 1 GiB heap is too small (measured at 5M orders). Give Trino more memory or use `DQ_ENGINE=duckdb` ([benchmarks](../benchmarks/results.md#latency)) |
| Exit code 2 and no report | Configuration or connectivity: check `DQ_LDL_REPO_PATH`, Lakekeeper (`:8181`), Trino (`:8088`), `docker ps` |
