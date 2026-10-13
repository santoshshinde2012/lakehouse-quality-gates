# Security

> [!NOTE]
> **At a glance.** The gate holds no storage keys, only reads data, runs as a non-root user on a read-only root
> filesystem, and never logs the Slack webhook.

| Area | Control |
|---|---|
| Storage credentials | None configured in `soda-dq`. DuckDB and Trino receive short-lived S3 credentials vended by Lakekeeper per table |
| Least privilege | The gate only reads; it never writes lakehouse tables (except `seed`, a demo command that refuses to run without `--yes`, and the opt-in `ops.dq_results` sink) |
| Container | Non-root user, read-only root filesystem, all capabilities dropped, `no-new-privileges`, digest-pinned base images, tmpfs for spills |
| Airflow → container | Through the lakehouse's socket proxy, `exec` only; task commands are built from validated, quoted constants |
| Secrets | `DQ_SLACK_WEBHOOK_URL` is a `SecretStr`, https-only, never logged; `.env` is git-ignored |
| Telemetry | `SODA_CORE_TELEMETRY_ENABLED=false` by default, in the image and at start-up |
| Supply chain | Versions pinned in `uv.lock`; pre-commit runs ruff, mypy, detect-private-key and YAML checks |
| Licence compliance | Our code: MIT. Soda Core v4: Elastic License 2.0 (source-available): internal use is allowed; offering Soda as a managed service is not. Kept out of `local-data-lakehouse`, whose rule is OSI-only dependencies (ADR 0001) |

The container settings are in [`docker/docker-compose.dq.yml`](../../docker/docker-compose.dq.yml) and [`docker/Dockerfile`](../../docker/Dockerfile); the licence decision is [ADR 0001](../adr/0001-soda-core-v4-proprietary-licence.md).

---

[← Deployment](deployment.md) · [Architecture index](README.md) · [Observability →](observability.md)
