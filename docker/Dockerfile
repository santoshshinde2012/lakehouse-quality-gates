# soda-dq runner image: Soda Core 4.26 + DuckDB 1.5.6 (+ soda-trino, PyIceberg) with the soda-dq CLI.
# Local-demo image, like the lakehouse's own. Refresh digests together:
#   docker buildx imagetools inspect python:3.12-slim-bookworm --format '{{.Manifest.Digest}}'
FROM ghcr.io/astral-sh/uv:0.12.23@sha256:61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21 AS uv

FROM python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258 AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
# Dependencies first (cached layer), then the project itself.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --extra trino --extra lakehouse
COPY src ./src
# --no-editable: the runtime stage copies only /opt/venv, not /app/src.
RUN uv sync --frozen --no-dev --no-editable --extra trino --extra lakehouse

FROM python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
RUN groupadd --system --gid 10001 dq && useradd --system --uid 10001 --gid dq --home /home/dq --create-home dq
COPY --from=build /opt/venv /opt/venv
WORKDIR /opt/soda-dq
COPY contracts ./contracts
COPY datasources ./datasources
COPY fixtures ./fixtures
RUN mkdir -p /opt/soda-dq/reports && chown dq:dq /opt/soda-dq/reports
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    SODA_CORE_TELEMETRY_ENABLED=false \
    DQ_CONTRACTS_DIR=/opt/soda-dq/contracts \
    DQ_DATASOURCES_DIR=/opt/soda-dq/datasources \
    DQ_REPORTS_DIR=/opt/soda-dq/reports \
    DQ_SAMPLE_DIR=/opt/soda-dq/fixtures/good_batch \
    DQ_LOG_JSON=true
USER dq
# The root filesystem is read-only at runtime: pre-install every extension the iceberg extension loads.
RUN python -c "import duckdb; c = duckdb.connect(); [c.sql(f'INSTALL {e}') for e in ('httpfs', 'avro', 'iceberg')]"
# Long-running so Airflow can `docker exec ldl-soda soda-dq verify <layer>` (like ldl-spark).
CMD ["sleep", "infinity"]
HEALTHCHECK --interval=30s --timeout=10s CMD ["soda-dq", "--help"]
