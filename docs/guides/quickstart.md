# Quickstart

> [!TIP]
> The first block needs only [uv](https://docs.astral.sh/uv/) and no Docker. The good batch passes every gate; the
> bad batch is blocked at every layer, and the Markdown report says why.

## Without Docker (one minute)

No Docker is needed. Requires [uv](https://docs.astral.sh/uv/) and takes about a minute:

<!-- region:quickstart -->
```bash
git clone https://github.com/santoshshinde2012/lakehouse-quality-gates.git && cd lakehouse-quality-gates
uv sync --all-extras            # Python 3.12 venv: Soda Core 4.26.0, DuckDB 1.5.6, dev tools
make validate                   # Soda parses every contract
make demo                       # good batch: all gates pass; bad batch: every gate blocks
open reports/*_retail_silver.md # what was caught, and why (use xdg-open on Linux)
```
<!-- /region -->

## Against the running lakehouse (DuckDB, Trino)

```bash
cp .env.example .env                             # set DQ_LDL_REPO_PATH to your local-data-lakehouse checkout
export DQ_LDL_REPO_PATH=../local-data-lakehouse  # same path, for the next line
(cd "$DQ_LDL_REPO_PATH" && make up-light && make demo-light)   # tables exist in Lakekeeper
DQ_ENGINE=duckdb uv run soda-dq run                            # host DuckDB -> Iceberg REST
# Trino (lakehouse: make up-full TRINO=1):
DQ_ENGINE=trino uv run soda-dq run
```

## Docker runner and Airflow

```bash
(cd "$DQ_LDL_REPO_PATH" && make up-full)
make dq-up                                  # build lakehouse-quality-gates:<version>, start ldl-soda on ldl-net
make airflow-dq-up                          # lakehouse Airflow + the gated DAG (add AIRFLOW_API_PORT=8090 if 8080 is taken)
make airflow-dq-trigger                     # good data: every task succeeds
make airflow-dq-trigger-bad                 # inject_bad_batch: dq_bronze fails, downstream is upstream_failed
make dq-down && make airflow-dq-down        # stop; volumes are kept
```

## What next

- Every test tier, make target and benchmark, from a fresh clone: [running and testing](running-and-testing.md).

- Read a report: `reports/<run>_retail_<layer>.md` lists every check and the reasons for a block.
- Write or change a check: [contracts guide](contracts.md).
- All commands and settings: [configuration](configuration.md).
- Operate it day to day: [runbook](runbook.md).
