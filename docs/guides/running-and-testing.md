# Running and testing

Everything needed to build, run and test this repository from a fresh clone, tier by tier. Each section gives the
command, what it does, what a passing run prints, and how long it took on the reference machine (MacBook Pro M1 Pro,
Docker Desktop with 10 CPUs and 8 GB). For day-to-day operation see the [runbook](runbook.md); for symptoms and fixes
see [troubleshooting](troubleshooting.md).

## Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Test tiers at a glance](#test-tiers-at-a-glance)
- [T0 and T1: no Docker](#t0-and-t1-no-docker)
- [T2: live light stack](#t2-live-light-stack)
- [T3: full stack with Trino, the runner container and Airflow](#t3-full-stack-with-trino-the-runner-container-and-airflow)
- [Benchmarks and the accuracy suite](#benchmarks-and-the-accuracy-suite)
- [Regenerating docs, diagrams and results](#regenerating-docs-diagrams-and-results)
- [Make targets](#make-targets)
- [Troubleshooting the test paths](#troubleshooting-the-test-paths)

## Prerequisites

| Tool | Version used | Needed for | Install |
|---|---|---|---|
| macOS 14+ or Linux (x86_64 or arm64) | macOS 26, Apple silicon | everything | |
| Python | 3.12 (`>=3.12,<3.13`) | everything; uv downloads it if missing | `uv python install 3.12` |
| [uv](https://docs.astral.sh/uv/) | 0.12 | the virtual environments and `uv.lock` | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| GNU Make, Bash, Git | Make 3.81+, Bash 3.2+ | the `make` targets and `scripts/e2e.sh` | Xcode command line tools or your distribution |
| Node.js + npx | 20+ | `make docs-check`, `make diagrams` (markdownlint, mermaid-cli) | nodejs.org or your package manager |
| Docker Engine or Docker Desktop, Compose v2 | Docker 29 | T2, T3 and the benchmarks | docker.com |
| [local-data-lakehouse](https://github.com/santoshshinde2012/local-data-lakehouse) checkout | main | T2, T3 and the benchmarks | `git clone` next to this repo |

Docker resources:

| Tier | CPUs | Memory for Docker | Disk |
|---|---|---|---|
| T2 light (Lakekeeper, RustFS, Postgres) | 2 | 2 GB | 2 GB |
| T3 full (adds Spark, Trino, Airflow, the runner) | 4+ | 6 GB, 8 GB recommended | 10 GB |
| Benchmarks at 1M to 5M orders on Trino | 6+ | 8 GB+ | 15 GB |

Trino ships with a 1 GiB heap (`-Xmx1G` in the lakehouse's `config/trino/jvm.config`). That is enough up to 1M orders.
At 5M orders the Trino gates fail with `EXCEEDED_LOCAL_MEMORY_LIMIT`. For that size, raise the heap locally to 3 to 4 GB
(keep the change out of your lakehouse commits) and give Docker at least 8 GB, or run the large sizes on DuckDB only.

## Setup

Follow the [quickstart](quickstart.md): steps 1 to 4 install everything for T0 and T1, and the live steps add
`DQ_LDL_REPO_PATH` for T2, T3 and the benchmarks. Set `AIRFLOW_API_PORT=8090` in `.env` if something else uses 8080.
[`.env.example`](../../.env.example) lists every setting with its default and [configuration](configuration.md) explains
each; a unit test fails if a setting is missing from it. Optional: `uv run pre-commit install` runs the hooks on commit.

## Test tiers at a glance

| Tier | Marker | What runs | Needs | Command | Time |
|---|---|---|---|---|---|
| T0 | `unit` | policy, models, rendering, sinks, notifiers, settings, CLI, DAG wiring, docs tooling, with fakes for every port | Python | `make test-unit` | about 4 s |
| T1 | `integration` | real Soda Core against in-memory DuckDB built from `fixtures/`: good batch passes, bad batch blocks every layer | Python | `make test-integration` | about 4 s |
| T2 | `live` | gates on the running light lakehouse (Lakekeeper + RustFS) with host DuckDB, good and bad batch | Docker, `DQ_LDL_REPO_PATH` | `make e2e-live` | 25 s to 1.5 min |
| T3 | `full` | lakehouse Spark pipeline, Trino gates, the `ldl-soda` container, the Airflow good and bad runs | Docker (8 GB), `DQ_LDL_REPO_PATH` | `make e2e-full` | 17 to 30 min |

pytest selects T0 and T1 by default (`addopts = -m 'not live and not full'`); CI runs exactly those plus lint, mypy and
the docs checks. The `live` and `full` tests skip with the reason (for example `Lakekeeper is not reachable at
http://localhost:8181`) when their part of the stack is not running, so `pytest -m "live or full"` is safe anywhere.

## T0 and T1: no Docker

```bash
make test-all
```

Runs, in order: `ruff format --check` and `ruff check`, `mypy --strict`, pytest T0 + T1 with coverage, `soda-dq validate`
on every contract, then the demo (good batch on CSV fixtures, then the bad batch layer by layer).

Pass criteria:

- `All checks passed!` from ruff and `Success: no issues found` from mypy.
- pytest: `110 passed, 7 deselected` (the 7 are the live and full tests) and coverage of at least 95%.
- `validate`: every contract reported valid.
- Demo good: three `PASSED` gates, exit 0. Demo bad: `OK: bronze|silver|gold blocked as expected`.
- Last line: `test-all: PASS (...)`.

Gate reports land in `reports/` as Markdown, JSON and JUnit XML.

## T2: live light stack

```bash
make e2e-live
```

[`scripts/e2e.sh`](../../scripts/e2e.sh) `live` starts the lakehouse light profile if it is not already running, then:

1. seeds the good batch into the live Iceberg tables and runs every gate on DuckDB (expects exit 0);
2. seeds the bad batch and verifies each layer (expects exit 1 for bronze, silver and gold);
3. restores the good batch and runs the gates again (expects exit 0);
4. runs `pytest -m live` (good passes, bad blocks every layer, dataset names resolve).

Each step logs to `reports/e2e-live/NN-step.log`. The script ends with a table and `RESULT: PASS` or `RESULT: FAIL`
and exits 1 on any failure. A trap stops only what the script started, with `make down` (never `-v`), so volumes and a
stack you started yourself are left alone. `KEEP_UP=1 make e2e-live` leaves everything running. Running it twice in a
row gives the same result.

## T3: full stack with Trino, the runner container and Airflow

```bash
AIRFLOW_API_PORT=8090 make e2e-full    # drop the port when 8080 is free
```

`scripts/e2e.sh full` runs:

1. `make up-full TRINO=1` and the lakehouse Spark pipeline (`make e2e` in the lakehouse);
2. the gates on the Spark output with DuckDB and with Trino (both exit 0);
3. the bad batch on Trino (every layer exits 1), then restores the good batch;
4. `make dq-up` and `soda-dq run` inside the hardened `ldl-soda` container (exit 0);
5. `make airflow-dq-up`, a good DAG run (success), a fault-injection run (`dq_bronze` fails, so the make target exits 2),
   and a second good run that restores the data;
6. `pytest -m "live or full"` (adds the Trino, container hardening and Airflow health tests).

Teardown stops Airflow, the runner and the lakehouse if the script started them (`down`, volumes kept). Note that
`make airflow-dq-down` stops the whole lakehouse compose project, because Airflow shares it.

To run only the pytest tiers against a stack you already started: `make test-live` or `make test-full`.

## Benchmarks and the accuracy suite

The benchmarks need the full stack with Trino (`make up-full TRINO=1` in the lakehouse, and `make dq-up` for the
container engine). They **overwrite the live retail tables**; reseed with `uv run soda-dq seed fixtures/good_batch --yes`
afterwards. `BENCH_OUT=dir` writes a run to its own folder so it never mixes with the published set in
`docs/results/benchmarks`.

| Target | What it measures | Quick run | Pass criteria |
|---|---|---|---|
| `make bench` | latency, CPU, peak RSS and queries per layer x engine (`duckdb`, `trino`, `container`) at `SIZES`, `WARM` warm repeats | `make bench SIZES="10000 100000" WARM=2 BENCH_OUT=docs/results/runs/<date>` | every row has `status=passed` and no `error` |
| `make bench-accuracy` | defect detection at 0.1, 1 and 10% injected defects against ground truth | `make bench-accuracy BENCH_OUT=...` | every expected check fails and no clean check fails (`correct=True`) |
| `make bench-spark` | Spark job time against gate time on the lakehouse sample | `make bench-spark BENCH_OUT=...` | one row per layer |
| `make bench-tools` | Soda against datacontract-cli, Great Expectations, Pointblank and Pandera, in a pinned venv (`benchmarks/tools/requirements.txt`) | `make bench-tools TOOL_SIZES=100000 BENCH_OUT=...` | `correct=True` for every tool that supports all six checks |
| `make bench-tiny` | the 7-row comparison in [benchmark-tiny](../research/benchmark-tiny/README.md), no Docker | `make bench-tiny` | bad batch fails for every tool; summary in `docs/research/benchmark-tiny/results/` |
| `make bench-report` | `summary.md` from the raw files in `BENCH_OUT` | `make bench-report BENCH_OUT=...` | file written |

`bench-tools` records the exact package versions in `tools_versions.json` next to `tools.csv`. The full published run
(10k to 5M orders) took about two hours; the quick sizes above take about 5 minutes. Method and caveats:
[benchmarks README](../../benchmarks/README.md) and [benchmark results](../benchmarks/results.md).

## Regenerating docs, diagrams and results

| Task | Command | Notes |
|---|---|---|
| Refresh Mermaid copies and shared snippets in the Markdown | `make docs` | sources in `docs/diagrams/*.mmd` and the `region` / `include` markers |
| Check links, anchors, markdownlint, Mermaid and snippet sync, duplicate prose | `make docs-check` | what CI runs |
| Render the SVGs (gitignored) and parse every diagram | `make diagrams`, `make diagrams-check` | uses `npx @mermaid-js/mermaid-cli@11` |
| Refresh verification artefacts | copy `reports/` output into `docs/results/<area>/` and update [results README](../results/README.md) | gate reports, e2e logs, coverage |
| Benchmark summary | `make bench-report` | rewrites `summary.md` in `BENCH_OUT` |

## Make targets

`make help` prints every target, grouped. In short:

| Group | Targets |
|---|---|
| Setup | `env`, `venv`, `lock`, `pre-commit` |
| Quality and tests | `fmt`, `lint`, `typecheck`, `test-unit`, `test-integration`, `test`, `test-all`, `check`, `validate`, `demo` |
| Live | `e2e-live`, `e2e-full`, `test-live`, `test-full`, `list`, `verify`, `run`, `dq-build`, `dq-up`, `dq-down`, `dq-shell`, `airflow-dq-up`, `airflow-dq-trigger`, `airflow-dq-trigger-bad`, `airflow-dq-down` |
| Benchmarks | `bench`, `bench-accuracy`, `bench-spark`, `bench-tools`, `bench-tiny`, `bench-report`, `bench-all` |
| Docs and housekeeping | `docs`, `docs-check`, `diagrams`, `diagrams-check`, `clean`, `dist` |

## Troubleshooting the test paths

| Symptom | Fix |
|---|---|
| `DQ_LDL_REPO_PATH is not set` from `scripts/e2e.sh` | set it in `.env` or the environment to the lakehouse checkout |
| live tests all `SKIPPED ... Lakekeeper is not reachable` | the stack is down: `make up-light` in the lakehouse, or use `make e2e-live` |
| `Bind for 0.0.0.0:8080 failed: port is already allocated` on `airflow-dq-up` | another service holds 8080: `AIRFLOW_API_PORT=8090` |
| Trino gates block with `EXCEEDED_LOCAL_MEMORY_LIMIT` | batch too big for the 1 GiB heap, see [prerequisites](#prerequisites) |
| a `FAIL` row in the e2e summary | open the log it names under `reports/e2e-<mode>/`; rerun, the script is idempotent |
| `bench-tools` cannot resolve the pins | it needs Python 3.12; delete `benchmarks/tools/.venv` and rerun |
| `docs-check` fails on `npx` | install Node.js 20+; the first run downloads markdownlint and mermaid-cli |
| data looks wrong after a benchmark | reseed: `uv run soda-dq seed fixtures/good_batch --yes` |

More symptoms: [troubleshooting](troubleshooting.md).
