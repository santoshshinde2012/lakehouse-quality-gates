# Verification results

Evidence for every claim in the docs: logs, gate reports and benchmark data from real runs. Each folder below was
produced by the make target listed next to it; only the README files were written by hand. Pages elsewhere cite these
files, and `make docs-check` fails if a quoted headline number no longer matches them (`scripts/check_numbers.py`).

## Runs, newest first

| Run | Date | Commit | What it shows | Reproduce |
|---|---|---|---|---|
| [Enterprise source review](runs/2026-10-14-v1.0-review/README.md) | 2026-10-14 | `9cd3ed3` + `codex/enterprise-verification` working tree | 359 tests, 97% line coverage, 53 checks; live 9/9, full 15/15, WAP 11/11; real history readback and first-use bootstrap; gate reports, audit manifests, exact Airflow snapshots, JUnit, coverage, source hashes and checksums | `make test-all e2e-live e2e-full e2e-wap` |
| [PR validation](runs/2026-10-14-v1.0-pr-checks/README.md) | 2026-10-14 | `2200222` + `codex/harden-quality-gates-wap` working tree | 216 tests, 97% coverage, 53 contract checks after the CLI reporting fix; lint, strict types, contract syntax and fixture demos; live tiers were not rerun | `make test-all` |
| [Hardening checks](runs/2026-10-14-v1.0-hardening/test-all.log) | 2026-10-14 | `2200222` + working tree | 214 tests, 97% coverage, 53 contract checks; [live 9/9](runs/2026-10-14-v1.0-hardening/e2e-live.log), [full 15/15](runs/2026-10-14-v1.0-hardening/e2e-full.log), [WAP 11/11](runs/2026-10-14-v1.0-hardening/e2e-wap.log); failure and concurrency regressions | `make test-all e2e-live e2e-full e2e-wap` |
| [Write-audit-publish](runs/2026-10-14-wap/README.md) | 2026-10-14 | v1.0.0 (pre-release tree) | Good batch published to `main` (snapshot ids before and after), bad batch blocked with `main` unchanged and the branch kept; CLI, Airflow and the live test | `make e2e-wap` |
| [v1.0 fresh-clone run](runs/2026-10-14-v1.0-fresh-clone/README.md) | 2026-10-14 | `30d7962` (v1.0.0) | test-all, e2e-live 9/9, e2e-full 15/15, e2e-wap 11/11, docs, diagrams and pre-commit from a fresh clone |
| [WAP overhead](benchmarks/2026-10-14-wap/wap_overhead.csv) | 2026-10-14 | v1.0.0 (pre-release tree) | Direct write against audit branch + gate + fast-forward at 100k and 1M orders, charted in [results](../benchmarks/results.md#write-audit-publish-overhead) | `make bench-wap` |
| [v0.5.0 benchmarks](benchmarks/2026-10-14-v0.4.1/specs.json) | 2026-10-14 | `e39dc18` (v0.4.1) | Own end-to-end benchmarks charted in [results](../benchmarks/results.md): latency 10k–5M on DuckDB, Trino (4 GiB heap) and the container, memory, seed-to-gold pipeline, Spark overhead, Airflow good/bad runs, accuracy, five-tool comparison | `make bench-all` |
| [v0.4.1 checks](runs/2026-10-14-v0.4.1-checks/README.md) | 2026-10-14 | v0.4.1 | `make test-all` after the documentation tidy: 110 tests, 99% coverage, demo | `make test-all` |
| [Fresh-clone run](runs/2026-10-13-fresh-clone/README.md) | 2026-10-13 | `51cf981` (v0.4.0) | Every documented path from a new clone: `test-all`, `e2e-live`, `e2e-full`, benchmarks at 10k/100k, `bench-tiny` | `make test-all e2e-live e2e-full`, then `make bench SIZES="10000 100000" WARM=2 BENCH_OUT=...`, `make bench-accuracy`, `make bench-tools TOOL_SIZES=100000` |
| [Earlier benchmarks](benchmarks/summary.md) (superseded) | 2026-10-13 | `60326e4` (v0.3.0) | Gate latency, resources, accuracy, Spark overhead and the tool comparison at 10k to 5M orders; analysed in [docs/benchmarks](../benchmarks/README.md) | `make bench-all` (about two hours) |
| [Component verification](runs/2026-10-13-v0.2-verification/README.md) | 2026-10-13 | `39b76a4` (v0.2.1) | Each component on its own: lint, types, tests, demo, live DuckDB and Trino, the runner image, Airflow good and bad runs, with every gate report | the manual walkthrough in the [runbook](../guides/runbook.md#manual-walkthrough) |
| [Archive](archive/README.md) | 2026-10-13 | | Superseded artefacts kept for the record (early Airflow debugging runs) | |

All runs used the same machine: Apple M1 Pro (10 cores, 16 GiB), macOS 26.6, Docker Desktop 29.8 with 10 CPUs and
8 GB, Python 3.12, local-data-lakehouse `main` (Lakekeeper, RustFS, Spark 4.1.3, Trino 483, Airflow 3). Gate and live-stack runs used Soda Core 4.26.0;
the five-tool benchmark used its separate pinned environment, including Soda 4.27.0 (see `tools_versions.json`).

## What each folder contains

| Folder | Contents |
|---|---|
| `runs/2026-10-14-v1.0-review/` | Full attachment index, raw workflow logs and reports, pytest and coverage XML, snapshot manifests, Airflow before/after state, runtime/source fingerprints, checksum manifest |
| `runs/2026-10-14-v1.0-pr-checks/` | `README.md` (validation scope and base), unchanged `test-all.log` from the final PR checks |
| `runs/2026-10-14-v0.4.1-checks/` | `test-all.log` |
| `runs/2026-10-13-fresh-clone/` | `test-all.log` (tests and coverage), `e2e-live/` and `e2e-full/` (one log per step), benchmark CSV/JSON and the generated `summary.md` |
| `runs/2026-10-14-wap/` | Evidence JSON of the CLI runs, Airflow task states, `main` refs before and after the bad run |
| `benchmarks/2026-10-14-wap/` | `wap_overhead.csv` |
| `benchmarks/2026-10-14-v0.4.1/` | Current benchmark data: `perf_raw`, `pipeline_raw`, `spark_reference`, `airflow_runs`, `airflow_tasks`, `accuracy`, `tools` (CSV; JSON lines gzipped), `specs.json`, `tools_versions.json` |
| `benchmarks/` (top level) | Superseded v0.3.0 benchmark data: raw CSV and JSON lines, machine `specs.json`, `tools_versions.json`, generated `summary.md` |
| `runs/2026-10-13-v0.2-verification/` | `logs/`, `gates/` (JSON, Markdown and JUnit report per gate), `docker/`, `airflow/`, `tests/` (JUnit) and `coverage/coverage.xml` |
| `archive/` | Superseded files, each with a note on why it was kept |

## Reproduce

Commands, expected output and pass criteria for every target: [running and testing](../guides/running-and-testing.md).
Write a new run to its own folder (`BENCH_OUT=docs/results/runs/<date>-<name>` for benchmarks). For each end-to-end
mode, copy the contents of the run directory printed in its summary to `<date>-<name>/e2e-<mode>/`, preserving
only that invocation's step logs. Add a row at the top of the table above.
