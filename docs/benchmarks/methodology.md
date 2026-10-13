# Benchmark methodology

> [!IMPORTANT]
> Every number comes from real runs on one laptop. Expect about ±20 % between runs; the tables report the
> spread (σ) and the slowest run, not only medians.

## Setup

| Item | Value |
|---|---|
| Machine | Apple M1 Pro, 10 cores, 16 GiB, macOS 26.6.2 |
| Docker | Docker 29.8.1, VM with 10 CPUs and 7.65 GiB; Trino 483 (JVM `-Xmx1G`, the lakehouse default) |
| Software | Python 3.12.9, soda-core 4.26.0, DuckDB 1.5.6 (tool comparison venv: soda-core 4.27.0) |
| Data | `scripts/gen_scale_data.py`, seed 42: 10k, 100k, 1M, 5M orders in 5 files, customers = orders / 10, 90 days |
| Engines | `duckdb` (host CLI process), `trino` (host process, Trino does the work), `container` (`ldl-soda`, read-only, DuckDB inside) |
| Runs | Per size × engine × layer: 1 cold (first after the seed) + 5 warm. Every run is a new process |
| Timings | `gate_s` = `VerifyLayerUseCase.execute` wall time (sinks off); `process s` adds interpreter start and imports |
| Stats | p50 = median of 5; **p95 = nearest rank of 5, which is the slowest warm run**; σ = population std-dev of 5 |
| Not controlled | Laptop background load, Docker Desktop VM, thermal state. Expect ±20 % run to run; see σ |

Each gate issues a fixed number of SQL queries regardless of size: **bronze 4, silver 5, gold 3** (Soda
batches the checks of a dataset into one aggregate query plus one per reference or duplicate check). Trino's
server-side count matched the client count in every run.

## How each metric is measured

| Metric | Definition |
|---|---|
| `gate_s` | Wall time of `VerifyLayerUseCase.execute` inside the probe process, with sinks off |
| `process_s` | `gate_s` plus interpreter start-up and imports: what an Airflow task sees |
| cold / warm | Cold is the first run after the data was written; warm are the next five. Every run is a fresh process |
| Peak RSS, CPU | `getrusage` of the gate client process (Soda + DuckDB, or the Trino *client*) |
| Trino server CPU, scan bytes, peak memory | Trino's `/v1/query` API, summed over the gate's queries |
| SQL queries | Soda's `SQL query` debug records; for Trino they matched the server's own count |
| Accuracy | Defects injected by [`scripts/gen_scale_data.py`](../../scripts/gen_scale_data.py) with `ground_truth.json`; expected vs actual gate status and failing checks |

## Reproduce

| Command | Produces |
|---|---|
| `make bench` | `perf_raw.csv`, `perf_raw.jsonl`, `pipeline_raw.csv`, `specs.json` |
| `make bench-accuracy` | `accuracy.csv`, `accuracy.jsonl` |
| `make bench-spark` | `spark_reference.csv` |
| `make bench-tools` | `tools.csv`, `tools_versions.json` (separate venv) |
| `make bench-report` | `summary.md` |

> [!WARNING]
> The benchmarks overwrite the live retail tables. Restore them afterwards with
> `soda-dq seed fixtures/good_batch --yes` or the lakehouse's `make e2e`.

---

[← Benchmarks index](README.md) · [Benchmarks index](README.md) · [Results →](results.md)
