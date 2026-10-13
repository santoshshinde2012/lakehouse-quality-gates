# Benchmarks

Real measurements of the quality gates on the live lakehouse. Results land in
[`docs/results/benchmarks/`](../docs/results/benchmarks/); the analysis is section 12 of
[`docs/system-design.md`](../docs/system-design.md).

| Harness | What it measures | Command |
|---|---|---|
| `scripts/gen_scale_data.py` | Deterministic retail batch at any size, optional defect injection with ground truth | `python scripts/gen_scale_data.py --rows 1000000 --out /tmp/b` |
| `benchmarks/run_perf.py` | Gate wall time (cold + warm), CPU, peak RSS, SQL queries per layer and engine; Trino server CPU and scan bytes; per-layer build time | `make bench` |
| `benchmarks/run_accuracy.py` | Each defect category at 0.1%, 1%, 10%: gate verdicts and reported counts against ground truth | `make bench-accuracy` |
| `benchmarks/spark_reference.py` | Spark job time per layer on the lakehouse sample vs gate time | `make bench-spark` |
| `benchmarks/tools/run_tools.py` | Same six checks in Soda, datacontract-cli, GX Core, Pointblank, Pandera at 100k and 1M rows | `make bench-tools` |
| `benchmarks/analyze.py` | Writes `summary.md` tables from the raw CSVs | `make bench-report` |

Prerequisites: the lakehouse full profile with Trino (`make up-full TRINO=1` in `local-data-lakehouse`) and
`make dq-up`. The benchmarks **overwrite the live retail tables**; restore them with
`soda-dq seed fixtures/good_batch --yes` or the lakehouse's `make e2e`.

How each number is taken:

- **gate_s**: `VerifyLayerUseCase.execute` wall time inside the probe process (sinks off). **process_s**
  adds interpreter start-up and imports, as an Airflow task would see it.
- **cold** is the first run after the seed; **warm** are the next five. Every run is a new process, so
  DuckDB has no cross-run cache; warm mainly benefits from OS, object store and Trino caches.
- **max_rss_bytes / cpu**: `getrusage(RUSAGE_SELF)` of the client process (the Soda + DuckDB process, or
  the Trino *client* for `trino`). Trino's own CPU, scan bytes and peak memory come from its REST API
  (`/v1/query`), summed over the gate's queries.
- **queries**: Soda's `SQL query ...` debug records; for Trino they match the server's query count.
- Percentiles are nearest-rank over five warm runs, so p95 is the slowest warm run.
- The per-run JSON lines (every check result) are committed gzipped as `perf_raw.jsonl.gz` to stay under the
  500 KB pre-commit limit; `run_perf` writes plain `perf_raw.jsonl`, gzip it before committing.
