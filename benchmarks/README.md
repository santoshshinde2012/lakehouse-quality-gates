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

The five-tool comparison runs locally without Docker and does not overwrite lakehouse tables.
Use a separate output directory to retain each measurement set:

```bash
make bench-tools TOOL_SIZES='100000 1000000' REPEATS=5 BENCH_OUT=/tmp/lqg-tool-run
```

Here, `run_s` includes tool imports, Parquet loading, setup, validation, and diagnostic mapping;
`process_s` also includes interpreter startup and shutdown. Each size includes five deliberately
injected defect categories at roughly 0.1% each. These timings include failure handling. The harness
verifies actual fixture sizes and ground-truth counts, interleaves tools with rotating starts,
and scores six identified pass/fail decisions. Diagnostic counts use each tool's own semantics.
Soda also runs an unscored row-count check. Backend choices differ, and no equal CPU or thread
budget is imposed. Fresh processes do not establish cold operating-system caches.

Alongside `tools.csv`, each run saves exact versions, `tools_metadata.json`, `tools_raw.jsonl`,
per-process logs, and the executed harness snapshot under `tools_logs/`. Metadata records timestamps,
the source revision and dirty state, fixture hashes and counts, execution order, and result hashes.
Failed processes, incomplete results, or incorrect scored verdicts make the command exit nonzero.
GX results are matched by explicit check identity because its result order can differ from the
declaration order. Historical archives retain the original harness's positional mapping and should
not be used as correctly labeled GX diagnostic counts.
