# Benchmark analysis

What the [results](results.md) mean, where the time goes, and how far to trust them. All numbers come from the
raw data in [`docs/results/benchmarks/2026-10-14-v0.4.1/`](../results/benchmarks/2026-10-14-v0.4.1/specs.json)
(commit e39dc18, Apple M1 Pro, Docker with 10 CPUs and 8 GB).

## Findings

1. **Host DuckDB and the container are the right default engines.** A single gate takes 0.42–0.68 s (warm p50) up to
   1M orders and at most 1.36 s at 5M. All three gates together take 3.09 s at 5M orders.
2. **Trino works at 5M only with more heap.** With the 4 GiB override (`make trino-heap`) every Trino gate passed at
   5M, but the three gates take 29.79 s, about 9.6x DuckDB, and the bronze gate has a long tail (p95 30.14 s).
3. **The gate is not the bottleneck of a real pipeline.** Against the Spark jobs, a gate adds 2.3–4.9 % (median per
   layer). In the Airflow DAG the three gate tasks are about 6 s of a 124 s good run.
4. **The gate fails fast.** A bad batch stops the DAG at `dq_bronze` after about 57 s; silver, gold and time travel
   are never built.
5. **Detection is exact for the encoded defects.** 66 of 66 verdicts were right across 7 defect types at 3 rates
   plus clean batches.
6. **Soda is the fastest of the five tools on the same six checks** (0.39 s at 1M rows); Pandera uses the most
   memory (1527 MiB) and GX Core is the slowest (11.40 s).

## Scaling

| Engine | 10k → 1M | 1M → 5M | Shape |
|---|---|---|---|
| Host DuckDB | flat (fixed cost ≈ 0.4–0.5 s) | bronze and silver about 2x | Fixed start-up cost, then roughly linear scan cost |
| Container (`ldl-soda`) | flat | bronze about 4x | As DuckDB, plus container start and tmpfs spill |
| Trino | about 3–5x for bronze and silver | about 4x | Linear from 100k; server-side, so client memory stays at ~91 MiB |

Client memory is ~130–180 MiB up to 100k orders and grows with the duplicate check on bronze: 685 MiB (host) and
690 MiB (container) at 5M.

## Bottlenecks

- **Fixed start-up cost** dominates below 1M: Python import, Soda contract parsing and the DuckDB connection.
- **Full scans per check family**: each contract issues a constant number of queries (4 / 5 / 3 for bronze / silver /
  gold). A reference or duplicate check adds one more full scan.
- **Trino memory**: on its default 1 GiB heap Trino fails at 5M with `EXCEEDED_LOCAL_MEMORY_LIMIT`. Even with 4 GiB
  heap, the bronze tail is long because of garbage collection.
- **Seed-to-gold at 5M**: the layer builds (silver build 41 %, bronze build 31 %) cost more than the three gates (24 %).

## Recommendations

1. Use host DuckDB or the container for batches up to at least 5M orders; budget ~1 GiB of memory per gate process.
2. To use Trino past 1M orders, run `make trino-heap`, which gives a 4 GiB heap and sets `query.max-memory-per-node`
   to 2.5 GB. This is a compose override in this repo; the lakehouse's files are not changed. Undo it with
   `make trino-heap-reset`.
3. Scope contracts to the batch with a partition `filter`, so cost grows with the batch and not the table.
4. Run the gates of a layer in parallel before adding engines; they are independent.
5. For very large batches, run `soda-sparkdf` inside the Spark job to avoid a second read.

## Threats to validity

- **One machine.** One laptop with other work running alongside. Repeat runs vary by about ±20 %.
- **Few repeats.** 5 warm runs and 1 cold run per cell, and p95 of 5 samples is close to the maximum.
- **Sleep and battery.** The Mac slept during the first Spark run and silver took 166–279 s. That file was discarded
  and `make bench-spark` was rerun; only the rerun is published.
- **Builds at scale.** At 1M and 5M orders the layers were built by DuckDB running the Spark SQL, not by Spark. The
  Spark comparison uses the lakehouse sample, where Spark start-up makes the gate look cheap.
- **Airflow wall clock.** DAG durations include scheduler latency and container start, not just task work.
- **Trino override.** The Trino numbers at all sizes use the 4 GiB heap, not the lakehouse default.
- **Accuracy scope.** The suite injects the defects the contracts were written for, one type per batch. It shows
  the gates enforce the contracts, not that the contracts cover every defect.
- **Tool versions.** The tool comparison ran in its own environment (see `tools_versions.json`).

---

[← Results](results.md) · [Benchmarks index](README.md) · [System design →](../system-design.md)
