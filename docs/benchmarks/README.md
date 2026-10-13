# Benchmarks

> [!NOTE]
> **At a glance.** On an Apple M1 Pro laptop, a gate on host DuckDB takes **0.4 to 0.6 s per layer up to 1M orders**
> and 0.5 to 2.0 s at 5M, costs **4 to 5 %** of the Spark job time, peaks under **780 MiB**, and got **66 of 66**
> verdicts right on injected defects. Trino on its default 1 GiB heap fails at 5M orders (the gate blocks).

| Page | What it covers |
|---|---|
| [Methodology](methodology.md) | Machine, data generator, engines, runs and how each metric is measured |
| [Results](results.md) | Latency, resources, detection accuracy, overhead against Spark, tool comparison |
| [Analysis](analysis.md) | SLO verdicts, what the numbers mean for the design, capacity planning, caveats |

| Headline | Value |
|---|---|
| Gate latency, host DuckDB, warm p50 per layer | 0.4–0.6 s up to 1M orders · 0.5–2.0 s at 5M |
| Worst warm p95 (DuckDB host or container) | 3.56 s (container, bronze, 5M) |
| Gate time as a share of the Spark job (lakehouse sample) | 4–5 % per layer |
| Peak gate memory at 5M orders | 780 MiB (container), 684 MiB (host) |
| Detection accuracy (7 defect types × 3 rates + clean) | 66/66 verdicts, precision 1.0, recall 1.0 |
| Same six checks at 1M rows | Soda 0.44 s, Pointblank 0.47 s, datacontract-cli 1.08 s, Pandera 2.83 s, GX Core 11.5 s |

**Raw data:** [`docs/results/benchmarks/`](../results/benchmarks/summary.md) (CSV, JSON, gzipped JSON lines and the generated
`summary.md`). **Harness:** [`benchmarks/`](../../benchmarks/README.md); reproduce with `make bench-all`.
