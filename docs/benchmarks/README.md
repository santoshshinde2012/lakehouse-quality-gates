# Benchmarks

> [!NOTE]
> **At a glance.** On an Apple M1 Pro laptop, a gate on host DuckDB takes **0.42–0.68 s per layer up to 1M orders**,
> all three gates take **3.09 s at 5M orders**, a gate adds **2.3–4.9 %** to its Spark job, memory peaks at
> **690 MiB**, and **66 of 66** verdicts were right. Trino needs the 4 GiB heap override at 5M orders.

| Page | What it covers |
|---|---|
| [Methodology](methodology.md) | Machine, data generator, engines, runs and how each metric is measured |
| [Results](results.md) | Mermaid charts and tables: latency, memory, pipeline, overhead, Airflow, accuracy, tools |
| [Analysis](analysis.md) | Findings, scaling, bottlenecks, recommendations, threats to validity |

<!-- region:headline -->
| Headline | Value |
|---|---|
| Gate latency, host DuckDB, warm p50 per layer | 0.42–0.68 s up to 1M orders |
| All three gates at 5M orders, warm p50 | 3.09 s host DuckDB · 29.79 s Trino (4 GiB heap) |
| Worst warm p95, DuckDB host or container (5M) | 3.47 s |
| Gate time as a share of its Spark job | 2.3–4.9 % per layer |
| Peak gate memory at 5M orders | 690 MiB (container), 685 MiB (host) |
| Detection accuracy (7 defect types × 3 rates + clean) | 66/66 verdicts |
| Same six checks at 1M rows | soda 0.39 s, pointblank 0.44 s, datacontract 0.89 s, pandera 3.34 s, gx 11.40 s |
<!-- /region -->

**Raw data:** [`docs/results/benchmarks/2026-10-14-v0.4.1/`](../results/benchmarks/2026-10-14-v0.4.1/specs.json) (CSV, JSON and gzipped JSON lines). **Harness:** [`benchmarks/`](../../benchmarks/README.md); reproduce with `make bench-all`.
