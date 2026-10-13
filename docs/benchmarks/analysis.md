# Benchmark analysis

## SLO verdicts

| ID | Area | Target (SLO) | Measured (the [benchmarks](results.md)) |
|---|---|---|---|
| N1 | Gate latency | p95 ≤ 10 s per layer up to 1M orders, ≤ 30 s at 5M, on the reference laptop | **Met** for host DuckDB and the container at every size (worst warm p95 3.56 s at 5M). Trino met up to 1M (worst p95 7.69 s, one outlier) and **fails** at 5M on the default 1 GiB heap (the [benchmarks](results.md)) |
| N2 | Overhead | A gate costs less than its layer's build at ≥ 1M rows | **Met against Spark** (4 to 5 %). Against the DuckDB writer met for bronze and silver at ≥ 1M (37 to 60 %); not met for gold, whose build is a 90-row aggregate (0.16 to 0.55 s) |
| N3 | Correctness | Recall 1.0 on blocking defect categories; 0 false blocks on clean data | **Met**: 30/30 expected failing or warning checks raised, 0 unexpected, 0 false blocks on clean data |
| N4 | Memory | Gate client peak RSS ≤ 2 GiB at 5M orders (fits the 8 GiB Docker VM next to Spark and Trino) | **Met**: 684 MiB host, 780 MiB container at 5M |
| N5 | Availability | A gate that cannot run **blocks** (fail closed); no silent pass | By design: execution errors and NOT_EVALUATED block |
| N6 | Security | No long-lived storage keys; non-root, read-only container; secrets never logged | [Security](../architecture/security.md) |
| N7 | Portability | One contract set for every engine; Python 3.12; macOS and Linux | CI on Ubuntu, live runs on macOS |
| N8 | Maintainability | `ruff`, `mypy --strict`, ≥ 95% line coverage | 103 tests at 99% coverage |

## What the numbers mean for the design

1. Host DuckDB or the container are the right default engines up to at least 5M orders per batch on a
   laptop; Trino adds 0.3 to 2 s of overhead and needs memory tuning past 1M.
2. The gate is a small fraction of a real Spark pipeline; it is not the bottleneck.
3. Fixed costs dominate below 1M, so splitting a layer into more gates is cheap, but more contracts per
   gate add queries (one aggregate per dataset plus one per reference or duplicate check).
4. Detection is exact for the defects the contracts encode; coverage, not accuracy, is the risk (FMEA 5).

## Capacity planning

Fitted from the [benchmarks](results.md) (host DuckDB, warm p50), with the caveat that four sizes and one machine make
this an estimate, not a model:

| Quantity | Observation | Rule of thumb |
|---|---|---|
| Gate latency | ≈ 0.4 to 0.6 s fixed; +0.2 to 0.35 s per million orders for bronze and silver above 1M | Up to ~20M orders per batch stays under 10 s per layer if the trend is linear (not measured past 5M) |
| Client memory | 120 MiB base; about +100 to +120 MiB per million orders for bronze (duplicate check) | Budget 1 GiB per gate process per 7M orders; the container's tmpfs `/tmp` spill dir must be sized to match |
| Trino | Default 1 GiB heap fails at 5M orders | Untested suggestion: ≥ 4 GiB heap and a matching `query.max-memory-per-node` before using Trino past 1M |
| Queries | Constant per contract (4 / 5 / 3) | Adding a reference or duplicate check adds one full scan of that dataset |
| Storage of results | One JSON and one Markdown report per gate and one row per check in `ops.dq_results` | Grows with checks × runs, not with data size |

Scaling options in order of cost: (1) scope contracts to the batch with a partition `filter` so cost grows
with the batch, not the table (L4); (2) run the three engines in parallel per contract; (3) move large
batches to Trino with more memory or to `soda-sparkdf` inside the Spark job, which removes the second read
entirely.

## Caveats

- One laptop, with other work running alongside; four batch sizes. Treat the capacity rules as estimates.
- At 1M and 5M orders the layers were built by DuckDB running the Spark SQL, not by Spark; the Spark
  comparison is from the lakehouse's small sample, where Spark's start-up time makes the gate look cheap.
- The accuracy suite injects the defects the contracts were written for, one type per batch. It shows the
  gates enforce the contracts; it does not show that the contracts cover every possible defect.
- The tool comparison ran soda-core 4.27 in its own environment; the gate benchmarks ran 4.26.

---

[← Results](results.md) · [Benchmarks index](README.md) · [System design →](../system-design.md)
