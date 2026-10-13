# Fresh-clone run, 2026-10-13

Every documented path, run from a fresh `git clone` of commit `51cf981` into `/tmp/lqg-complete`, following only
[running and testing](../../../guides/running-and-testing.md). macOS 26 (M1 Pro), Docker Desktop 29.8 with 10 CPUs and
8 GB, local-data-lakehouse `main`, Airflow on port 8090 (8080 was taken by another service).

| Path | Command | Result | Time |
|---|---|---|---|
| T0 + T1 | `make test-all` | PASS: ruff, mypy, 108 passed / 7 deselected, 99% coverage, contracts valid, demo good passes, bad blocks 3/3 | 22 s |
| T2 | `make e2e-live` | PASS: 9/9 steps, `pytest -m live` 3 passed ([logs](e2e-live/)) | 25 s |
| T3 | `AIRFLOW_API_PORT=8090 make e2e-full` | PASS: 15/15 steps, Airflow bad run failed at `dq_bronze` as intended, `pytest -m "live or full"` 7 passed ([logs](e2e-full/)) | 17 min |
| Performance | `make bench SIZES="10000 100000" WARM=2 BENCH_OUT=...` | 54 gate runs, all `passed` (3 engines x 3 layers x 2 sizes x 3 runs) | 2 min |
| Accuracy | `make bench-accuracy BENCH_OUT=...` | 66/66 verdicts correct, 0 false positives, 0 false negatives | 1 min |
| Tools | `make bench-tools TOOL_SIZES=100000 BENCH_OUT=...` | 15 runs, all five tools `correct=True` | 1 min |
| Tiny | `make bench-tiny` | bad batch fails for every tool ([results](../../../research/benchmark-tiny/results/summary.csv)) | 13 s |

Raw files and the generated [summary](summary.md) are in this folder. These quick sizes check that the harness runs; the
published 10k to 5M numbers stay in [docs/results/benchmarks](../../benchmarks/summary.md).
