# Fresh-clone run, v1.0.0, 2026-10-14

Every documented path, run from a fresh `git clone` of commit `30d7962` into `SODA-fresh`, with only `.env` copied in
(`DQ_LDL_REPO_PATH`). macOS (M1 Pro), Docker Desktop, local-data-lakehouse `main` (unmodified), Airflow on port 8090.
Status per step with clock times (IST) is in [status.txt](status.txt).

| Path | Command | Result | Time |
|---|---|---|---|
| T0 + T1 | `make test-all` | PASS: ruff, mypy, 132 passed / 1 skipped / 8 deselected, 96% coverage, contracts valid, demo good passes, bad blocks ([log](test-all.log)) | 1 min |
| T2 | `make e2e-live` | PASS: 9/9 steps, `pytest -m live` 3 passed ([logs](e2e-live/), [summary](e2e-live.log)) | 24 s |
| T3 | `make e2e-full` | PASS: 15/15 steps, Airflow bad run failed at `dq_bronze` as intended, `pytest -m "live or full"` 8 passed ([logs](e2e-full/), [summary](e2e-full.log)) | 13 min |
| T4 | `make e2e-wap` | PASS: 11/11 steps, CLI and Airflow good batches published, bad batches blocked with `main` untouched, `pytest -m full -k wap` 1 passed ([logs](e2e-wap/), [summary](e2e-wap.log)) | 11.5 min |
| Docs | `make docs-check` | PASS ([log](docs-check.log)) | 2 s |
| Diagrams | `make diagrams-check` | PASS: all diagrams and benchmark charts render ([log](diagrams-check.log)) | 29 s |
| Hooks | `pre-commit run --all-files` | PASS ([log](pre-commit.log)) | 7 s |
