# Quickstart

The one canonical setup flow. The README copies these steps; [running and testing](running-and-testing.md) covers every
test tier, make target and benchmark in depth.

## Run it locally (no Docker)

<!-- region:quickstart -->
1. **Check the prerequisites:** [uv](https://docs.astral.sh/uv/), Git and Make. Docker is only needed for the live steps.

   ```bash
   uv --version && git --version && make --version | head -1
   ```

   Expected: three version lines (uv 0.5 or newer). uv installs Python 3.12 itself if it is missing.

2. **Clone the repository.**

   ```bash
   git clone https://github.com/santoshshinde2012/lakehouse-quality-gates.git && cd lakehouse-quality-gates
   ```

   Expected: a `lakehouse-quality-gates` folder, now your working directory.

3. **Create your `.env`.**

   ```bash
   make env
   ```

   Expected: `.env ready`. The defaults work for the local demo; every setting is documented in `.env.example`.

4. **Install.**

   ```bash
   make venv
   ```

   Expected: a `.venv` with Python 3.12, Soda Core 4.26, DuckDB and the dev tools, built from `uv.lock`.

5. **Validate the contracts.**

   ```bash
   make validate
   ```

   Expected: `ok` for each of the five contracts, exit code 0.

6. **Run the good batch.**

   ```bash
   make demo-good
   ```

   Expected: `retail/bronze`, `retail/silver` and `retail/gold` each report `passed` (53 checks in total).

7. **Run the bad batch.**

   ```bash
   make demo-bad
   ```

   Expected: `OK: bronze blocked as expected`, then the same for silver and gold. The reasons are in `reports/*.md`.
<!-- /region -->

## Run against the live lakehouse

<!-- region:live -->
1. **Clone [local-data-lakehouse](https://github.com/santoshshinde2012/local-data-lakehouse) next to this repository.**

   ```bash
   git clone https://github.com/santoshshinde2012/local-data-lakehouse.git ../local-data-lakehouse
   ```

   Expected: `../local-data-lakehouse` exists. Docker must be running, with at least 2 GB for this step.

2. **Point `.env` at it.**

   ```bash
   echo "DQ_LDL_REPO_PATH=../local-data-lakehouse" >> .env
   ```

   Expected: no output; `scripts/e2e.sh` and the live tests read this path.

3. **Run the live end-to-end check.**

   ```bash
   make e2e-live
   ```

   Expected: a summary table ending in `RESULT: PASS` (9 steps, about a minute). The good batch passes on the live
   Iceberg tables, the bad batch is blocked at every layer, and the stack is stopped again with its volumes kept.
<!-- /region -->

## Full stack (Trino, Docker runner, Airflow)

<!-- region:full -->
1. **Run the full end-to-end check** (give Docker 8 GB).

   ```bash
   make e2e-full                      # or: AIRFLOW_API_PORT=8090 make e2e-full, if port 8080 is taken
   ```

   Expected: `RESULT: PASS` after 15 steps (17 to 30 minutes): the Spark pipeline, Trino gates, the `ldl-soda`
   runner container, an Airflow run that succeeds and one that stops at `dq_bronze`.
<!-- /region -->

To drive the pieces yourself (`make dq-up`, `make airflow-dq-up`, `make airflow-dq-trigger`), see the
[runbook](runbook.md#daily-operation).

## Write-audit-publish (bad data never reaches main)

<!-- region:wap -->
1. **Start the lakehouse with the WAP overlay**, then publish a good batch through audit branches.

   ```bash
   make wap-up
   make wap-demo-good
   ```

   Expected: bronze, silver and gold each `passed`, and one line per table such as
   `published bronze.orders_raw: main 6960… -> 6313… (22 rows)`.

2. **Send the bad batch.**

   ```bash
   make wap-demo-bad
   ```

   Expected: `retail/bronze: blocked`, `main stays <same id>`, the audit branch kept with the bad rows, and
   `OK: blocked as expected, main untouched`.

3. **Run it all with Airflow and the WAP tests.**

   ```bash
   make e2e-wap                       # or: AIRFLOW_API_PORT=8090 make e2e-wap
   ```

   Expected: `RESULT: PASS` after 11 steps (about 12 minutes), including an Airflow run where `dq_bronze` fails and
   `publish_bronze` never runs.
<!-- /region -->

## What next

- Read a report: `reports/<run>_retail_<layer>.md` lists every check and why a gate blocked.
- Write or change a check: [contracts guide](contracts.md).
- Commands, exit codes and settings: [configuration](configuration.md).
