# lakehouse-quality-gates: Soda Core v4 contract gates for local-data-lakehouse (see README.md).
SHELL := /bin/bash
UV ?= uv
RUN := $(UV) run --frozen
SODA_HOME := $(abspath .)
-include .env
export
DQ_LDL_REPO_PATH ?= $(abspath ../local-data-lakehouse)
# Host port for the Airflow UI/API. The lakehouse default is 8080; override when it is taken,
# e.g. `make airflow-dq-up AIRFLOW_API_PORT=8090` (also settable in .env).
AIRFLOW_API_PORT ?= 8080
export AIRFLOW_API_PORT
LDL_COMPOSE := docker compose -f docker-compose.yml -f docker-compose.airflow.yml \
	-f $(SODA_HOME)/docker/docker-compose.airflow-dq.yml --profile full

.PHONY: test-all test-live test-full e2e-live e2e-full bench-tiny tools-venv bench-airflow bench-charts trino-heap trino-heap-reset bench bench-accuracy bench-spark bench-tools bench-report bench-all help env venv lock fmt lint typecheck test test-unit test-integration check validate list \
	demo demo-good demo-bad verify run pre-commit dq-build dq-up dq-down dq-shell \
	airflow-dq-up airflow-dq-trigger airflow-dq-trigger-bad airflow-dq-down diagrams diagrams-check docs docs-check clean dist

help:
	@echo "lakehouse-quality-gates: Soda Core data contracts as medallion quality gates"
	@echo "Full guide: docs/guides/running-and-testing.md"
	@echo ""
	@echo "Setup"
	@echo "  make env                .env from .env.example (set DQ_LDL_REPO_PATH for the live tiers)"
	@echo "  make venv               uv sync (all extras + dev group) into .venv"
	@echo "  make lock               refresh uv.lock"
	@echo "  make pre-commit         every pre-commit hook on all files"
	@echo ""
	@echo "Quality and tests (no Docker)"
	@echo "  make fmt | lint         ruff format/fix | ruff format --check + ruff check"
	@echo "  make typecheck          mypy --strict"
	@echo "  make test-unit          T0: unit tests (-m unit)"
	@echo "  make test-integration   T1: real Soda verification on DuckDB + CSV fixtures"
	@echo "  make test               T0+T1 with coverage (what CI runs)"
	@echo "  make test-all           lint + typecheck + T0 + T1 + validate + demo"
	@echo "  make check              lint + typecheck + test"
	@echo "  make validate           parse every contract with Soda (no data access)"
	@echo "  make demo               good batch passes, bad batch is blocked (DuckDB on CSVs)"
	@echo ""
	@echo "Live lakehouse (Docker, DQ_LDL_REPO_PATH)"
	@echo "  make e2e-live           T2: light stack, gates on good + bad data, pytest -m live (scripts/e2e.sh)"
	@echo "  make e2e-full           T3: full stack + Trino + ldl-soda + Airflow, pytest -m 'live or full'"
	@echo "  make test-live | test-full   only the pytest tiers, against a stack that is already up"
	@echo "  make list [ENGINE=...]  contracts and the datasets they resolve to"
	@echo "  make verify LAYER=silver [ENGINE=duckdb|duckdb-local|trino]   one gate"
	@echo "  make run [ENGINE=...]   bronze -> silver -> gold, stop at the first blocked layer"
	@echo "  make dq-build | dq-up | dq-down | dq-shell   runner container ldl-soda on ldl-net"
	@echo "  make airflow-dq-up      lakehouse Airflow + the Soda-gated DAG"
	@echo "  make airflow-dq-trigger trigger lakehouse_retail_medallion_dq and wait"
	@echo "  make airflow-dq-trigger-bad  same, bad batch injected (dq_bronze must fail; exits non-zero)"
	@echo "  make airflow-dq-down    stop Airflow + lakehouse (volumes kept)"
	@echo "  AIRFLOW_API_PORT=8080   Airflow host port (override if taken, e.g. 8090)"
	@echo ""
	@echo "Benchmarks (live stack; BENCH_OUT=dir keeps a run apart from docs/results/benchmarks)"
	@echo "  make bench [SIZES='10000 100000'] [WARM=5]   gate latency/CPU/RSS/queries per layer x engine"
	@echo "  make bench-accuracy     defect detection at 0.1/1/10% against ground truth"
	@echo "  make bench-spark        Spark job time vs gate time"
	@echo "  make bench-tools [TOOL_SIZES='100000 1000000'] [REPEATS=3]   Soda vs other tools (pinned venv)"
	@echo "  make bench-tiny         the 7-row tool comparison in docs/research/benchmark-tiny (no Docker)"
	@echo "  make bench-report       summary.md from the raw results in BENCH_OUT"
	@echo "  make bench-all          all of the above"
	@echo ""
	@echo "Docs and housekeeping"
	@echo "  make docs | docs-check  refresh | check links, markdownlint, Mermaid and snippet sync, quoted numbers"
	@echo "  make diagrams | diagrams-check   render SVGs | parse every .mmd with mermaid-cli"
	@echo "  make clean | dist       caches and reports | SODA.tar.gz without .venv"

env:
	@test -f .env || cp .env.example .env && echo ".env ready"

venv:
	$(UV) sync --all-extras

lock:
	$(UV) lock

fmt:
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

lint:
	$(RUN) ruff format --check .
	$(RUN) ruff check .

typecheck:
	$(RUN) mypy

test-unit:
	$(RUN) pytest -m unit

test-integration:
	$(RUN) pytest -m integration

test:
	$(RUN) pytest --cov=soda_lakehouse_dq --cov-report=term-missing

check: lint typecheck test

test-all: lint typecheck test validate demo
	@echo "test-all: PASS (T0 unit + T1 integration, contracts valid, demo gates behave)"

test-live:
	$(RUN) pytest -m live -v

test-full:
	$(RUN) pytest -m "live or full" -v

e2e-live:
	scripts/e2e.sh live

e2e-full:
	scripts/e2e.sh full

pre-commit:
	$(RUN) pre-commit run --all-files

validate:
	DQ_ENGINE=duckdb-local $(RUN) soda-dq validate

list:
	$(RUN) soda-dq list $(if $(ENGINE),-e $(ENGINE),)

demo: demo-good demo-bad

demo-good:
	@echo "==> good batch (expect: all gates pass)"
	DQ_ENGINE=duckdb-local $(RUN) soda-dq run --sample-dir fixtures/good_batch

demo-bad:
	@echo "==> bad batch, every layer (expect: each gate blocks; non-zero exits are the point)"
	@for layer in bronze silver gold; do \
	  DQ_ENGINE=duckdb-local $(RUN) soda-dq verify $$layer --sample-dir fixtures/bad_batch && \
	    { echo "UNEXPECTED: $$layer passed"; exit 1; } || echo "OK: $$layer blocked as expected"; \
	done
	@echo "Reports: reports/*.md | *.json | *.xml"

verify:
	$(RUN) soda-dq verify $(or $(LAYER),bronze) $(if $(ENGINE),-e $(ENGINE),)

run:
	$(RUN) soda-dq run $(if $(ENGINE),-e $(ENGINE),)

dq-build:
	docker compose -f docker/docker-compose.dq.yml build

dq-up:
	docker compose -f docker/docker-compose.dq.yml up -d --build --wait

dq-down:
	docker compose -f docker/docker-compose.dq.yml down

dq-shell:
	docker exec -it $${LDL_SODA_CONTAINER:-ldl-soda} bash

airflow-dq-up: dq-up
	cd "$(DQ_LDL_REPO_PATH)" && ./pipelines/airflow_env.sh && SODA_HOME="$(SODA_HOME)" $(LDL_COMPOSE) up -d --wait --build

airflow-dq-trigger:
	cd "$(DQ_LDL_REPO_PATH)" && ./pipelines/airflow_trigger.sh lakehouse_retail_medallion_dq

# Fault-injection run: seeds fixtures/bad_batch after the bronze Spark job, so dq_bronze must block
# (the make target exits non-zero on purpose). Restore with a normal `make airflow-dq-trigger`.
airflow-dq-trigger-bad:
	cd "$(DQ_LDL_REPO_PATH)" && ./pipelines/airflow_trigger.sh lakehouse_retail_medallion_dq '{"inject_bad_batch": true}'

airflow-dq-down:
	cd "$(DQ_LDL_REPO_PATH)" && SODA_HOME="$(SODA_HOME)" $(LDL_COMPOSE) down

# Mermaid sources: docs/diagrams/*.mmd (see docs/diagrams/THEME.md). README.md and the pages under docs/ embed
# copies between <!-- diagram:NAME --> markers; scripts/diagrams.py keeps them in sync.
MMDC ?= npx -y @mermaid-js/mermaid-cli@11
diagrams:
	python3 scripts/diagrams.py
	mkdir -p docs/diagrams/svg
	for f in docs/diagrams/*.mmd; do n=$$(basename $$f .mmd); \
	  $(MMDC) -q -i $$f -o docs/diagrams/svg/$$n.svg -b transparent || exit 1; done

docs: ## Refresh the Mermaid copies and the shared snippets in the Markdown pages
	python3 scripts/diagrams.py
	python3 scripts/doc_snippets.py

docs-check: ## Relative links and anchors, markdownlint, Mermaid and snippet sync, no prose written twice
	python3 scripts/check_links.py
	npx -y markdownlint-cli2@0.23.3
	python3 scripts/diagrams.py --check
	python3 scripts/doc_snippets.py --check
	python3 scripts/check_numbers.py
	python3 scripts/bench_charts.py --check

diagrams-check:
	python3 scripts/diagrams.py --check
	tmp=$$(mktemp -d); for f in docs/diagrams/*.mmd; do \
	  $(MMDC) -q -i $$f -o $$tmp/$$(basename $$f .mmd).svg || exit 1; done; \
	python3 scripts/bench_charts.py --extract $$tmp/charts && for f in $$tmp/charts/*.mmd; do \
	  $(MMDC) -q -i $$f -o $$f.svg || exit 1; done; rm -rf $$tmp; echo "diagrams and benchmark charts OK"

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage reports/*.json reports/*.xml reports/*.md reports/rendered
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

dist: clean
	tar --exclude=.venv --exclude=.git --exclude=.env --exclude='*/__pycache__' \
	    -C .. -czf ../SODA.tar.gz $(notdir $(SODA_HOME))

# ---- benchmarks (need the live stack; they OVERWRITE the retail tables, see benchmarks/README.md) ----
SIZES ?= 10000 100000 1000000 5000000
WARM ?= 5
TOOL_SIZES ?= 100000 1000000
REPEATS ?= 3
BENCH_OUT ?= docs/results/benchmarks
export DQ_BENCH_RESULTS := $(abspath $(BENCH_OUT))
TOOLS_VENV := benchmarks/tools/.venv
bench: ## Gate latency/CPU/RSS/queries per layer x engine at SIZES
	$(RUN) python -m benchmarks.specs
	$(RUN) python -m benchmarks.run_perf --sizes $(SIZES) --warm $(WARM)

bench-accuracy: ## Defect detection at 0.1/1/10% against ground truth
	$(RUN) python -m benchmarks.run_accuracy

bench-spark: ## Spark job time vs gate time on the lakehouse sample
	$(RUN) python -m benchmarks.spark_reference

bench-tools: tools-venv ## Soda vs datacontract-cli vs GX vs Pointblank vs Pandera (own venv)
	$(TOOLS_VENV)/bin/python benchmarks/tools/run_tools.py bench --sizes $(TOOL_SIZES) --repeats $(REPEATS) \
	  --out $(DQ_BENCH_RESULTS)/tools.csv

bench-tiny: tools-venv ## The 7-row comparison behind docs/research/benchmark-tiny (no Docker)
	$(TOOLS_VENV)/bin/python benchmarks/tools/tiny/run_all.py

tools-venv:
	test -x $(TOOLS_VENV)/bin/python || $(UV) venv -q --python 3.12 $(TOOLS_VENV)
	$(UV) pip install -q -p $(TOOLS_VENV) -r benchmarks/tools/requirements.txt

bench-report: ## Write docs/results/benchmarks/summary.md from the raw results
	$(RUN) python -m benchmarks.analyze

bench-airflow: ## Good and bad runs of the gated DAG, per-task durations (needs make airflow-dq-up)
	$(RUN) python -m benchmarks.airflow_runs

bench-charts: ## Mermaid charts in docs/benchmarks/results.md from BENCH_CHARTS_DIR
	$(RUN) python scripts/bench_charts.py

# Trino heap for 5M orders without editing the lakehouse: a compose override kept in this repo.
TRINO_OVERRIDE := -f docker-compose.yml -f $(SODA_HOME)/benchmarks/trino/docker-compose.trino-heap.yml --profile full --profile trino
trino-heap: ## Recreate the lakehouse Trino with a 4 GiB heap (benchmarks/trino/)
	cd "$(DQ_LDL_REPO_PATH)" && LQG_TRINO_DIR=$(SODA_HOME)/benchmarks/trino docker compose $(TRINO_OVERRIDE) up -d --wait trino

trino-heap-reset: ## Back to the lakehouse default 1 GiB heap
	cd "$(DQ_LDL_REPO_PATH)" && docker compose -f docker-compose.yml --profile full --profile trino up -d --wait --force-recreate trino

bench-all: bench bench-accuracy bench-spark bench-tools bench-airflow bench-report bench-charts ## Everything above
