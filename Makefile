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

.PHONY: help env venv lock fmt lint typecheck test test-unit test-integration check validate list \
	demo demo-good demo-bad verify run pre-commit dq-build dq-up dq-down dq-shell \
	airflow-dq-up airflow-dq-trigger airflow-dq-trigger-bad airflow-dq-down clean dist

help:
	@echo "lakehouse-quality-gates (Soda Core 4.26 data contracts as medallion quality gates)"
	@echo "  make venv               uv sync (all extras + dev group) into .venv"
	@echo "  make check              ruff + mypy --strict + pytest (what CI runs)"
	@echo "  make validate           parse every contract with Soda (no data access)"
	@echo "  make demo               good batch passes, bad batch is blocked (DuckDB on CSVs, no Docker)"
	@echo "  make verify LAYER=silver [ENGINE=duckdb|duckdb-local|trino]   one gate"
	@echo "  make run [ENGINE=...]   bronze -> silver -> gold, stop at the first blocked layer"
	@echo "  make dq-up | dq-down    runner container ldl-soda on ldl-net (lakehouse must be up)"
	@echo "  make airflow-dq-up      lakehouse Airflow + Soda-gated DAG (needs DQ_LDL_REPO_PATH)"
	@echo "  make airflow-dq-trigger trigger lakehouse_retail_medallion_dq and wait"
	@echo "  make airflow-dq-trigger-bad  same, with the bad batch injected (dq_bronze must fail)"
	@echo "  make airflow-dq-down    stop Airflow + lakehouse (volumes kept)"
	@echo "  AIRFLOW_API_PORT=8080   Airflow host port (override if taken, e.g. 8090)"
	@echo "  make dist               SODA.tar.gz without .venv / caches / reports"

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
	$(RUN) pytest -m "not integration"

test-integration:
	$(RUN) pytest -m integration

test:
	$(RUN) pytest --cov=soda_lakehouse_dq --cov-report=term-missing

check: lint typecheck test

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

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage reports/*.json reports/*.xml reports/*.md reports/rendered
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

dist: clean
	tar --exclude=.venv --exclude=.git --exclude=.env --exclude='*/__pycache__' \
	    -C .. -czf ../SODA.tar.gz $(notdir $(SODA_HOME))
