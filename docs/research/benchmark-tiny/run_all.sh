#!/usr/bin/env bash
# Runs every tool on good+bad silver.orders fixtures; writes results/*.txt and results/summary.csv
set -u; cd "$(dirname "$0")"; PY=.venv/bin/python
echo "tool,wall_seconds_incl_import,exit_code,config_lines" > results/summary.csv
run(){ n=$1; shift; cfg=$1; shift; s=$(date +%s%N); "$@" > results/$n.txt 2>&1; rc=$?; e=$(( ($(date +%s%N)-s)/1000000 ));
  echo "$n,$(printf '%d.%03d' $((e/1000)) $((e%1000))),$rc,$(grep -cv '^\s*\(#.*\)\?$' $cfg)" >> results/summary.csv; }
run soda_v4 configs/soda_orders.yml $PY scripts/run_soda.py
run gx_core scripts/run_gx.py $PY scripts/run_gx.py
run pointblank scripts/run_pointblank.py $PY scripts/run_pointblank.py
run pandera scripts/run_pandera.py $PY scripts/run_pandera.py
run datacontract_good configs/odcs_orders_good.yaml .venv/bin/datacontract test configs/odcs_orders_good.yaml
run datacontract_bad configs/odcs_orders_bad.yaml .venv/bin/datacontract test configs/odcs_orders_bad.yaml
.venv/bin/pip freeze 2>/dev/null > results/versions.txt || uv pip freeze -p $PY > results/versions.txt
cat results/summary.csv
