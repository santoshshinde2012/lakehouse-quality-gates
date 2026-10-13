#!/usr/bin/env bash
# End-to-end runs against a real local-data-lakehouse, with a pass/fail summary.
#
#   scripts/e2e.sh live   T2: lakehouse light profile (Lakekeeper + RustFS), host DuckDB gates, good + bad batch
#   scripts/e2e.sh full   T3: full profile with Spark + Trino, Trino gates, the ldl-soda container, Airflow good + bad
#
# Needs DQ_LDL_REPO_PATH (environment or .env) pointing at a local-data-lakehouse checkout. Idempotent: it
# reuses a stack that is already running and only stops what it started, always with `down` and never `-v`,
# so no volume is deleted. KEEP_UP=1 leaves everything running. Exits 1 if any step fails.
set -uo pipefail

MODE="${1:-}"
case "$MODE" in live | full) ;; *) echo "usage: $0 live|full" >&2; exit 2 ;; esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ -z "${DQ_LDL_REPO_PATH:-}" && -f .env ]]; then
  DQ_LDL_REPO_PATH="$(sed -n 's/^DQ_LDL_REPO_PATH=//p' .env | tail -1)"
fi
if [[ -z "${DQ_LDL_REPO_PATH:-}" ]]; then
  echo "DQ_LDL_REPO_PATH is not set: point it at your local-data-lakehouse checkout (export it or put it in .env)" >&2
  exit 2
fi
LDL="$(cd "$ROOT" && cd "$DQ_LDL_REPO_PATH" 2>/dev/null && pwd)" || { echo "DQ_LDL_REPO_PATH not found: $DQ_LDL_REPO_PATH" >&2; exit 2; }
[[ -f "$LDL/docker-compose.yml" ]] || { echo "$LDL does not look like local-data-lakehouse (no docker-compose.yml)" >&2; exit 2; }
export DQ_LDL_REPO_PATH="$LDL" SODA_CORE_TELEMETRY_ENABLED=false
export AIRFLOW_API_PORT="${AIRFLOW_API_PORT:-8080}"
for tool in uv docker; do command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 2; }; done
docker info >/dev/null 2>&1 || { echo "Docker is not running" >&2; exit 2; }

DQ=(uv run --frozen soda-dq)
LOG_DIR="$ROOT/reports/e2e-$MODE"
mkdir -p "$LOG_DIR"
RESULTS=()
FAILED=0
STARTED_LAKEHOUSE=0
STARTED_RUNNER=0
STARTED_AIRFLOW=0

running() { [[ -n "$(docker ps -q --filter "name=^$1$" --filter status=running)" ]]; }

# step NAME EXPECTED_EXIT COMMAND...: run, log to reports/e2e-<mode>/, record PASS or FAIL.
step() {
  local name="$1" expected="$2"
  shift 2
  local log slug
  slug="$(printf '%s' "$name" | tr 'A-Z' 'a-z' | tr -cs 'a-z0-9' '-' | sed 's/-*$//')"
  log="$LOG_DIR/$(printf '%02d' "$((${#RESULTS[@]} + 1))")-$slug.log"
  local start=$SECONDS
  echo "==> $name"
  "$@" >"$log" 2>&1
  local code=$?
  local secs=$((SECONDS - start))
  if [[ "$code" == "$expected" ]]; then
    RESULTS+=("PASS|$name|exit $code (expected $expected)|${secs}s")
  else
    RESULTS+=("FAIL|$name|exit $code (expected $expected), see ${log#"$ROOT"/}|${secs}s")
    FAILED=1
    tail -20 "$log" | sed 's/^/    /'
  fi
}

cleanup() {
  if [[ "${KEEP_UP:-0}" == 1 ]]; then
    echo "==> KEEP_UP=1: leaving the stack running"
  else
    [[ $STARTED_AIRFLOW == 1 ]] && { echo "==> stopping Airflow"; make -s airflow-dq-down >/dev/null 2>&1 || true; }
    [[ $STARTED_RUNNER == 1 ]] && { echo "==> stopping ldl-soda"; make -s dq-down >/dev/null 2>&1 || true; }
    [[ $STARTED_LAKEHOUSE == 1 ]] && { echo "==> stopping the lakehouse (volumes kept)"; (cd "$LDL" && make -s down >/dev/null 2>&1) || true; }
  fi
  echo
  echo "e2e-$MODE summary (logs in ${LOG_DIR#"$ROOT"/}/)"
  printf '%-5s | %-46s | %s\n' "" "step" "result"
  local r
  for r in "${RESULTS[@]}"; do
    IFS='|' read -r status name detail secs <<<"$r"
    printf '%-5s | %-46s | %s, %s\n' "$status" "$name" "$detail" "$secs"
  done
  if [[ $FAILED == 0 ]]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

seed() { "${DQ[@]}" seed "$1" --yes; }
blocks_every_layer() { # every layer of the bad batch must exit 1
  local engine="$1" layer code
  for layer in bronze silver gold; do
    "${DQ[@]}" verify "$layer" -e "$engine"
    code=$?
    [[ $code == 1 ]] || { echo "$layer exited $code, expected 1"; return 1; }
  done
}

step "uv sync" 0 uv sync --frozen --all-extras
if [[ ! -x "$LDL/.venv/bin/python" ]]; then
  step "lakehouse venv (make venv)" 0 make -C "$LDL" venv
fi

if [[ "$MODE" == live ]]; then
  if running ldl-lakekeeper; then echo "==> lakehouse already running: reusing it"; else STARTED_LAKEHOUSE=1; fi
  step "lakehouse up (light profile)" 0 make -C "$LDL" up-light
  step "seed good batch (DuckDB)" 0 seed fixtures/good_batch
  step "gates on good batch, duckdb (expect pass)" 0 "${DQ[@]}" run -e duckdb
  step "seed bad batch" 0 seed fixtures/bad_batch
  step "gates on bad batch, duckdb (expect 3 blocks)" 0 blocks_every_layer duckdb
  step "restore good batch" 0 seed fixtures/good_batch
  step "gates after restore (expect pass)" 0 "${DQ[@]}" run -e duckdb
  step "pytest -m live" 0 uv run --frozen pytest -m live -p no:cacheprovider
else
  if running ldl-trino; then echo "==> full lakehouse already running: reusing it"; else STARTED_LAKEHOUSE=1; fi
  running ldl-soda || STARTED_RUNNER=1
  running ldl-airflow-apiserver || running ldl-airflow-api-server || STARTED_AIRFLOW=1
  step "lakehouse up (full profile, Trino)" 0 make -C "$LDL" up-full TRINO=1
  step "lakehouse Spark pipeline (make e2e)" 0 make -C "$LDL" e2e
  step "gates on Spark output, duckdb (expect pass)" 0 "${DQ[@]}" run -e duckdb
  step "gates on Spark output, trino (expect pass)" 0 "${DQ[@]}" run -e trino
  step "seed bad batch" 0 seed fixtures/bad_batch
  step "gates on bad batch, trino (expect 3 blocks)" 0 blocks_every_layer trino
  step "restore good batch" 0 seed fixtures/good_batch
  step "runner container up (make dq-up)" 0 make dq-up
  step "gates in ldl-soda container (expect pass)" 0 docker exec ldl-soda soda-dq run
  step "Airflow up on :$AIRFLOW_API_PORT" 0 make airflow-dq-up
  step "Airflow good run (expect success)" 0 make airflow-dq-trigger
  step "Airflow bad run (expect dq_bronze to fail)" 2 make airflow-dq-trigger-bad
  step "Airflow good run again (restores data)" 0 make airflow-dq-trigger
  step "pytest -m 'live or full'" 0 uv run --frozen pytest -m "live or full" -p no:cacheprovider
fi
exit $FAILED
