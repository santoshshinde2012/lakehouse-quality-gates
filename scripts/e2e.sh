#!/usr/bin/env bash
# End-to-end runs against a real local-data-lakehouse, with a pass/fail summary.
#
#   scripts/e2e.sh live   T2: lakehouse light profile (Lakekeeper + RustFS), host DuckDB gates, good + bad batch
#   scripts/e2e.sh full   T3: full profile with Spark + Trino, Trino gates, the ldl-soda container, Airflow good + bad
#   scripts/e2e.sh wap    T4: write-audit-publish: CLI good + bad batch, Airflow WAP DAG good + bad, pytest WAP tests
#
# Needs DQ_LDL_REPO_PATH (environment or .env) pointing at a local-data-lakehouse checkout. Idempotent: it
# reuses running containers, stops containers it started, and removes only containers it created. Existing
# stopped containers and all volumes are kept. KEEP_UP=1 leaves everything running. Exits 1 if any step fails.
set -uo pipefail

MODE="${1:-}"
case "$MODE" in live | full | wap) ;; *) echo "usage: $0 live|full|wap" >&2; exit 2 ;; esac

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
LOG_ROOT="$ROOT/reports/e2e-$MODE"
mkdir -p "$LOG_ROOT"
LOG_DIR="$(mktemp -d "$LOG_ROOT/run-XXXXXXXX")" || exit 2
RESULTS=()
FAILED=0

# Discover project names without resolving secrets or requiring a populated lakehouse .env.
compose_project() { docker compose -f "$1" config --no-interpolate | sed -n 's/^name: //p'; }
LAKEHOUSE_PROJECT="$(cd "$LDL" && compose_project docker-compose.yml)" || exit 2
RUNNER_PROJECT="$(compose_project docker/docker-compose.dq.yml)" || exit 2
[[ -n "$LAKEHOUSE_PROJECT" && -n "$RUNNER_PROJECT" ]] || { echo "Cannot identify Compose projects" >&2; exit 2; }
# Names survive Compose recreation; container IDs do not. Snapshot before any startup, including partial stacks.
EXISTING_CONTAINERS="$(docker ps -a --format '{{.Names}}')" || exit 2
RUNNING_CONTAINERS="$(docker ps --format '{{.Names}}')" || exit 2

listed() {
  local names="$1" wanted="$2" name
  while IFS= read -r name; do [[ "$name" == "$wanted" ]] && return 0; done <<< "$names"
  return 1
}

running() { [[ -n "$(docker ps -q --filter "name=^$1$" --filter status=running)" ]]; }

# step NAME EXPECTED_EXIT COMMAND...: log in this run's directory, record PASS or FAIL.
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
    return 0
  else
    RESULTS+=("FAIL|$name|exit $code (expected $expected), see ${log#"$ROOT"/}|${secs}s")
    FAILED=1
    tail -20 "$log" | sed 's/^/    /'
    return 1
  fi
}

# Startup failure must not fall through into seed jobs or fault injection on a partial stack.
required_step() { step "$@" || exit 1; }

cleanup() {
  local code=$? project containers active name
  [[ $code == 0 ]] || FAILED=1
  if [[ "${KEEP_UP:-0}" == 1 ]]; then
    echo "==> KEEP_UP=1: leaving the stack running"
  else
    for project in "$RUNNER_PROJECT" "$LAKEHOUSE_PROJECT"; do
      containers="$(docker ps -a --filter "label=com.docker.compose.project=$project" --format '{{.Names}}')" || { FAILED=1; continue; }
      active="$(docker ps --filter "label=com.docker.compose.project=$project" --format '{{.Names}}')" || { FAILED=1; continue; }
      while IFS= read -r name; do
        [[ -n "$name" ]] || continue
        listed "$RUNNING_CONTAINERS" "$name" && continue
        if listed "$active" "$name"; then
          echo "==> stopping $name"
          docker stop "$name" >/dev/null 2>&1 || { FAILED=1; continue; }
        fi
        if ! listed "$EXISTING_CONTAINERS" "$name"; then
          echo "==> removing $name (volumes kept)"
          docker rm "$name" >/dev/null 2>&1 || FAILED=1
        fi
      done <<< "$containers"
    done
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
  [[ $code != 0 || $FAILED == 0 ]] || code=1
  trap - EXIT
  exit "$code"
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

pytest_complete() {
  local report="$LOG_DIR/pytest-$MODE.xml" code
  uv run --frozen pytest "$@" --junitxml="$report" --basetemp="$LOG_DIR/pytest-tmp"
  code=$?
  [[ $code == 0 ]] || return "$code"
  # Deselected tests are absent from JUnit. Selected skips are incomplete e2e coverage.
  uv run --frozen python - "$report" <<'PY'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

path = Path(sys.argv[1])
try:
    cases = ET.parse(path).getroot().findall(".//testcase")
except (OSError, ET.ParseError) as exc:
    print(f"Cannot validate e2e JUnit report {path}: {exc}", file=sys.stderr)
    sys.exit(1)
skipped = sum(case.find("skipped") is not None for case in cases)
failed = sum(case.find("failure") is not None or case.find("error") is not None for case in cases)
print(f"E2E JUnit: {len(cases)} selected tests, {skipped} skipped, {failed} failed ({path})")
if not cases or skipped or failed:
    print("E2E tests must execute completely, without selected skips or failures", file=sys.stderr)
    sys.exit(1)
PY
}

airflow_bad_trigger() {
  local kind="$1" output="$LOG_DIR/airflow-$1-bad-trigger.txt" code
  make "airflow-$kind-trigger-bad" >"$output" 2>&1
  code=$?
  cat "$output" || return 1
  [[ $code == 2 ]] || { echo "Airflow $kind bad trigger exited $code, expected 2" >&2; return 1; }
  uv run --frozen python scripts/airflow_bad_evidence.py "$kind" "$output" || return 1
  return 2
}

airflow_wap_bad() {
  local before="$LOG_DIR/airflow-main-before.json" after="$LOG_DIR/airflow-main-after.json" code
  uv run --frozen python scripts/wap_main_evidence.py capture "$before" || return 1
  airflow_bad_trigger wap
  code=$?
  uv run --frozen python scripts/wap_main_evidence.py capture "$after" || return 1
  [[ $code == 2 ]] || return 1
  uv run --frozen python scripts/wap_main_evidence.py compare "$before" "$after" || return 1
  return 2
}

required_step "uv sync" 0 uv sync --frozen --all-extras
if [[ ! -x "$LDL/.venv/bin/python" ]]; then
  required_step "lakehouse venv (make venv)" 0 make -C "$LDL" venv
fi

if [[ "$MODE" == live ]]; then
  if running ldl-lakekeeper; then echo "==> lakehouse already running: reusing it"; fi
  required_step "lakehouse up (light profile)" 0 make -C "$LDL" up-light
  step "seed good batch (DuckDB)" 0 seed fixtures/good_batch
  step "gates on good batch, duckdb (expect pass)" 0 "${DQ[@]}" run -e duckdb
  step "seed bad batch" 0 seed fixtures/bad_batch
  step "gates on bad batch, duckdb (expect 3 blocks)" 0 blocks_every_layer duckdb
  step "restore good batch" 0 seed fixtures/good_batch
  step "gates after restore (expect pass)" 0 "${DQ[@]}" run -e duckdb
  step "pytest -m live" 0 pytest_complete -m live -p no:cacheprovider
elif [[ "$MODE" == wap ]]; then
  if running ldl-spark; then echo "==> full lakehouse already running: reusing it"; fi
  required_step "lakehouse up (full profile, WAP overlay)" 0 make wap-up
  step "WAP good batch (expect 3 layers published)" 0 make wap-demo-good WAP_EVIDENCE="$LOG_DIR"
  step "WAP bad batch (expect bronze blocked, main untouched)" 0 make wap-demo-bad WAP_EVIDENCE="$LOG_DIR"
  step "gates on main after the bad batch (expect pass)" 0 "${DQ[@]}" run -e duckdb
  required_step "runner container up (make dq-up)" 0 make dq-up
  required_step "Airflow up on :$AIRFLOW_API_PORT" 0 make airflow-dq-up
  step "Airflow WAP good run (expect success)" 0 make airflow-wap-trigger
  step "Airflow WAP bad run (expect dq_bronze to fail)" 2 airflow_wap_bad
  step "gates on main after the Airflow bad run (expect pass)" 0 "${DQ[@]}" run -e duckdb
  step "pytest WAP tests (-m full -k wap)" 0 pytest_complete -m full -k wap -p no:cacheprovider
else
  if running ldl-trino; then echo "==> full lakehouse already running: reusing it"; fi
  required_step "lakehouse up (full profile, Trino, WAP overlay)" 0 make wap-up
  required_step "lakehouse Spark pipeline (make e2e)" 0 make -C "$LDL" e2e
  step "gates on Spark output, duckdb (expect pass)" 0 "${DQ[@]}" run -e duckdb
  step "gates on Spark output, trino (expect pass)" 0 "${DQ[@]}" run -e trino
  step "seed bad batch" 0 seed fixtures/bad_batch
  step "gates on bad batch, trino (expect 3 blocks)" 0 blocks_every_layer trino
  step "restore good batch" 0 seed fixtures/good_batch
  required_step "runner container up (make dq-up)" 0 make dq-up
  step "gates in ldl-soda container (expect pass)" 0 docker exec ldl-soda soda-dq run
  required_step "Airflow up on :$AIRFLOW_API_PORT" 0 make airflow-dq-up
  step "Airflow good run (expect success)" 0 make airflow-dq-trigger
  step "Airflow bad run (expect dq_bronze to fail)" 2 airflow_bad_trigger dq
  step "Airflow good run again (restores data)" 0 make airflow-dq-trigger
  step "pytest -m 'live or full'" 0 pytest_complete -m "live or full" -p no:cacheprovider
fi
exit $FAILED
