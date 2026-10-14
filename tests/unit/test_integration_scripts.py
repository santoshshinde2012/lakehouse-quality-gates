"""Run Make and e2e recipes against fake tools, without touching Docker or live tables."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from tests.conftest import PROJECT_ROOT

_FAKE_TOOL = r"""
import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

path = Path(os.environ["DQ_TEST_STATE"])
state = json.loads(path.read_text())
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
state["calls"].append([tool, *args])
containers = state["containers"]
code = 0

def start(names, project):
    for name in names:
        containers[name] = {"project": project, "running": not name.endswith("init"), "id": "recreated"}

base = ["ldl-postgres", "ldl-lakekeeper", "ldl-objectstore", "ldl-lakehouse-init"]
airflow = ["ldl-airflow-postgres", "ldl-docker-proxy", "ldl-airflow-scheduler",
           "ldl-airflow-apiserver", "ldl-airflow-dag-processor", "ldl-airflow-init"]

if tool == "docker":
    failure = state.get("docker_failure")
    if args[0] == failure and (failure != "ps" or any(arg.startswith("label=") for arg in args)):
        path.write_text(json.dumps(state))
        sys.exit(2)
    if args[0] == "compose":
        if "config" in args:
            print("name: " + ("runner" if "docker/docker-compose.dq.yml" in args else "lakehouse"))
        elif "up" in args:
            compose_dir = Path.cwd() / "docker"
            source = compose_dir / os.environ["DQ_LDL_REPO_PATH"] / "src" / "lakehouse_client"
            state["mount_source"] = str(source.resolve())
    elif args[0] == "ps":
        filters = [args[i + 1] for i, arg in enumerate(args) if arg == "--filter"]
        for name, container in containers.items():
            if "-a" not in args and not container["running"]:
                continue
            if any(f.startswith("label=") and f.split("=", 2)[2] != container["project"] for f in filters):
                continue
            if any(f.startswith("name=") and f != "name=^" + name + "$" for f in filters):
                continue
            print(container["id"] if "-q" in args else name)
    elif args[0] == "stop":
        containers[args[1]]["running"] = False
    elif args[0] == "rm":
        del containers[args[1]]
elif tool == "make":
    if "up-light" in args:
        start(base, "lakehouse")
        code = state.get("startup_exit", 0)
    elif "wap-up" in args:
        start([*base, "ldl-spark", "ldl-trino"], "lakehouse")
    elif "dq-up" in args:
        start(["ldl-soda"], "runner")
    elif "airflow-dq-up" in args:
        start([*base, "ldl-spark", *airflow], "lakehouse")
        start(["ldl-soda"], "runner")
    elif any(arg in {"airflow-dq-trigger-bad", "airflow-wap-trigger-bad"} for arg in args):
        code = state.get("airflow_bad_exit", 2)
        wap = "airflow-wap-trigger-bad" in args
        if wap:
            state["airflow_wap_bad_seen"] = True
            blocked = ("publish_bronze", "wap_write_silver", "dq_silver", "publish_silver",
                       "wap_write_gold", "dq_gold", "publish_gold", "query_timetravel")
            expected = {"land_smoke": "success", "wap_write_bronze": "success", "dq_bronze": "failed",
                        **{name: "upstream_failed" for name in blocked}}
        else:
            expected = {"land_smoke": "success", "bronze": "success", "inject_bad_batch": "success",
                        "dq_bronze": "failed", **{name: "upstream_failed" for name in
                         ("silver", "dq_silver", "gold", "dq_gold", "query_timetravel")}}
        dag = "lakehouse_retail_medallion_" + ("wap" if wap else "dq")
        print("dag_id logical_date task_id state start_date end_date")
        for task, default in expected.items():
            task_state = state.get("airflow_task_states", {}).get(task, default)
            print(f"{dag}    {task}    {task_state}    "
                  "2026-10-14T14:00:00+00:00    2026-10-14T14:00:01+00:00")
    elif any(arg in {"airflow-dq-down", "down", "dq-down"} for arg in args):
        # Model project-wide teardown so regressions to the old cleanup fail the tests.
        project = "runner" if "dq-down" in args else "lakehouse"
        for name in list(containers):
            if containers[name]["project"] == project:
                del containers[name]
elif tool == "uv":
    if "seed" in args:
        state["bad_batch"] = "fixtures/bad_batch" in args
    elif "verify" in args:
        code = state.get("verify_exit", 1 if state.get("bad_batch") else 0)
    elif "sync" in args:
        code = state.get("sync_exit", 0)
    elif "status" in args:
        layer = args[-1]
        stage = "after" if state.get("airflow_wap_bad_seen") else "before"
        if state.get("status_failure") == stage:
            code = 2
        elif state.get("malformed_status") == stage:
            print("not-json")
        else:
            names = {"bronze": ["orders_raw", "customers_raw"], "silver": ["orders", "customers"],
                     "gold": ["daily_order_metrics"]}[layer]
            tables = {f"{layer}.{name}": {"main_snapshot": 101 + index, "main_rows": 10 + index,
                      "refs": {"main": 101 + index}} for index, name in enumerate(names)}
            if stage == "after":
                for table in tables.values():
                    table["refs"]["new_audit_ref"] = 999
                changed = state.get("changed_main_field")
                if changed and layer == "bronze":
                    tables["bronze.orders_raw"][changed] += 1
            print(json.dumps({"action": "status", "seconds": 1.0, "tables": tables}))
    elif "pytest" in args:
        destination = next((a.split("=", 1)[1] for a in args if a.startswith("--junitxml=")), None)
        if destination and state.get("junit") != "missing":
            report = Path(destination)
            report.parent.mkdir(parents=True, exist_ok=True)
            if state.get("junit") == "malformed":
                report.write_text("<not-valid-xml")
            else:
                suite = ET.Element("testsuite")
                for index in range(state.get("pytest_selected", 3)):
                    case = ET.SubElement(suite, "testcase", name=f"test_{index}")
                    if index < state.get("pytest_skipped", 0):
                        ET.SubElement(case, "skipped")
                    if state.get("junit") == "failed":
                        ET.SubElement(case, "failure")
                ET.ElementTree(suite).write(report, encoding="unicode")
        code = state.get("pytest_exit", 0)
    elif "python" in args:
        path.write_text(json.dumps(state))
        os.execv(sys.executable, [sys.executable, *args[args.index("python") + 1:]])

if tool == "make" and state.get("failed_make") in args:
    code = 2

path.write_text(json.dumps(state))
sys.exit(code)
"""


@pytest.fixture
def shell_repo(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    root = tmp_path / "quality-gates"
    (root / "scripts").mkdir(parents=True)
    (root / "docker").mkdir()
    for relative in (
        "Makefile",
        "scripts/e2e.sh",
        "scripts/airflow_bad_evidence.py",
        "scripts/wap_main_evidence.py",
        "docker/docker-compose.dq.yml",
    ):
        shutil.copy2(PROJECT_ROOT / relative, root / relative)
    (root / ".env").write_text("DQ_LDL_REPO_PATH=../local-data-lakehouse\n")
    lakehouse = tmp_path / "local-data-lakehouse"
    (lakehouse / ".venv" / "bin").mkdir(parents=True)
    (lakehouse / "docker-compose.yml").write_text("services: {}\n")
    python = lakehouse / ".venv" / "bin" / "python"
    python.touch()
    python.chmod(0o755)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("docker", "uv", "make"):
        executable = bin_dir / tool
        executable.write_text(f"#!{sys.executable}\n" + _FAKE_TOOL)
        executable.chmod(0o755)
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"calls": [], "containers": {}}))
    env = {k: v for k, v in os.environ.items() if not k.startswith(("DQ_", "KEEP_UP", "MAKE"))}
    env.update(
        PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}", DQ_TEST_STATE=str(state), UV=str(bin_dir / "uv")
    )
    return root, state, env


def _run_make(shell_repo: tuple[Path, Path, dict[str, str]], *args: str) -> subprocess.CompletedProcess[str]:
    root, _, env = shell_repo
    make = shutil.which("make")
    assert make is not None
    return subprocess.run([make, *args], cwd=root, env=env, capture_output=True, text=True, check=False)


@pytest.mark.parametrize("exit_code", [0, 1, 2, 127])
def test_demo_bad_requires_blocked_exit_code(shell_repo, exit_code):
    _, state_file, _ = shell_repo
    state = json.loads(state_file.read_text())
    state["verify_exit"] = exit_code
    state_file.write_text(json.dumps(state))
    result = _run_make(shell_repo, "demo-bad")
    assert (result.returncode == 0) == (exit_code == 1), result.stdout + result.stderr
    calls = json.loads(state_file.read_text())["calls"]
    assert len(calls) == (3 if exit_code == 1 else 1)


@pytest.mark.parametrize("command_line_path", [False, True])
def test_make_resolves_checkout_path_before_compose(shell_repo, command_line_path):
    root, state_file, _ = shell_repo
    args = ["dq-up"]
    if command_line_path:
        args.append("DQ_LDL_REPO_PATH=../local-data-lakehouse")
    result = _run_make(shell_repo, *args)
    assert result.returncode == 0, result.stdout + result.stderr
    state = json.loads(state_file.read_text())
    assert state["mount_source"] == str(root.parent / "local-data-lakehouse" / "src" / "lakehouse_client")


@pytest.mark.parametrize("mode", ["live", "full", "wap"])
def test_e2e_preserves_running_and_previously_stopped_containers(shell_repo, mode):
    root, state_file, env = shell_repo
    existing = {
        "ldl-lakekeeper": {"project": "lakehouse", "running": True, "id": "original"},
        "ldl-postgres": {"project": "lakehouse", "running": False, "id": "original"},
        "ldl-soda": {"project": "runner", "running": True, "id": "original"},
        "unrelated": {"project": "elsewhere", "running": True, "id": "original"},
    }
    state_file.write_text(json.dumps({"calls": [], "containers": existing}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", mode], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    state = json.loads(state_file.read_text())
    assert set(state["containers"]) == set(existing)
    assert {name: c["running"] for name, c in state["containers"].items()} == {
        name: c["running"] for name, c in existing.items()
    }
    assert state["containers"]["ldl-lakekeeper"]["id"] == "recreated"
    assert all("down" not in call and "airflow-dq-down" not in call for call in state["calls"])
    report = next((root / "reports" / f"e2e-{mode}").glob(f"run-*/pytest-{mode}.xml"))
    assert len(ET.parse(report).getroot().findall(".//testcase")) == 3


def test_e2e_cleans_partial_startup_after_failure(shell_repo):
    root, state_file, env = shell_repo
    existing = {"ldl-lakekeeper": {"project": "lakehouse", "running": True, "id": "original"}}
    state_file.write_text(json.dumps({"calls": [], "containers": existing, "startup_exit": 1}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", "live"], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1, result.stdout + result.stderr
    state = json.loads(state_file.read_text())
    assert set(state["containers"]) == set(existing)
    assert state["containers"]["ldl-lakekeeper"]["running"]
    assert not any("seed" in call for call in state["calls"])


@pytest.mark.parametrize("mode", ["live", "full", "wap"])
def test_e2e_dependency_failure_never_starts_or_seeds(shell_repo, mode):
    root, state_file, env = shell_repo
    state_file.write_text(json.dumps({"calls": [], "containers": {}, "sync_exit": 2}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", mode], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    state = json.loads(state_file.read_text())
    assert not any(call[0] == "make" or "seed" in call for call in state["calls"])
    assert "RESULT: FAIL" in result.stdout


@pytest.mark.parametrize(
    ("mode", "target"),
    [("full", "wap-up"), ("wap", "wap-up"), ("full", "dq-up"), ("wap", "airflow-dq-up")],
)
def test_e2e_failed_startup_only_runs_cleanup_afterward(shell_repo, mode, target):
    root, state_file, env = shell_repo
    state_file.write_text(json.dumps({"calls": [], "containers": {}, "failed_make": target}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", mode], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    state = json.loads(state_file.read_text())
    failure_index = next(index for index, call in enumerate(state["calls"]) if target in call)
    assert all(call[0] == "docker" for call in state["calls"][failure_index + 1 :])
    assert not state["containers"]
    assert "RESULT: FAIL" in result.stdout


@pytest.mark.parametrize("mode", ["live", "full", "wap"])
@pytest.mark.parametrize("skipped", [1, 3])
def test_e2e_fails_on_any_selected_test_skip_and_keeps_junit(shell_repo, mode, skipped):
    root, state_file, env = shell_repo
    state_file.write_text(json.dumps({"calls": [], "containers": {}, "pytest_skipped": skipped}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", mode], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert f"3 selected tests, {skipped} skipped" in result.stdout
    assert "RESULT: FAIL" in result.stdout
    report = next((root / "reports" / f"e2e-{mode}").glob(f"run-*/pytest-{mode}.xml"))
    cases = ET.parse(report).getroot().findall(".//testcase")
    assert len(cases) == 3
    assert sum(case.find("skipped") is not None for case in cases) == skipped


@pytest.mark.parametrize("junit", ["empty", "missing", "malformed", "failed"])
def test_e2e_rejects_incomplete_or_invalid_success_reports(shell_repo, junit):
    root, state_file, env = shell_repo
    state = {"calls": [], "containers": {}, "junit": junit}
    if junit == "empty":
        state["pytest_selected"] = 0
    state_file.write_text(json.dumps(state))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", "live"], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert "RESULT: FAIL" in result.stdout


def test_e2e_airflow_wap_bad_preserves_main_and_attaches_both_observations(shell_repo):
    root, _, env = shell_repo
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", "wap"], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    directory = next((root / "reports" / "e2e-wap").glob("run-*"))
    before = json.loads((directory / "airflow-main-before.json").read_text())
    after = json.loads((directory / "airflow-main-after.json").read_text())
    assert set(before) == set(after) == {"bronze", "silver", "gold"}
    for layer, state in before.items():
        for name, table in state["tables"].items():
            final = after[layer]["tables"][name]
            assert (table["main_snapshot"], table["main_rows"]) == (
                final["main_snapshot"],
                final["main_rows"],
            )
            assert table["refs"] != final["refs"]
    log = (directory / "09-airflow-wap-bad-run-expect-dq-bronze-to-fail.log").read_text()
    assert "preserved all five main snapshot ids and row counts" in log
    assert len(list(directory.glob("*.log"))) == 11


@pytest.mark.parametrize(
    "failure",
    [
        {"changed_main_field": "main_snapshot"},
        {"changed_main_field": "main_rows"},
        {"status_failure": "before"},
        {"status_failure": "after"},
        {"malformed_status": "after"},
        {"airflow_bad_exit": 0},
        {"airflow_bad_exit": 1},
    ],
)
def test_e2e_airflow_wap_capture_and_invariance_errors_cannot_match_expected_exit_two(shell_repo, failure):
    root, state_file, env = shell_repo
    state_file.write_text(json.dumps({"calls": [], "containers": {}, **failure}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", "wap"], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert "RESULT: FAIL" in result.stdout
    summary = next(
        line for line in result.stdout.splitlines() if line.startswith("FAIL") and "Airflow WAP bad" in line
    )
    assert "exit 1 (expected 2)" in summary
    if failure.get("status_failure") == "before":
        assert not json.loads(state_file.read_text()).get("airflow_wap_bad_seen")


@pytest.mark.parametrize("mode", ["full", "wap"])
@pytest.mark.parametrize("failure", ["writer", "downstream"])
def test_e2e_rejects_airflow_writer_failure_or_unblocked_downstream(shell_repo, mode, failure):
    root, state_file, env = shell_repo
    if failure == "writer":
        states = {"wap_write_bronze" if mode == "wap" else "bronze": "failed", "dq_bronze": "upstream_failed"}
        if mode == "full":
            states["inject_bad_batch"] = "upstream_failed"
    else:
        states = {"publish_bronze" if mode == "wap" else "silver": "success"}
    state_file.write_text(json.dumps({"calls": [], "containers": {}, "airflow_task_states": states}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", mode], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert "Airflow bad-run evidence error" in result.stdout
    summary = next(
        line for line in result.stdout.splitlines() if line.startswith("FAIL") and "Airflow" in line
    )
    assert "exit 1 (expected 2)" in summary
    directory = next((root / "reports" / f"e2e-{mode}").glob("run-*"))
    kind = "wap" if mode == "wap" else "dq"
    assert (directory / f"airflow-{kind}-bad-trigger.txt").exists()
    assert len(list(directory.glob("*.log"))) == (11 if mode == "wap" else 15)


def test_e2e_keep_up_skips_cleanup(shell_repo):
    root, state_file, env = shell_repo
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", "full"],
        cwd=root,
        env={**env, "KEEP_UP": "1"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    state = json.loads(state_file.read_text())
    assert state["containers"]["ldl-airflow-apiserver"]["running"]
    assert all(call[:2] not in (["docker", "stop"], ["docker", "rm"]) for call in state["calls"])


@pytest.mark.parametrize("docker_failure", ["ps", "stop", "rm"])
def test_e2e_cleanup_failure_is_reported(shell_repo, docker_failure):
    root, state_file, env = shell_repo
    state_file.write_text(json.dumps({"calls": [], "containers": {}, "docker_failure": docker_failure}))
    result = subprocess.run(
        ["bash", "scripts/e2e.sh", "live"], cwd=root, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "RESULT: FAIL" in result.stdout


def test_e2e_runs_isolate_logs_and_preserve_previous_evidence(shell_repo):
    root, _, env = shell_repo
    for mode, expected_steps in (("live", 9), ("full", 15)):
        log_root = root / "reports" / f"e2e-{mode}"
        log_root.mkdir(parents=True)
        previous = log_root / "01-obsolete-step.log"
        previous.write_text("previous run evidence\n")
        for _ in range(2):
            result = subprocess.run(
                ["bash", "scripts/e2e.sh", mode],
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            summary = next(line for line in result.stdout.splitlines() if " summary (logs in " in line)
            assert f"reports/e2e-{mode}/run-" in summary
        run_dirs = list(log_root.glob("run-*"))
        assert len(run_dirs) == 2
        assert all(len(list(directory.glob("*.log"))) == expected_steps for directory in run_dirs)
        assert previous.read_text() == "previous run evidence\n"
