"""Run Make and e2e recipes against fake tools, without touching Docker or live tables."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import PROJECT_ROOT

_FAKE_TOOL = r"""
import json
import os
import sys
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
        code = 2
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

path.write_text(json.dumps(state))
sys.exit(code)
"""


@pytest.fixture
def shell_repo(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    root = tmp_path / "quality-gates"
    (root / "scripts").mkdir(parents=True)
    (root / "docker").mkdir()
    for relative in ("Makefile", "scripts/e2e.sh", "docker/docker-compose.dq.yml"):
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
