"""Import the DAG against minimal stand-ins for the Airflow SDK (Airflow itself is not a dependency)."""

from __future__ import annotations

import sys
import types

import pytest

from tests.conftest import PROJECT_ROOT


class FakeOperator:
    current_dag = None

    def __init__(self, task_id, bash_command, **kwargs):
        self.task_id = task_id
        self.bash_command = bash_command
        self.kwargs = kwargs
        self.downstream: list[FakeOperator] = []
        if FakeOperator.current_dag is not None:
            FakeOperator.current_dag.tasks.append(self)

    def __rshift__(self, other):
        self.downstream.append(other)
        return other


class FakeDAG:
    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks: list[FakeOperator] = []

    def __enter__(self):
        FakeOperator.current_dag = self
        return self

    def __exit__(self, *exc):
        FakeOperator.current_dag = None


@pytest.fixture
def dag_module(monkeypatch):
    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG = FakeDAG
    bash = types.ModuleType("airflow.providers.standard.operators.bash")
    bash.BashOperator = FakeOperator
    lakehouse_ops = types.ModuleType("lakehouse_operators")
    lakehouse_ops.spark_submit_task = lambda task_id, job: FakeOperator(task_id, f"spark-submit {job}")
    for name, module in {
        "airflow": types.ModuleType("airflow"),
        "airflow.sdk": sdk,
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": types.ModuleType("airflow.providers.standard.operators"),
        "airflow.providers.standard.operators.bash": bash,
        "lakehouse_operators": lakehouse_ops,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.syspath_prepend(str(PROJECT_ROOT / "airflow" / "dags"))
    for name in ("dq_operators", "lakehouse_retail_medallion_dq"):
        sys.modules.pop(name, None)
    import lakehouse_retail_medallion_dq

    return lakehouse_retail_medallion_dq


def test_dag_chain_has_a_gate_after_every_layer(dag_module):
    dag = dag_module.dag
    assert dag.dag_id == "lakehouse_retail_medallion_dq"
    order, task = [], dag.tasks[0]
    while task:
        order.append(task.task_id)
        task = task.downstream[0] if task.downstream else None
    assert order == [
        "land_smoke",
        "bronze",
        "inject_bad_batch",
        "dq_bronze",
        "silver",
        "dq_silver",
        "gold",
        "dq_gold",
        "query_timetravel",
    ]


def test_gate_command_and_validation(dag_module):
    import dq_operators

    assert dq_operators.soda_gate_command("silver") == (
        "docker exec ldl-soda soda-dq verify silver --pipeline retail"
    )
    gate = dq_operators.soda_gate_task("gold")
    assert gate.kwargs["retries"] == 0
    with pytest.raises(ValueError, match="unknown layer"):
        dq_operators.soda_gate_command("platinum")
    with pytest.raises(ValueError, match="unsafe"):
        dq_operators.soda_gate_command("gold", "retail; rm -rf /")


def test_fault_injection_is_off_unless_conf_flag(dag_module):
    import dq_operators

    cmd = dq_operators.fault_injection_command()
    assert "dag_run.conf.get('inject_bad_batch')" in cmd
    assert "docker exec ldl-soda soda-dq seed /opt/soda-dq/fixtures/bad_batch --yes" in cmd
    assert cmd.endswith('else echo "no fault injected"; fi')


@pytest.fixture
def wap_dag(dag_module, monkeypatch):
    for name in ("wap_operators", "lakehouse_retail_medallion_wap"):
        sys.modules.pop(name, None)
    import lakehouse_retail_medallion_wap

    return lakehouse_retail_medallion_wap


def test_wap_dag_writes_audits_then_publishes(wap_dag):
    dag = wap_dag.dag
    assert dag.dag_id == "lakehouse_retail_medallion_wap"
    assert dag.kwargs["max_active_runs"] == 1
    order, task = [], dag.tasks[0]
    while task:
        order.append(task.task_id)
        task = task.downstream[0] if task.downstream else None
    assert order == [
        "land_smoke",
        *[
            f"{step}_{layer}"
            for layer in ("bronze", "silver", "gold")
            for step in ("wap_write", "dq", "publish")
        ],
        "query_timetravel",
    ]


def test_wap_commands(wap_dag):
    import wap_operators as ops

    write = ops.spark_wap_command("write", "bronze")
    assert write.startswith("docker exec ldl-spark /opt/spark/bin/spark-submit --master 'local[*]'")
    assert f"write --layer bronze --branch {ops.BRANCH}" in write
    assert "strftime('%Y%m%dt%H%M%S')" in ops.BRANCH
    assert "dag_run.conf.get('bad_batch')" in write
    assert "--source" not in ops.spark_wap_command("write", "silver")
    assert ops.spark_wap_command("publish", "gold").endswith(f"publish --layer gold --branch {ops.BRANCH}")
    assert ops.wap_gate_command("silver") == (
        f"docker exec ldl-soda soda-dq wap verify silver --pipeline retail --branch {ops.BRANCH}"
    )
    assert ops.wap_gate_task("gold").kwargs["retries"] == 0
    for bad in (lambda: ops.spark_wap_command("rm", "gold"), lambda: ops.spark_wap_command("write", "x"),
                lambda: ops.wap_gate_command("gold", "a;b")):  # fmt: skip
        with pytest.raises(ValueError):  # noqa: PT011
            bad()
