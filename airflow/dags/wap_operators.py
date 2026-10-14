"""Airflow helpers for the write-audit-publish DAG (ADR 0006).

Three task kinds per layer, all ``docker exec`` through the lakehouse's socket proxy:

* ``wap_write_<layer>``: Spark builds the layer on the run's audit branch (``/opt/soda-wap/wap_job.py write``)
* ``dq_<layer>``: ``soda-dq wap verify <layer> --branch ...`` in ``ldl-soda``; exit 1 (blocked) fails the task
* ``publish_<layer>``: Spark promotes exact audited snapshots; source branches stay for triage

Values that reach the shell are fixed words or validated names; the branch is the run's ``run_after`` time.
"""

from __future__ import annotations

import os
import shlex

from airflow.providers.standard.operators.bash import BashOperator

SPARK_CONTAINER = os.environ.get("LDL_SPARK_CONTAINER", "ldl-spark")
SODA_CONTAINER = os.environ.get("LDL_SODA_CONTAINER", "ldl-soda")
WAP_JOB = "/opt/soda-wap/wap_job.py"
LAYERS = ("bronze", "silver", "gold")
# One audit branch per DAG run, named after the run time: digits and t only reach the shell.
# (Airflow 3 manual runs have no logical date, so ts_nodash is undefined there.)
BRANCH = "audit_{{ dag_run.run_after.strftime('%Y%m%dt%H%M%S%f') }}"
# conf {"bad_batch": true} writes fixtures/bad_batch to the bronze branch instead of the lakehouse sample.
SOURCE = "{{ '/opt/soda-wap-fixtures/bad_batch' if dag_run.conf.get('bad_batch') else '/opt/data/sample' }}"


def audit_path(layer: str, *, spark: bool) -> str:
    directory = "/opt/soda-wap-reports" if spark else "/opt/soda-dq/reports"
    return f"{directory}/wap/{BRANCH}/{_check(layer)}.audit.json"


def _check(layer: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
    return layer


def spark_wap_command(action: str, layer: str, container: str = SPARK_CONTAINER) -> str:
    if action not in {"write", "publish", "drop"}:
        raise ValueError(f"unknown WAP action {action!r}")
    args = ["docker", "exec", container, "/opt/spark/bin/spark-submit", "--master", "local[*]", WAP_JOB]
    args += [action, "--layer", _check(layer)]
    cmd = " ".join(shlex.quote(a) for a in args) + f" --branch {BRANCH}"
    if action == "write" and layer == "bronze":
        cmd += f' --source "{SOURCE}"'
    if action == "publish":
        cmd += f' --audit-file "{audit_path(layer, spark=True)}"'
    return cmd


def wap_gate_command(layer: str, pipeline: str = "retail", container: str = SODA_CONTAINER) -> str:
    if not pipeline.replace("_", "").isalnum():
        raise ValueError(f"unsafe pipeline name {pipeline!r}")
    args = ["docker", "exec", container, "soda-dq", "wap", "verify", _check(layer), "--pipeline", pipeline]
    return (
        " ".join(shlex.quote(a) for a in args)
        + f" --branch {BRANCH}"
        + f' --evidence "{audit_path(layer, spark=False)}"'
    )


def wap_write_task(layer: str) -> BashOperator:
    return BashOperator(
        task_id=f"wap_write_{layer}",
        bash_command=spark_wap_command("write", layer),
        doc_md=f"Spark writes `{layer}` to the run's Iceberg audit branch; main is not touched.",
    )


def wap_gate_task(layer: str, pipeline: str = "retail") -> BashOperator:
    return BashOperator(
        task_id=f"dq_{layer}",
        bash_command=wap_gate_command(layer, pipeline),
        doc_md=f"Soda gate on the audit branch of `{pipeline}/{layer}`: fails when the gate blocks.",
        retries=0,
    )


def wap_publish_task(layer: str) -> BashOperator:
    return BashOperator(
        task_id=f"publish_{layer}",
        bash_command=spark_wap_command("publish", layer),
        doc_md=f"Fast-forward `{layer}` main to the audited snapshots; keep the audit branch for triage.",
        retries=0,
    )
