"""Airflow helpers for Soda quality gates.

A gate is a BashOperator that runs ``soda-dq verify <layer>`` inside the ``ldl-soda`` container through
the same socket proxy the lakehouse DAGs already use for ``ldl-spark``. Exit codes from the CLI:
0 = passed (warnings are logged, not fatal), 1 = blocked (task fails), 2 = configuration error.
"""

from __future__ import annotations

import os
import shlex

from airflow.providers.standard.operators.bash import BashOperator

SODA_CONTAINER = os.environ.get("LDL_SODA_CONTAINER", "ldl-soda")
LAYERS = ("bronze", "silver", "gold")


def soda_gate_command(layer: str, pipeline: str = "retail", container: str = SODA_CONTAINER) -> str:
    """Shell command for one gate; values are validated and quoted (no templated user input)."""
    if layer not in LAYERS:
        raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
    if not pipeline.replace("_", "").isalnum():
        raise ValueError(f"unsafe pipeline name {pipeline!r}")
    args = ["docker", "exec", container, "soda-dq", "verify", layer, "--pipeline", pipeline]
    return " ".join(shlex.quote(a) for a in args)


def soda_gate_task(layer: str, pipeline: str = "retail") -> BashOperator:
    return BashOperator(
        task_id=f"dq_{layer}",
        bash_command=soda_gate_command(layer, pipeline),
        doc_md=f"Soda contract gate for `{pipeline}/{layer}`: fails the task when the gate blocks.",
        retries=0,  # a data problem does not fix itself on retry
    )


def fault_injection_command(container: str = SODA_CONTAINER) -> str:
    """Seed the bad batch only when the run's conf has ``inject_bad_batch: true``.

    The Jinja expression renders to the fixed words ``yes`` or ``no``; no conf value reaches the shell.
    """
    seed = " ".join(
        shlex.quote(a)
        for a in ["docker", "exec", container, "soda-dq", "seed", "/opt/soda-dq/fixtures/bad_batch", "--yes"]
    )
    flag = "{{ 'yes' if dag_run.conf.get('inject_bad_batch') else 'no' }}"
    return f'if [ "{flag}" = yes ]; then {seed}; else echo "no fault injected"; fi'


def fault_injection_task() -> BashOperator:
    return BashOperator(
        task_id="inject_bad_batch",
        bash_command=fault_injection_command(),
        doc_md='Demo only: with conf `{"inject_bad_batch": true}` the bad batch replaces the tables, '
        "so `dq_bronze` must block. Otherwise a no-op.",
        retries=0,
    )
