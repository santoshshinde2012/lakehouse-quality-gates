"""
Retail medallion with write-audit-publish (ADR 0006):

    land_smoke >> (wap_write_<layer> >> dq_<layer> >> publish_<layer>) x bronze, silver, gold >> query

Each layer is written to the Iceberg branch ``audit_<run time>``, gated there with the same Soda contracts,
and only then fast-forwarded to ``main``. A blocked gate fails ``dq_<layer>``; ``publish_<layer>`` and every
later task never run, so consumers of ``main`` keep the previous good data and the branch stays for forensics.
Trigger with conf ``{"bad_batch": true}`` to write fixtures/bad_batch instead of the lakehouse sample.
The original DAGs are untouched.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from lakehouse_operators import spark_submit_task
from wap_operators import LAYERS, wap_gate_task, wap_publish_task, wap_write_task

with DAG(
    dag_id="lakehouse_retail_medallion_wap",
    description="Retail medallion, write-audit-publish: audit branch, Soda gate, fast-forward main",
    start_date=datetime(2024, 3, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,  # fast-forward needs main to be the branch's ancestor
    tags=["lakehouse", "retail", "medallion", "soda", "data-quality", "wap"],
) as dag:
    previous = spark_submit_task("land_smoke", "retail/01_smoke_test.py")
    for layer in LAYERS:
        write, gate, publish = wap_write_task(layer), wap_gate_task(layer), wap_publish_task(layer)
        previous >> write >> gate >> publish
        previous = publish
    previous >> spark_submit_task("query_timetravel", "retail/05_query_timetravel.py")
