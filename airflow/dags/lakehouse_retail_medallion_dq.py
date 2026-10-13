"""
Retail medallion with Soda quality gates (contract-gated medallion):

    land/smoke >> bronze >> dq_bronze >> silver >> dq_silver >> gold >> dq_gold >> query

Same Spark jobs as ``lakehouse_retail_medallion`` (that DAG is untouched). Each ``dq_<layer>`` task runs
the layer's Soda contracts; a blocked gate fails the task, so the next layer is never built from bad data.
Warnings are logged and the run continues.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from dq_operators import soda_gate_task
from lakehouse_operators import spark_submit_task

with DAG(
    dag_id="lakehouse_retail_medallion_dq",
    description="Retail bronze → silver → gold with Soda Core v4 contract gates between layers",
    start_date=datetime(2024, 3, 1),
    schedule=None,  # manual trigger for local demos, like the original DAG
    catchup=False,
    tags=["lakehouse", "retail", "medallion", "soda", "data-quality"],
) as dag:
    smoke = spark_submit_task("land_smoke", "retail/01_smoke_test.py")
    bronze = spark_submit_task("bronze", "retail/02_ingest_bronze.py")
    dq_bronze = soda_gate_task("bronze")
    silver = spark_submit_task("silver", "retail/03_transform_silver.py")
    dq_silver = soda_gate_task("silver")
    gold = spark_submit_task("gold", "retail/04_publish_gold.py")
    dq_gold = soda_gate_task("gold")
    query = spark_submit_task("query_timetravel", "retail/05_query_timetravel.py")

    smoke >> bronze >> dq_bronze >> silver >> dq_silver >> gold >> dq_gold >> query
