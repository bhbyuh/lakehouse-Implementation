"""Rebuild the gold aggregate served to Trino/BI.

Batch, not streaming: gold is a projection of silver, so it is cheaper and
safer to recompute it after maintenance than to keep it incrementally.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="gold_customer_daily_spend",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(seconds=10),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "gold"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="rebuild",
        conn_id="trino_default",
        sql="""
        SELECT COUNT(*)
        FROM iceberg.demo.cache_test
        """,
    )
