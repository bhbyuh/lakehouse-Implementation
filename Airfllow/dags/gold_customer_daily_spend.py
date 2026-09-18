"""Rebuild the gold aggregate served to Trino/BI.

Batch, not streaming: gold is a projection of silver, so it is cheaper and
safer to recompute it after maintenance than to keep it incrementally.
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="gold_customer_daily_spend",
    start_date=datetime(2026, 1, 1),
    schedule="30 3 * * *",  # after iceberg_maintenance
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "gold"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="rebuild",
        conn_id="trino_default",
        sql="""
        CREATE OR REPLACE TABLE iceberg.gold.customer_daily_spend
        WITH (partitioning = ARRAY['day(event_day)']) AS
        SELECT customer_id,
               date_trunc('day', event_time) AS event_day,
               count(*)    AS num_events,
               sum(amount) AS total_spent
        FROM iceberg.silver.events
        GROUP BY customer_id, date_trunc('day', event_time)
        """,
    )
