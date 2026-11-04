"""Kafka -> Spark -> Iceberg -> Trino -> Superset, end to end.

1. stream_kafka_to_iceberg : Spark job reads new Kafka messages, appends to
                             lakehouse.demo.kafka_events (Iceberg), then exits.
2. build_summary_via_trino : Trino aggregates it into iceberg.demo.user_spend_summary.
3. create_superset_chart   : Superset API registers that table + a bar chart
                             (skips whatever already exists).

Before triggering: run `python kafka_producer.py` so there is data in Kafka.
Note: Spark calls the catalog `lakehouse`, Trino calls the same one `iceberg`.
"""

import json
from datetime import datetime

import requests
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SUPERSET = "http://superset:8088"
CHART_NAME = "Total amount per user"


def create_superset_chart():
    s = requests.Session()
    token = s.post(f"{SUPERSET}/api/v1/security/login", json={
        "username": "admin", "password": "admin", "provider": "db", "refresh": True,
    }).json()["access_token"]
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["X-CSRFToken"] = s.get(f"{SUPERSET}/api/v1/security/csrf_token/").json()["result"]

    def find(resource, col, value):
        q = f"(filters:!((col:{col},opr:eq,value:'{value}')))"
        res = s.get(f"{SUPERSET}/api/v1/{resource}/", params={"q": q}).json()["result"]
        return res[0]["id"] if res else None

    # Database connection "Lakehouse" (-> Trino) is created by superset-init
    db_id = find("database", "database_name", "Lakehouse")

    dataset_id = find("dataset", "table_name", "user_spend_summary")
    if dataset_id is None:
        r = s.post(f"{SUPERSET}/api/v1/dataset/", json={
            "database": db_id, "schema": "demo", "table_name": "user_spend_summary",
        })
        r.raise_for_status()
        dataset_id = r.json()["id"]
    else:
        # pick up any column changes in the table
        s.put(f"{SUPERSET}/api/v1/dataset/{dataset_id}/refresh").raise_for_status()

    if find("chart", "slice_name", CHART_NAME) is None:
        params = {
            "viz_type": "echarts_timeseries_bar",
            "datasource": f"{dataset_id}__table",
            "x_axis": "user_name",
            "metrics": [{
                "expressionType": "SIMPLE",
                "column": {"column_name": "total_amount"},
                "aggregate": "SUM",
                "label": "total_amount",
            }],
            "groupby": [],
            "row_limit": 100,
        }
        r = s.post(f"{SUPERSET}/api/v1/chart/", json={
            "slice_name": CHART_NAME,
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        })
        r.raise_for_status()

    print(f"Open Superset -> Charts -> '{CHART_NAME}' (http://localhost:8088/chart/list/)")


with DAG(
    dag_id="kafka_to_superset",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger manually from the UI
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "kafka", "superset"],
) as dag:

    stream_kafka_to_iceberg = SparkSubmitOperator(
        task_id="stream_kafka_to_iceberg",
        conn_id="spark_default",  # spark://spark-master:7077
        application="/opt/airflow/code/kafka_to_iceberg.py",
        name="kafka_to_iceberg",
        conf={
            "spark.cores.max": "2",  # leave the other 2 cores for Spark Connect
            "spark.executor.memory": "1g",
        },
    )

    build_summary_via_trino = SQLExecuteQueryOperator(
        task_id="build_summary_via_trino",
        conn_id="trino_default",
        sql="""
        CREATE OR REPLACE TABLE iceberg.demo.user_spend_summary AS
        SELECT user_name,
               count(*)    AS num_events,
               sum(amount) AS total_amount,
               max(event_time) AS last_event_time
        FROM iceberg.demo.kafka_events
        GROUP BY user_name
        """,
    )

    create_chart = PythonOperator(
        task_id="create_superset_chart",
        python_callable=create_superset_chart,
    )

    stream_kafka_to_iceberg >> build_summary_via_trino >> create_chart
