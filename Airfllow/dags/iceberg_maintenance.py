"""Nightly Iceberg maintenance: compact, expire, then sweep orphans.

Without this, a 60-second streaming trigger produces ~1,440 data files and
~1,440 snapshots per table per day. Query planning degrades steadily and
expire_snapshots eventually takes longer than the window it enforces.

Trino runs the procedures rather than Spark: the Iceberg connector has them
built in, so there is no JVM, no cluster dependency and no driver-host
problem (F-11) in the Airflow worker.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

TABLES = ["bronze.events_raw", "silver.events"]

with DAG(
    dag_id="iceberg_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["lakehouse", "maintenance"],
) as dag:
    previous = None

    for table in TABLES:
        name = table.replace(".", "_")

        optimize = SQLExecuteQueryOperator(
            task_id=f"optimize__{name}",
            conn_id="trino_default",
            sql=f"ALTER TABLE iceberg.{table} EXECUTE optimize(file_size_threshold => '128MB')",
        )
        expire = SQLExecuteQueryOperator(
            task_id=f"expire_snapshots__{name}",
            conn_id="trino_default",
            sql=f"ALTER TABLE iceberg.{table} EXECUTE expire_snapshots(retention_threshold => '7d')",
        )
        orphans = SQLExecuteQueryOperator(
            task_id=f"remove_orphan_files__{name}",
            conn_id="trino_default",
            sql=f"ALTER TABLE iceberg.{table} EXECUTE remove_orphan_files(retention_threshold => '7d')",
        )

        # Order is not arbitrary: optimize writes new files and orphans the old
        # ones, expire_snapshots releases the snapshots still referencing them,
        # and only then can remove_orphan_files delete anything.
        optimize >> expire >> orphans

        # One table at a time — a 2GB Trino should not compact two at once.
        if previous:
            previous >> optimize
        previous = orphans
