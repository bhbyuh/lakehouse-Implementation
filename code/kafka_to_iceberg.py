"""Kafka topic `events` -> Iceberg table lakehouse.demo.kafka_events.

Submitted by Airflow (DAG kafka_to_superset) with spark-submit.
Uses trigger(availableNow=True): read everything new since the last
checkpoint, write it, then STOP -- so the Airflow task finishes and the
next task can run. The checkpoint remembers the Kafka offsets, so each
run only picks up new messages.

Messages come from ../kafka_producer.py:
    {"user": "alice", "amount": 42, "ts": "2026-11-04T15:00:00+00:00"}
"""

import os
import socket

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

CATALOG = "lakehouse"
C = f"spark.sql.catalog.{CATALOG}"
TABLE = f"{CATALOG}.demo.kafka_events"
CHECKPOINT = "s3a://buck1/checkpoints/kafka_events"

spark = (
    SparkSession.builder
    .appName("kafka_to_iceberg")
    # The driver runs inside the Airflow worker container; tell the Spark
    # executors which IP to call back on.
    .config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Iceberg catalog backed by Hive Metastore, data in MinIO (same as lakehouse.ipynb)
    .config(C, "org.apache.iceberg.spark.SparkCatalog")
    .config(f"{C}.type", "hive")
    .config(f"{C}.uri", os.environ["HIVE_METASTORE_URI"])
    .config(f"{C}.warehouse", os.environ["ICEBERG_WAREHOUSE"])
    .config(f"{C}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"{C}.s3.endpoint", os.environ["S3_ENDPOINT"])
    .config(f"{C}.s3.path-style-access", "true")
    .config(f"{C}.http-client.type", "apache")
    .config("spark.sql.defaultCatalog", CATALOG)
    # S3A is used only for the streaming checkpoint
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        user_name  STRING,
        amount     INT,
        event_time TIMESTAMP
    ) USING ICEBERG
""")

event_schema = StructType([
    StructField("user", StringType()),
    StructField("amount", IntegerType()),
    StructField("ts", StringType()),
])

# 1. Read: Kafka (broker:9092 is the in-network listener)
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "broker:9092"))
    .option("subscribe", "events")
    .option("startingOffsets", "earliest")
    # The playground broker has no volume, so a restart wipes the topic and the
    # checkpointed offsets no longer exist. Skip them (logged as a WARN) instead
    # of failing. In production keep the default (true) so real data loss is loud.
    .option("failOnDataLoss", "false")
    .load()
)

# 2. Transform: JSON bytes -> columns
events_df = (
    kafka_df
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
    .select(
        F.col("e.user").alias("user_name"),
        F.col("e.amount").alias("amount"),
        F.to_timestamp("e.ts").alias("event_time"),
    )
)

# 3. Write: append to Iceberg, then stop once caught up
query = (
    events_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
)
query.awaitTermination()

print(f"Done. {TABLE} now has {spark.table(TABLE).count()} rows.")
spark.stop()
