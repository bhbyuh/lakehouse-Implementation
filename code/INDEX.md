# Code Index

Quick lookup of where each feature / test lives in this folder.
Cell numbers are 0-based (top cell = 0), as shown in JupyterLab's cell order.

## Files

| File | Purpose |
|---|---|
| `lakehouse.ipynb` | Original lakehouse setup: Hive Metastore tables, Iceberg migration, Kafka streaming |
| `spark_tester.ipynb` | Spark internals experiments: JVM/Py4J plans, caching, broadcasting, backfilling |
| `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | New features when upgrading from Spark 3.5 to 4.x |
| `Java UDFs.ipynb` | Registers and tests the Java UDF jar from `my_udf/` |
| `my_udf/` | Maven project for Java UDFs (`SanitizeIPUdf.java`), built to `target/my-udfs.jar` |

## Features / Tests

| Feature / Test | File | Cells | Notes |
|---|---|---|---|
| Create `demo` database, plain Parquet tables (customers, orders) | `lakehouse.ipynb` | 2–8 | Hive Metastore catalog |
| Iceberg table creation and migration from Parquet | `lakehouse.ipynb` | 9–13 | `demo.customers_iceberg` |
| Kafka readStream → Iceberg (`demo.events`) | `lakehouse.ipynb` | 17–24 | Section "Kafka Implementation" |
| Write stream back to Kafka | `lakehouse.ipynb` | 28 | |
| Lakehouse catalog config (Iceberg on MinIO) | `spark_tester.ipynb` | 0–5 | Same setup also in Spark 4 notebook, cells 0–1 |
| Iceberg table `demo.cache_test` (1M rows) | `spark_tester.ipynb` | 6–9 | |
| JVM-side Spark APIs via Py4J (plans, stats, cached RDD lookup) | `spark_tester.ipynb` | 10–17 | |
| Table caching verified in SQL/DataFrame DAG | `spark_tester.ipynb` | 18–21 | |
| Manual broadcast join | `spark_tester.ipynb` | 22–27 | |
| Executor config check | `spark_tester.ipynb` | 33 | |
| Data backfilling (`demo.backfill_demo`) | `spark_tester.ipynb` | 36–42 | |
| ANSI mode on by default | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 3–5 | |
| VARIANT data type (`parse_json`, `variant_get`) | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 6–10 | |
| Java 11 → 21 runtime | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 11–12 | |
| Python Data Source API (custom reader) | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 13–16 | `PostsAPIDataSource` |
| Python UDTFs (polymorphic) and Arrow UDFs | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 18–23 | `poly_inspector` |
| Spark Connect (`sc://localhost:15002`) | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 24–27 | |
| Stateful streaming + State Store reader | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 28–34 | `state-metadata`, `statestore`, change feed |
| Plain-text vs structured (JSON) logging | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 35–43 | `SPARK_LOG_SCHEMA` |
| Continuous vs Real-time streaming mode | `spark 3.5.x -> 4.0,4.1,4.2 .ipynb` | 46–57 | `watch_stages()` helper in cell 50 |
| Java UDF (`SanitizeIP`) | `Java UDFs.ipynb` + `my_udf/` | 0 | Uses `spark.jars` |

## How to add an entry

When you add or test something, add one row to **Features / Tests**:
`| What it is | file name | cell range | anything useful |`.
If you add a new file, also add it to **Files**.
