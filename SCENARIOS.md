# Data Engineering Scenarios — Lakehouse Playground

Day-to-day data engineering work, reproduced on the stack in this repo.

The scenarios are ordered **Easy → Medium → Hard**. Together they cover roughly
95% of what an actual DE job asks you to do: ingest messy files, land them in a
table format, keep the table healthy, join and aggregate at scale, orchestrate
it on a schedule, handle late/duplicate/broken data, and debug it at 3am when it
breaks.

Nothing here needs a new service. Everything runs on what is already in
`docker-compose.yaml` + `Airfllow/docker-compose.yaml`.

---

## How to use this document

Each scenario has the same shape:

| Field | Meaning |
|---|---|
| **Goal** | The engineering skill being drilled |
| **Real-world** | The ticket you'd actually get at work |
| **Run in** | Jupyter / Trino CLI / Airflow / shell |
| **Do** | The steps |
| **Done when** | Objective check — no hand-waving |
| **Gotcha** | The thing that bites people. The reason the scenario exists |

Rules that make this worth doing:

1. **Write the "done when" check before the code.** If you can't state the
   assertion, you don't understand the requirement yet.
2. **Break it on purpose.** A scenario you only ran on the happy path taught you
   nothing. Kill a worker, corrupt a row, re-run a DAG twice.
3. **Keep a log.** Append every failure + root cause to `errors.txt` — that file
   is already the most valuable artifact in this repo.
4. **Re-run everything twice.** If run #2 changes the row count, the pipeline is
   not idempotent and it is broken, whether or not it errored.

---

## Environment cheat sheet

| Thing | Value |
|---|---|
| Jupyter Lab | http://localhost:8888 (no token) |
| Spark Master UI | http://localhost:8080 · workers 8081 / 8082 |
| Spark app UI | http://localhost:4040 (while a session is alive) |
| Trino UI | http://localhost:8085 |
| Kafka UI | http://localhost:7070 |
| MinIO Console | http://localhost:9001 — `admin` / `12345678` |
| Airflow UI | http://localhost:8090 — `airflow` / `airflow` |
| Kafka bootstrap | `broker:9092` inside network, `localhost:9094` from host |
| Trino catalog | `iceberg` (Hive Metastore type) |
| Warehouse root | `s3://buck1/warehouse` |
| Spark worker size | 2 cores / 2 GB each — **deliberately small; treat it as a constraint, not a bug** |

Start order:

```bash
docker network create lakehouse-network        # once
docker compose up -d                           # repo root
docker compose -f Airfllow/docker-compose.yaml up -d
```

Handy shells:

```bash
# Trino SQL shell
docker exec -it trino trino --catalog iceberg

# Kafka admin
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --list

# MinIO file listing (see what a table actually looks like on disk)
docker run --rm --network lakehouse-network quay.io/minio/mc:latest sh -c \
  "mc alias set m http://minio:9000 admin 12345678 && mc ls -r m/buck1/warehouse"
```

### The Spark session you'll reuse everywhere

Put this in the first cell of every notebook. The three-schema medallion layout
(`bronze` / `silver` / `gold`) matches the DAGs already in `Airfllow/dags/`.

```python
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("scenarios")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "12345678")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.warehouse.dir", "s3a://buck1/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

for db in ("bronze", "silver", "gold", "quarantine"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
```

### The seed generator you'll reuse everywhere

Save as `code/seed.py`. Most scenarios say "generate N events" — this is that.
It can write files *or* produce to Kafka, and it can inject the exact defects
each scenario needs.

```python
"""Event generator with controllable defects."""
import json, random, uuid, datetime as dt

CUSTOMERS = list(range(1, 501))
TYPES = ["view", "add_to_cart", "purchase", "refund"]

def make_event(day: dt.date, *, dupe=False, late=False, bad=False, drift=False):
    ts = dt.datetime.combine(day, dt.time(random.randrange(24), random.randrange(60)))
    if late:                                  # event stamped days before it arrives
        ts -= dt.timedelta(days=random.randint(1, 5))
    e = {
        "event_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "event_type": random.choice(TYPES),
        "amount": round(random.uniform(1, 900), 2),
        "event_time": ts.isoformat(),
    }
    if bad:                                   # the kind of row that kills a job
        e["amount"] = random.choice(["N/A", None, "12,50", -1])
    if drift:                                 # a field nobody told you about
        e["channel"] = random.choice(["ios", "android", "web"])
    return e

def batch(day, n=10_000, dupe_pct=0.02, late_pct=0.03, bad_pct=0.01, drift_pct=0.0):
    out = []
    for _ in range(n):
        e = make_event(day,
                       late=random.random() < late_pct,
                       bad=random.random() < bad_pct,
                       drift=random.random() < drift_pct)
        out.append(e)
        if random.random() < dupe_pct:
            out.append(dict(e))               # exact duplicate, same event_id
    random.shuffle(out)
    return out

if __name__ == "__main__":
    import sys
    day = dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else dt.date.today()
    for e in batch(day):
        print(json.dumps(e))
```

```bash
# to a file the Spark containers can read
docker exec jupyter-local python3 /app/code/seed.py 2026-03-01 > data/events_2026-03-01.json

# to Kafka
docker exec jupyter-local python3 /app/code/seed.py 2026-03-01 \
  | docker exec -i broker /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server broker:9092 --topic events
```

---

# 🟢 EASY — the daily grind

These are the tasks that fill most of an actual working day. They are "easy"
only in the sense that each one is a single concept. Doing them *correctly* —
idempotent, typed, verified — is the whole job.

---

### E1 — Land a raw file into a bronze Iceberg table

**Goal** File → table, without losing anything.
**Real-world** "Finance dropped a CSV in the bucket, get it queryable."
**Run in** Jupyter

**Do**
1. Generate `data/events_2026-03-01.json` with the seed script.
2. Read it with an **explicit schema** (not `inferSchema`).
3. Write `bronze.events_raw` as Iceberg, appending a `_ingested_at` and
   `_source_file` column (`F.input_file_name()`).
4. Query the same table from Trino: `SELECT count(*) FROM iceberg.bronze.events_raw;`

**Done when** Spark count == Trino count == source line count, and every row
carries the source filename.

**Gotcha** Bronze keeps the data **as it arrived** — no cleaning, no casting to
the "right" type, no dropping bad rows. The moment you clean in bronze you lose
the ability to prove what the source actually sent you.

---

### E2 — Explicit schema vs `inferSchema`

**Goal** Understand why schema inference is banned in production.
**Real-world** "The job worked yesterday and today `amount` is a string."

**Do**
1. Read the file with `inferSchema=True`, print the schema.
2. Re-read with 10 rows only (`samplingRatio`) — note the schema can change.
3. Time both reads; inference costs a full extra pass over the data.
4. Read with an explicit `StructType` and compare.

**Done when** You can state the inferred type of `amount` for two different
input files and explain why they differ.

**Gotcha** Inference is a *data-dependent* schema. Your table's contract must not
depend on today's sample.

---

### E3 — Survive malformed rows

**Goal** Don't let one bad row kill a 10M-row job.
**Real-world** "Job failed at 02:14. One record had `amount = 'N/A'`."

**Do**
1. Seed with `bad_pct=0.05`.
2. Read with `mode="PERMISSIVE"` + a `_corrupt_record` column in the schema.
3. Split: clean rows → `bronze.events_raw`, corrupt rows → `quarantine.events_bad`
   with the reason and the ingest timestamp.
4. Repeat with `mode="FAILFAST"` and watch it die.

**Done when** clean + quarantined == total input, and nothing was silently
dropped.

**Gotcha** `DROPMALFORMED` is the trap — it loses rows *silently*. Reconciliation
counts will never add up and you won't know why. Quarantine, never drop.

---

### E4 — Deduplicate on a natural key

**Goal** Exactly-once semantics at the data level.
**Real-world** "Revenue is 2% too high. Upstream retried a batch."

**Do**
1. Seed with `dupe_pct=0.02`.
2. Count duplicates: `GROUP BY event_id HAVING count(*) > 1`.
3. Dedupe with a window: `row_number() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC) = 1`.
4. Compare with `dropDuplicates(["event_id"])` — which row wins, and is it deterministic?

**Done when** `silver.events` has a provable unique `event_id`, and you can
explain which physical row survived.

**Gotcha** `dropDuplicates` keeps an *arbitrary* row. If the duplicates differ in
any column (a corrected amount!), arbitrary is wrong. Always dedupe with an
explicit ordering.

---

### E5 — Null and default handling

**Goal** Distinguish "missing", "empty", and "zero".
**Real-world** "Why do we have 4,000 customers with $0 lifetime spend?"

**Do**
1. Profile nulls per column: `df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])`.
2. Decide per column: reject / default / keep null. Write the decision in a comment.
3. Implement with `coalesce`, `fillna`, and a NOT NULL constraint on silver.

**Done when** Every nullable column in silver has a documented policy.

**Gotcha** `fillna(0)` on a currency column converts "we don't know" into "we
know it's zero". Downstream averages are now wrong and nobody will ever notice.

---

### E6 — Timestamp and date parsing

**Goal** The single most common source of silent corruption.
**Real-world** "Dashboard shows 1970 rows."

**Do**
1. Feed mixed formats: `2026-03-01T10:00:00`, `03/01/2026`, `2026-03-01 10:00:00+05:00`, epoch millis.
2. Parse each with `to_timestamp` + explicit format. Observe nulls on mismatch.
3. Set `spark.sql.session.timeZone` to `UTC`, then to `Asia/Karachi`, and re-run
   a daily aggregation. Watch the day boundaries move.

**Done when** You can state, for `gold.customer_daily_spend`, *which timezone a
"day" is in* — and the answer is written down.

**Gotcha** Spark's default session timezone is the JVM's. Your container, your
laptop and your warehouse can all disagree. Pin it to UTC everywhere and convert
only at the presentation layer.

---

### E7 — Partitioned write and partition pruning

**Goal** Prove that partitioning actually does something.
**Real-world** "The query scans the whole table even though I filtered on date."

**Do**
1. Create `silver.events` partitioned by `days(event_time)` (Iceberg hidden partitioning).
2. Load 30 days of seed data.
3. Run `SELECT count(*) FROM silver.events WHERE event_time >= DATE '2026-03-15'`.
4. In Trino: `EXPLAIN ANALYZE` the same query; check the input rows / splits.
5. Now filter on `date_format(event_time,'yyyy-MM-dd') = '2026-03-15'` and compare.

**Done when** You can show two queries returning identical results where one
scans ~1/30 of the data and the other scans all of it.

**Gotcha** Wrapping the partition column in a function defeats pruning. This is
*the* most common "why is my query slow" answer in real life.

---

### E8 — Append vs overwrite vs dynamic partition overwrite

**Goal** Pick the right write mode; the wrong one is a data-loss incident.
**Real-world** "The backfill wiped 90 days of history."

**Do**
1. Load day 1. Note the row count.
2. `mode("overwrite")` day 2 → observe day 1 is gone.
3. Use `overwritePartitions()` / `INSERT OVERWRITE` with dynamic partitions → only
   day 2 is replaced.
4. Use `append` twice → observe duplicates.

**Done when** You can restate each mode in one sentence and name the incident
each one causes.

**Gotcha** `overwrite` on a partitioned Iceberg table with no filter truncates
everything. Always express a backfill as "replace exactly these partitions".

---

### E9 — Write in Spark, read in Trino (and back)

**Goal** Engine interop through one metastore.
**Real-world** "Analysts use Trino, pipelines use Spark, same tables."

**Do**
1. Create a table in Spark, insert rows, then `SELECT` it in Trino without any
   refresh command.
2. Create a table in Trino (`CREATE TABLE iceberg.silver.t (...) WITH (partitioning = ARRAY['day(event_time)'])`),
   insert from Spark.
3. Compare `SHOW CREATE TABLE` output in both engines.

**Done when** Both engines read each other's writes with no manual metadata sync.

**Gotcha** This works *because* it's Iceberg. Do the same with a plain Parquet
Hive table and a new partition: Trino won't see it until the metastore is told.
That difference is the entire argument for a table format.

---

### E10 — Inspect table metadata

**Goal** Learn to answer "what is this table actually made of?"
**Run in** Trino

**Do**
```sql
SELECT * FROM iceberg.silver."events$snapshots" ORDER BY committed_at DESC;
SELECT * FROM iceberg.silver."events$files";
SELECT * FROM iceberg.silver."events$partitions";
SELECT * FROM iceberg.silver."events$manifests";
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.silver."events$files" ORDER BY file_size_in_bytes;
```

**Done when** You can state, for any table: snapshot count, file count, average
file size, and which partition holds the most data.

**Gotcha** Average file size is the health metric that predicts every future
performance complaint. Target ~128 MB. Watch it after every streaming run.

---

### E11 — Time travel and rollback

**Goal** Undo a bad write without a backup.
**Real-world** "The 3am job wrote garbage. Fix it before the 9am standup."

**Do**
1. Note the current snapshot: `SELECT * FROM iceberg.silver."events$snapshots"`.
2. Deliberately write garbage (e.g. all amounts × 100).
3. Query the old snapshot: `SELECT * FROM iceberg.silver.events FOR VERSION AS OF <snapshot_id>`
   (Spark: `spark.read.option("snapshot-id", ...)`).
4. Roll back: `CALL iceberg.system.rollback_to_snapshot('silver', 'events', <id>)`.

**Done when** The table is back to the pre-garbage state and you did not restore
a file from anywhere.

**Gotcha** Rollback is only possible while the snapshot still exists. Your
`iceberg_maintenance` DAG expires snapshots at 7 days — that retention *is* your
undo window. Set it deliberately.

---

### E12 — Additive schema evolution

**Goal** Add a column without rewriting the table.
**Real-world** "Product added a `channel` field, start capturing it."

**Do**
1. `ALTER TABLE silver.events ADD COLUMN channel STRING`.
2. Query old rows — `channel` is NULL, no rewrite happened.
3. Seed new data with `drift_pct=1.0` and append.
4. Check `$files` — confirm old data files were never touched.

**Done when** New and old rows coexist and no file was rewritten.

**Gotcha** Iceberg tracks columns by **ID**, not by position or name. That's why
this is free — and it's why the same operation on plain Parquet+Hive can
silently shift your columns.

---

### E13 — A real Airflow DAG with dependencies and retries

**Goal** Orchestration basics that survive contact with reality.
**Run in** `Airfllow/dags/`

**Do**
1. Write `bronze_to_silver.py`: extract → transform → quality-check → load, as
   four tasks with explicit dependencies.
2. Set `retries=2`, `retry_delay=5m`, `max_active_runs=1`, `catchup=False`.
3. Make the transform task fail on the first attempt (raise if
   `context["ti"].try_number == 1`) and confirm the retry succeeds.

**Done when** The DAG goes green on attempt 2 without manual intervention.

**Gotcha** `max_active_runs=1` is not optional for a pipeline that writes to a
table. Two concurrent runs of the same DAG writing the same partition is a
race, and Airflow will happily do it.

---

### E14 — Backfill one day

**Goal** Parameterised, re-runnable pipelines.
**Real-world** "Re-run 2026-03-05, the source was late."

**Do**
1. Make every task read `{{ ds }}` (logical date), never `datetime.now()`.
2. `airflow dags backfill -s 2026-03-05 -e 2026-03-05 bronze_to_silver`
   (`docker exec -it <scheduler> ...`).
3. Run it **twice**. Compare row counts.

**Done when** Run #2 produces byte-identical output to run #1.

**Gotcha** `datetime.now()` inside a task makes backfills produce the wrong
answer and makes reruns non-deterministic. Logical date, always.

---

### E15 — Kafka produce and consume

**Goal** Topic mechanics: partitions, keys, offsets, consumer groups.

**Do**
```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create \
  --topic events --bootstrap-server broker:9092 --partitions 3 --replication-factor 1

docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh \
  --topic events --bootstrap-server broker:9092 \
  --property parse.key=true --property key.separator=:

docker exec -it broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker:9092 --group my-group --describe
```
1. Produce with a key (`customer_id`) and without. Check partition distribution
   in Kafka UI (:7070).
2. Consume with two consumers in the same group, then in different groups.

**Done when** You can explain why keyed messages for the same customer always
land in the same partition, and what happens to a 4th consumer in a 3-partition
group.

**Gotcha** Ordering in Kafka is per-partition only. "Our events are out of
order" is almost always "we didn't key them".

---

### E16 — First streaming query

**Goal** Micro-batch mental model.

**Do**
1. `readStream` from Kafka with `startingOffsets="earliest"`, sink to `console`.
2. Inspect `query.lastProgress` — `numInputRows`, `inputRowsPerSecond`,
   `batchDuration`.
3. Switch to `startingOffsets="latest"` with the **same** checkpoint, then with a
   fresh one.

**Done when** You can reproduce the exact bug already recorded in `errors.txt`
(zero rows, `latest` offsets, checkpoint locking it in) **on purpose**.

**Gotcha** The checkpoint wins over your option. Once a checkpoint has recorded a
starting position, changing `startingOffsets` in code does nothing. New
checkpoint path or nothing.

---

### E17 — Read the physical layout in MinIO

**Goal** Demystify what a table *is* on object storage.

**Do**
1. `mc ls -r m/buck1/warehouse/silver/events/` — find `data/` and `metadata/`.
2. Open a `.metadata.json` — read `current-snapshot-id`, `schemas`, `partition-specs`.
3. Follow it: metadata.json → manifest list (`snap-*.avro`) → manifests → data files.
4. Count data files; compare to `$files`.

**Done when** You can trace a single row from a `SELECT` down to a physical
object key, by hand.

**Gotcha** There is no directory-per-partition requirement in Iceberg. Pruning
comes from manifest statistics, not from the path. This is why Iceberg can change
partitioning without moving files.

---

### E18 — Source-to-target reconciliation

**Goal** The check that catches most real incidents.
**Real-world** "Are we sure we loaded everything?"

**Do**
1. Count rows in the source file, in bronze, in quarantine, in silver.
2. Build a reconciliation table: `run_date, source_count, bronze_count,
   quarantine_count, silver_count, delta, explanation`.
3. Also reconcile a **sum** (total `amount`), not just counts.

**Done when** `bronze = source`, `silver + quarantine + duplicates_removed = bronze`,
and every delta has a written explanation.

**Gotcha** Row counts alone miss corruption that preserves cardinality. Always
reconcile at least one numeric aggregate.

---

### E19 — Basic data-quality checks

**Goal** Assertions as code.

**Do** Implement, as plain Spark SQL returning a failure count:
- not null: `event_id`, `customer_id`, `event_time`
- unique: `event_id`
- range: `amount BETWEEN 0 AND 10000`
- accepted values: `event_type IN ('view','add_to_cart','purchase','refund')`
- freshness: `max(event_time) > now() - interval 1 day`
- referential: every `customer_id` exists in the dimension

**Done when** One function returns a list of `(check_name, failed_rows)` and you
can run it against any table.

**Gotcha** A check that logs a warning gets ignored within a week. Decide up
front which checks **block the pipeline** — see M16.

---

### E20 — Incremental load by watermark column

**Goal** Stop reprocessing history every night.

**Do**
1. Keep a control table `meta.load_state (table_name, last_loaded_ts)`.
2. Read only `WHERE updated_at > last_loaded_ts`.
3. Update the watermark **after** a successful commit, in the same logical step.
4. Crash the job between the write and the watermark update. Re-run.

**Done when** The crash-and-rerun produces no duplicates and no gaps.

**Gotcha** Update the watermark *before* the write and a crash loses data;
update it in a separate non-atomic step and a crash duplicates data. Only an
idempotent write (MERGE, or partition overwrite) makes this safe — which is
exactly why M1/M2 exist.

---

# 🟡 MEDIUM — where the actual work is

---

### M1 — SCD Type 2 dimension

**Goal** Track history of a changing dimension.
**Real-world** "What city was this customer in when they placed that order?"

**Do**
1. Build `silver.dim_customer (customer_id, name, city, valid_from, valid_to, is_current, hash)`.
2. Day 1: initial load, all `is_current=true`, `valid_to='9999-12-31'`.
3. Day 2: some customers change city, some are new, some unchanged.
4. Implement with `MERGE INTO`: close the old row (`valid_to = now, is_current = false`)
   and insert the new version.
5. Compare with SCD1 (overwrite) on the same input.

**Done when** A customer who moved twice has exactly 3 rows, exactly 1 current,
no overlapping validity ranges — assert this with SQL.

**Gotcha** Iceberg `MERGE` can't both update the old row and insert a new one for
the same key in a single statement. Standard solution: two passes (close, then
insert), or a staged union with an explicit action column. Use a row hash to
detect change — comparing columns one by one breaks the moment schema evolves.

---

### M2 — Idempotent upsert (CDC merge)

**Goal** The most-used pattern in modern DE.

**Do**
1. Stage a batch containing inserts, updates and deletes (`op` = `c`/`u`/`d`).
2. `MERGE INTO silver.events t USING staged s ON t.event_id = s.event_id
   WHEN MATCHED AND s.op='d' THEN DELETE
   WHEN MATCHED THEN UPDATE SET *
   WHEN NOT MATCHED AND s.op<>'d' THEN INSERT *`
3. Run the same batch 3 times.
4. Now shuffle the batch so an update arrives *before* its insert, and re-run.

**Done when** Runs 1, 2 and 3 leave the table identical, and out-of-order input
still converges to the correct final state.

**Gotcha** Out-of-order CDC needs dedup-by-latest-version **before** the merge
(`row_number() OVER (PARTITION BY key ORDER BY lsn DESC)`), or an older version
overwrites a newer one. This is the #1 CDC bug in the wild.

---

### M3 — Exactly-once streaming into Iceberg

**Goal** Kafka → Iceberg without duplicates across restarts.

**Do**
1. `readStream` Kafka → `foreachBatch(lambda df, batch_id: merge_into(df))`.
2. Use `MERGE` on `event_id` inside `foreachBatch` — not a blind append.
3. Checkpoint to `s3a://buck1/checkpoints/events-v1`.
4. Kill the stream mid-batch (`docker restart` a worker). Restart it.
5. Count duplicates.

**Done when** Zero duplicates after three kill/restart cycles.

**Gotcha** Structured Streaming guarantees *at-least-once* to an arbitrary sink.
Exactly-once comes from the sink being idempotent — which is what the merge on
`event_id` buys you. `batch_id` is also available for dedup at the batch level.

---

### M4 — Late-arriving events and watermarks

**Goal** Windowed aggregation that tolerates lateness.

**Do**
1. Seed with `late_pct=0.20` (events stamped up to 5 days before arrival).
2. Aggregate 1-hour tumbling windows with `withWatermark("event_time", "10 minutes")`.
3. Count how many events are dropped as too-late.
4. Widen to `"3 days"` — compare state store size and memory in the Spark UI.
5. Implement the batch alternative: recompute the last N days nightly.

**Done when** You can quantify the trade-off: X% accuracy gained for Y MB of
state, and you can say which you'd choose for this pipeline.

**Gotcha** Watermark is a *promise about lateness*, and Spark drops anything
beyond it — silently. A real streaming design nearly always pairs a short
watermark with a nightly batch restatement, exactly like the
`gold_customer_daily_spend` DAG in this repo.

---

### M5 — Diagnose and fix data skew

**Goal** The most common "my job is slow" root cause.

**Do**
1. Seed a skewed dataset: 80% of events on 3 customer IDs.
2. Join events to a customer dimension. Watch the Spark UI: most tasks finish in
   seconds, 2–3 run for minutes.
3. Confirm skew: `df.groupBy("customer_id").count().orderBy(F.desc("count"))`.
4. Fix three ways and time each:
   - enable AQE skew join (`spark.sql.adaptive.skewJoin.enabled=true`)
   - salt the key (append `rand()%N`, explode the dimension N times)
   - broadcast the dimension if it's small enough

**Done when** You have a table of wall-clock times for all four variants on this
2-core cluster.

**Gotcha** The Spark UI's task-duration distribution (max vs median) is the
diagnostic. If max ≫ p75, it's skew, not "not enough memory" — and adding
executors will not help.

---

### M6 — Join strategy and broadcast tuning

**Goal** Know which join Spark picked and why.

**Do**
1. `df.explain(True)` — identify `BroadcastHashJoin` vs `SortMergeJoin`.
2. Move `spark.sql.autoBroadcastJoinThreshold` up and down; observe the plan flip.
3. Force with `F.broadcast(dim)`; then broadcast something too large and watch
   the driver struggle.
4. Turn AQE off and on and re-read the plan.

**Done when** You can predict the join strategy before running `explain`, three
times in a row.

**Gotcha** Broadcast threshold is compared against Spark's *estimate*. With no
statistics on a lakehouse table, the estimate can be wildly wrong — which is
what M14 (`ANALYZE`) is about.

---

### M7 — The small-files problem

**Goal** Understand and fix the defining pathology of streaming lakehouses.

**Do**
1. Run a streaming job with a 10-second trigger for 10 minutes into
   `bronze.events_raw`.
2. Count files: `SELECT count(*) FROM iceberg.bronze."events_raw$files"`.
   Expect ~60 per partition per 10 minutes — the DAG comment in
   `iceberg_maintenance.py` does this math for a full day.
3. Time a `SELECT count(*)` before compaction.
4. `ALTER TABLE iceberg.bronze.events_raw EXECUTE optimize(file_size_threshold => '128MB')`.
5. Time the same query after. Compare file counts.

**Done when** You can show the query time and file count before/after, and you
know the right trigger interval for this cluster.

**Gotcha** Compaction writes *new* files and leaves the old ones referenced by
old snapshots — storage goes **up** until `expire_snapshots` runs. That ordering
is exactly why the maintenance DAG chains optimize → expire → remove_orphans.

---

### M8 — Partition evolution

**Goal** Change partitioning on a live table without a rewrite.

**Do**
1. Start partitioned by `days(event_time)`; load 30 days.
2. `ALTER TABLE silver.events ADD PARTITION FIELD bucket(16, customer_id)`.
3. Load 10 more days.
4. Query across the boundary and inspect `$partitions` — two specs coexist.
5. Now try `DROP PARTITION FIELD days(event_time)` and query historical data.

**Done when** A single query correctly reads data written under two different
partition specs.

**Gotcha** Old data keeps its old spec — evolution is not a rewrite. Great for
agility, but it means pruning effectiveness differs by data age. If you need the
old data re-laid-out, that's H7.

---

### M9 — Run and validate the maintenance DAG

**Goal** Operate the table, don't just write to it.
**Run in** Airflow (`iceberg_maintenance` already exists)

**Do**
1. Generate the mess first: streaming job + several batch appends + a few deletes.
2. Record file count, snapshot count, and total bytes in MinIO.
3. Trigger `iceberg_maintenance`.
4. Record the same three numbers after each of the three tasks separately.
5. Now set `expire_snapshots` retention to `0d` and try time travel afterwards.

**Done when** You have a before/after table for all three metrics, and you have
personally destroyed your own ability to time travel.

**Gotcha** `remove_orphan_files` with a short retention can delete files that an
in-flight writer is still committing. In production its retention must exceed
your longest-running write. Never set it to zero on a live table.

---

### M10 — Backfill without double counting

**Goal** Restate history safely.
**Real-world** "The tax rate was wrong for all of February. Fix it."

**Do**
1. Load Feb with a bug (amount × 1.0 instead of × 1.17).
2. Build `gold.customer_daily_spend` from it.
3. Fix the logic and restate Feb **only**: replace exactly the affected
   partitions in silver, then rebuild the affected gold days.
4. Verify January and March are untouched, byte for byte (compare snapshot IDs).

**Done when** Only February's partitions have new snapshots.

**Gotcha** `CREATE OR REPLACE TABLE ... AS SELECT` (what the existing
`gold_customer_daily_spend` DAG does) rebuilds *everything* — fine for a small
gold table, unacceptable once it's large. Know when you've outgrown it and
what replaces it (`MERGE` or partition overwrite).

---

### M11 — Idempotent, parameterised Airflow task

**Goal** A task that is safe to run 100 times.

**Do**
1. Rewrite `bronze_to_silver` to take `{{ ds }}` and process exactly that day.
2. The write must be `INSERT OVERWRITE` of that one partition — never append.
3. `airflow dags backfill -s 2026-03-01 -e 2026-03-07`, twice.
4. Compare counts and per-partition snapshot history.

**Done when** Two full backfills of the same window give identical results.

**Gotcha** "Idempotent" means the *partition* is replaced, not that rows are
deduped afterwards. Design the write so re-running is a no-op by construction.

---

### M12 — Event-driven scheduling with Airflow Assets

**Goal** Replace "run gold 30 minutes after silver and hope" with real
dependency.
**Real-world** Your current `gold` DAG is scheduled at 03:30 purely because
maintenance runs at 03:00 — that's a guess, not a dependency.

**Do**
1. Define an `Asset` for `silver.events` (Airflow 3 `airflow.sdk.Asset`).
2. `bronze_to_silver` declares `outlets=[silver_events]`.
3. `gold_customer_daily_spend` switches to `schedule=[silver_events]`.
4. Delay the silver DAG by 2 hours and confirm gold waits instead of producing a
   partial result.

**Done when** Gold never runs on stale silver, with no time-based coupling left.

**Gotcha** Time-based coupling between DAGs is a latent incident: it works until
upstream is slow once. Also compare against `ExternalTaskSensor` — and know why
assets are better (no polling, no schedule-alignment trap).

---

### M13 — Multi-table consistent publish

**Goal** Readers never see a half-updated gold layer.

**Do**
1. Build three gold tables that must agree (daily spend, daily counts, customer
   summary).
2. Write all three, then flip readers over — either by writing to `_new` tables
   and renaming, or by publishing one snapshot per table and pinning readers.
3. Query continuously from Trino while the rebuild runs; log any inconsistent
   read.

**Done when** A continuous reader never observes a mix of old and new.

**Gotcha** Iceberg gives atomicity **per table**, not across tables. Cross-table
consistency is a design you implement (staging + swap), not a feature you enable.

---

### M14 — Statistics and query planning

**Goal** Make the optimiser's estimates real.

**Do**
1. In Trino: `EXPLAIN ANALYZE SELECT ... JOIN ...` — compare *estimated* vs
   *actual* rows at each stage.
2. `ANALYZE iceberg.silver.events;` then re-explain.
3. In Spark: `ANALYZE TABLE silver.events COMPUTE STATISTICS FOR ALL COLUMNS`,
   then `explain(True)`.
4. Find a query where a wrong estimate picks the wrong join order.

**Done when** You have one query whose plan measurably improves after `ANALYZE`.

**Gotcha** Bad estimates cause bad join orders, and bad join orders are
order-of-magnitude problems, not percentage problems. After a big load, stats
are stale — refresh them in the maintenance DAG.

---

### M15 — Fact/dimension gold model

**Goal** Dimensional modelling on a lakehouse.

**Do**
1. Build `gold.fact_events` (grain: one row per event) and `gold.dim_customer`
   (SCD2 from M1).
2. Join the fact to the dimension **as of the event time** (not current) —
   `AND e.event_time BETWEEN d.valid_from AND d.valid_to`.
3. Build `gold.customer_daily_spend` on top.
4. Write down the grain of every table, in the table comment.

**Done when** A customer who moved cities has their old city on old orders.

**Gotcha** Joining a fact to `is_current = true` silently rewrites history —
last year's revenue-by-city report changes every time someone moves. If the
business wants "current city", that's a *second*, explicitly-named column.

---

### M16 — Quality gate that stops the pipeline

**Goal** Fail loudly, fail early, quarantine the rest.

**Do**
1. Turn the E19 checks into an Airflow task between silver and gold.
2. Classify: `error` → raise `AirflowFailException` (gold does not run);
   `warn` → log and continue.
3. Route failing rows to `quarantine.events_bad` with the failed check name.
4. Inject a breach (20% null `customer_id`) and confirm gold is not rebuilt.
5. Add an `AirflowSkipException` path so downstream skips rather than fails when
   the source is legitimately empty.

**Done when** Bad data cannot reach gold, and the failure names the exact check
and row count.

**Gotcha** A quality gate that isn't on the critical path isn't a gate. And the
error message must name the check — "task failed" at 3am costs you 20 minutes.

---

### M17 — Streaming ingest with a dead-letter path

**Goal** Streaming that never stops for one bad message.

**Do**
1. Produce a mix of valid JSON, invalid JSON, and valid JSON with a wrong schema.
2. In `foreachBatch`: parse, split valid/invalid, write valid → bronze,
   invalid → `quarantine.events_dlq` (raw bytes + error + offset + partition).
3. Kill and restart; confirm no message is lost on either path.

**Done when** The stream survives a 100% invalid batch and you can replay from
the DLQ by offset.

**Gotcha** Store the **raw bytes plus Kafka coordinates** (topic/partition/offset)
in the DLQ. Storing only the parsed-and-failed representation makes replay
impossible.

---

### M18 — Schema drift from upstream

**Goal** Handle "they added a field and told nobody".

**Do**
1. Stream with a fixed schema; mid-stream switch the producer to `drift_pct=1.0`
   (adds `channel`).
2. Observe: the new field is dropped silently.
3. Implement detection: keep the raw JSON in bronze, compare
   `schema_of_json` against the registered schema, alert on difference.
4. Auto-evolve silver with `ALTER TABLE ... ADD COLUMN`, then reprocess bronze to
   backfill `channel` for the drifted window.

**Done when** You detect the drift within one batch, and can backfill the new
column for data that arrived before you noticed.

**Gotcha** Keeping the raw payload in bronze is what makes the backfill possible.
This is the concrete payoff of "bronze stores data as it arrived" from E1.

---

### M19 — Reprocess from a Kafka offset

**Goal** Replay history after a logic bug.

**Do**
1. Note the current offsets (`kafka-consumer-groups.sh --describe`).
2. Fix a "bug" in the transform.
3. Reprocess three ways and compare:
   - new checkpoint + `startingOffsets` JSON per partition
   - batch read with `startingOffsets`/`endingOffsets` bounded
   - reprocess from bronze instead of Kafka
4. Ensure the target ends with no duplicates (merge on `event_id`).

**Done when** You replay a known window and land on exactly the same row count
as a clean load.

**Gotcha** Kafka retention is finite. If retention is 7 days, Kafka is not your
replay source for a 30-day bug — bronze is. That's the real reason bronze exists.

---

### M20 — Prove predicate and column pushdown

**Goal** Understand where the I/O savings actually come from.

**Do**
1. `SELECT *` vs `SELECT customer_id, amount` on the same filter — compare bytes
   scanned in Trino's UI (:8085) query details.
2. Filter on a partition column vs a non-partition column vs a non-partition
   column with good min/max clustering.
3. `ORDER BY`/`sortWithinPartitions` on `customer_id` before writing, then
   re-run the non-partition filter. Compare files scanned via `$files` min/max.

**Done when** You can rank the three techniques (partitioning, column pruning,
sort clustering) by bytes saved for *your* query mix.

**Gotcha** Iceberg prunes files by min/max stats per column. Unsorted writes give
overlapping ranges, so every file is a candidate. Sorting on the common filter
column is often a bigger win than adding another partition level.

---

### M21 — Configuration and secrets hygiene

**Goal** One definition of every connection fact.

**Do**
1. Inventory where `minio:9000` / `admin` / `12345678` appear today: root
   compose, Airflow compose, `trino/etc/catalog/iceberg.properties`,
   `hive_custom_conf/hive-site.xml`, notebooks.
2. Collapse the compose duplication using the existing `x-s3-env` /
   `x-lakehouse-env` anchors.
3. Make notebooks read `os.environ["S3_ENDPOINT"]` instead of hardcoding.
4. Break it on purpose: change the password in one place only, and see which
   service fails and with what error.

**Done when** Changing the MinIO password requires editing exactly the number of
places you decided on, and you've seen each failure mode.

**Gotcha** This is a playground, so credentials stay committed on purpose — the
skill being drilled is **de-duplication**, not secret management. Note which
files would become `.env`/Vault-backed in a real deployment.

---

# 🔴 HARD — incidents, scale, and design

---

### H1 — End-to-end exactly-once under injected failure

**Goal** Prove your pipeline's correctness claim instead of asserting it.

**Do**
1. Produce a known 100,000 events to Kafka with deterministic IDs.
2. Run Kafka → bronze → silver → gold with merges at each hop.
3. While it runs, inject: `docker restart spark-worker-a`, then `broker`, then
   `hive-metastore`, then `minio`, one at a time, 60 s apart.
4. After recovery, assert: exactly 100,000 distinct `event_id` in silver, gold
   sums match a from-scratch recompute.

**Done when** All four failures are survived with exact counts, or you can name
precisely which hop loses/duplicates data and why.

**Gotcha** Metastore restart is the interesting one — the commit protocol depends
on it. Expect commit failures, and design the retry. "Exactly once" that has
never been tested under failure is just "once, so far".

---

### H2 — Executor loss mid-shuffle

**Goal** Understand Spark's recovery boundaries.

**Do**
1. Run a wide join/aggregation big enough to spill (watch Shuffle Spill in the UI).
2. Kill `spark-worker-b` mid-stage.
3. Observe stage retry, task re-attempts, and total wall clock.
4. Repeat with `spark.sql.adaptive.enabled=false` and compare.
5. Repeat killing the **driver** (the Jupyter kernel) and note what is *not*
   recoverable.

**Done when** You can explain which work was recomputed, which was not, and why
losing the driver is categorically different from losing an executor.

**Gotcha** Shuffle output lives on the executor. Losing an executor means
recomputing its upstream tasks, not just re-running the shuffle read. On a
2-worker cluster that's a 50% loss.

---

### H3 — Concurrent writer conflict

**Goal** Optimistic concurrency, for real.

**Do**
1. Two notebooks (or a stream + a batch job) `MERGE` into `silver.events`
   simultaneously, touching overlapping partitions.
2. Observe `CommitFailedException` / validation failure.
3. Tune `commit.retry.num-retries` and `commit.retry.min-wait-ms`.
4. Re-run with the writers touching **disjoint** partitions and compare.
5. Compare copy-on-write vs merge-on-read
   (`write.update.mode`, `write.merge.mode`) for conflict rate and read cost.

**Done when** You can state which concurrent operations conflict on this table
and which don't, and you've configured retries that actually resolve it.

**Gotcha** Iceberg is optimistic: conflicts surface at *commit*, after all the
work is done. Two long merges on the same partitions can livelock — partition
your writers, don't just raise the retry count.

---

### H4 — Streaming and batch on one table

**Goal** The mixed workload that breaks naive designs.

**Do**
1. Continuous stream appending to `silver.events` (30 s trigger).
2. Simultaneously: the nightly `iceberg_maintenance` DAG (optimize + expire +
   orphans) **and** the gold rebuild.
3. Run a long analytical query in Trino throughout.
4. Log: stream failures, query failures, and whether the Trino query saw a
   consistent snapshot.

**Done when** You know exactly which combination breaks, and you have a schedule
or isolation strategy that prevents it.

**Gotcha** `remove_orphan_files` and an in-flight streaming commit are the
dangerous pair (see M9). Snapshot isolation protects the *reader*; it does not
protect a writer from a concurrent file cleaner.

---

### H5 — Full CDC pipeline with deletes

**Goal** Mirror an OLTP table into the lakehouse.

**Do**
1. Use the Postgres already in the stack as the source. Create `customers`,
   `INSERT`/`UPDATE`/`DELETE` against it.
2. Emit Debezium-shaped envelopes (`before`, `after`, `op`, `ts_ms`, `lsn`) to
   Kafka — hand-rolled is fine, the shape is the lesson.
3. Consume → dedup by `(key, max lsn)` → `MERGE` into `silver.dim_customer`
   handling `c`/`u`/`d`.
4. Handle: out-of-order arrival, a delete followed by a re-insert of the same
   key, and a full snapshot ("read") batch replayed over existing data.
5. Reconcile row-by-row against Postgres.

**Done when** `silver.dim_customer` matches `SELECT * FROM customers` exactly,
after a shuffled event stream containing all four operations.

**Gotcha** Tombstones and the snapshot-then-stream handover are where CDC
pipelines actually break. A delete arriving before its insert must not resurrect
the row — order by LSN, not arrival time.

---

### H6 — Memory tuning and OOM forensics

**Goal** Debug a job that dies on 2 GB executors.

**Do**
1. Write a job that OOMs: `collect()` a large result, or group by a
   high-cardinality key with a huge `collect_list`.
2. Read the failure: executor lost vs driver OOM vs GC overhead limit.
3. Tune in order and measure each: `spark.sql.shuffle.partitions` (default 200 is
   wrong for a 4-core cluster), `spark.memory.fraction`,
   `spark.sql.adaptive.coalescePartitions.enabled`, then finally the algorithm.
4. Rewrite the query to avoid the materialisation entirely.

**Done when** The job completes on the same 2 GB executors, and you can name
which change mattered most.

**Gotcha** `spark.sql.shuffle.partitions=200` on 4 total cores means 200 tiny
tasks with 50 waves of scheduling overhead. Match it to your parallelism — this
single setting is the most common easy win in real clusters.

---

### H7 — Re-partition a large table with zero downtime

**Goal** A migration, executed safely.
**Real-world** "Partitioning by day was wrong; we query by customer."

**Do**
1. Build a 30-day table partitioned by `days(event_time)`. Record query times for
   two representative queries.
2. Plan a migration to `days(event_time) + bucket(32, customer_id)`.
3. Execute as shadow-write: build `silver.events_v2`, backfill, validate
   (counts + sums + a row-level sample), then swap names atomically.
4. Keep readers running throughout; log any failed query.
5. Write the rollback procedure **before** the swap.

**Done when** The swap is invisible to a continuously-running Trino query, and
you can roll back in one statement.

**Gotcha** `ADD PARTITION FIELD` (M8) only affects *new* data. Re-laying-out
existing data is a rewrite, and a rewrite of a live table is a migration with a
rollback plan — not an `ALTER`.

---

### H8 — 90-day backfill inside an SLA

**Goal** Large backfill without starving production.

**Do**
1. Seed 90 days (~1M rows).
2. Naive: one Airflow backfill with unlimited parallelism. Measure, and watch the
   nightly job miss its window.
3. Constrain: `max_active_tis_per_dag`, a Celery queue/pool for backfills, and
   chunking (7 days at a time).
4. Add resumability: if it dies on day 47, restart resumes at 47.
5. Compare against a single big Spark job processing all 90 days.

**Done when** The backfill completes, the nightly production DAG never misses its
SLA, and a mid-way kill resumes correctly.

**Gotcha** Airflow pools are the mechanism for "backfill may use at most N slots".
Without one, a backfill is a self-inflicted denial of service on your own
scheduler.

---

### H9 — Metastore outage drill

**Goal** Know your blast radius.

**Do**
1. Under load (stream + Trino queries): `docker stop hive-metastore`.
2. Record exactly what fails and what keeps working. Does an already-planned
   Trino query survive? Does a running Spark stream?
3. Restart it. Measure recovery time and whether anything needed manual repair.
4. Now stop `hive-postgres` instead and repeat.
5. Write a one-page runbook: symptom → diagnosis → fix → prevention.

**Done when** The runbook exists and a second person could follow it.

**Gotcha** The metastore is the single point of failure in this architecture —
and its Postgres is the SPOF behind the SPOF. This drill is the concrete argument
for a REST catalog, and knowing *why* is worth more than the migration.

---

### H10 — Breaking schema change

**Goal** Roll out an incompatible change with live consumers.

**Do**
1. `silver.events.amount` is DOUBLE; the business needs `DECIMAL(18,2)` for exact
   money.
2. Attempt the direct `ALTER` — observe what Iceberg permits and what it refuses.
3. Do it properly: add `amount_decimal`, dual-write, backfill, migrate consumers,
   verify, drop the old column.
4. Repeat for a rename (`customer_id` → `cust_id`) and check whether Trino and
   Spark agree.
5. Document a deprecation timeline.

**Done when** No consumer breaks at any point in the sequence, and you can state
which changes Iceberg allows in place and which need the expand/contract dance.

**Gotcha** Iceberg's column IDs make *rename* safe and *widening* safe, but
narrowing (DOUBLE → DECIMAL) is not — it can lose data. Know the safe set by
heart before you're asked to do it under pressure.

---

### H11 — Row-level deletion / GDPR erasure

**Goal** Delete one customer from a table format built on immutable files.

**Do**
1. `DELETE FROM silver.events WHERE customer_id = 42`.
2. Inspect `$files` and `$delete_files` — the data is still physically present in
   old files and old snapshots.
3. Prove it: time-travel to the prior snapshot and read the "deleted" rows.
4. Actually erase: rewrite affected files (`optimize`), `expire_snapshots`,
   `remove_orphan_files`. Verify the bytes are gone from MinIO.
5. Compare copy-on-write vs merge-on-read cost for this delete.
6. Repeat across bronze, silver, gold — plus Kafka retention, checkpoints, and
   quarantine tables.

**Done when** The customer's data is unrecoverable from every layer, and you can
demonstrate it from MinIO, not just from SQL.

**Gotcha** `DELETE` is a logical operation. A compliance delete is
`DELETE` + rewrite + expire + orphan-sweep across **every** layer including
checkpoints and DLQs. Most teams miss bronze and the quarantine table.

---

### H12 — Lineage and data contracts

**Goal** Answer "what breaks if I change this?" before changing it.

**Do**
1. Write a contract per table: schema, grain, freshness SLA, quality rules,
   owner — as YAML next to the DAG.
2. Enforce it in CI-style: a task that validates the live table against its
   contract and fails on violation.
3. Build a lineage map (Airflow assets + table-level dependencies) and render it.
4. Simulate a proposed change and produce an impact list from the map.

**Done when** Changing a silver column produces an automated list of affected
gold tables and consumers.

**Gotcha** Lineage that isn't derived from the code goes stale in two weeks.
Derive it from Airflow assets or from parsing the SQL — never maintain it by
hand in a wiki.

---

### H13 — Trino query performance investigation

**Goal** Systematic query debugging, not guesswork.

**Do**
1. Write a deliberately bad query: no partition filter, `SELECT *`, join order
   reversed, a `DISTINCT` that should be a `GROUP BY`.
2. `EXPLAIN ANALYZE`; identify the stage consuming the time. Use the :8085 UI's
   stage view and per-operator timings.
3. Fix one thing at a time; record the improvement for each.
4. Compare the final query against Spark's plan for the same logic.

**Done when** You have a table of ~5 changes with measured before/after times and
you can explain why each helped.

**Gotcha** Read the plan **bottom-up** and look for the operator where estimated
rows diverge from actual. That divergence points at the missing statistic or the
defeated pruning — and fixing that beats every other tweak.

---

### H14 — Point-in-time recovery after a bad merge

**Goal** Recover when the bad data has already propagated.

**Do**
1. At T0 note snapshots of silver and gold. Run a merge with a wrong join
   condition that duplicates 30% of rows.
2. The nightly gold rebuild then propagates it. The next maintenance run expires
   snapshots.
3. Recover: how far back can you go? What if the needed snapshot is expired?
4. Rebuild the unrecoverable window from bronze / from Kafka.
5. Write the incident report: detection, blast radius, recovery, prevention.

**Done when** Both tables are correct and the report names the check that would
have caught it in minutes instead of a day.

**Gotcha** Your recovery window is `min(snapshot retention, Kafka retention,
bronze retention)`. Most teams discover this number during the incident. Compute
it today and write it in the README.

---

### H15 — Pipeline observability

**Goal** Know it's broken before the business tells you.

**Do**
1. Emit per-run metrics to a metrics table: rows in/out, duration, quality
   failures, freshness lag, file count, table bytes.
2. Build an Airflow DAG that checks SLAs: gold fresher than 6 h, row count
   within ±30% of the 7-day average, zero quality errors.
3. Add anomaly detection on volume (z-score vs the trailing 7 days).
4. Alert on `on_failure_callback` and on SLA miss, with a message that names
   table, check, expected, actual.
5. Break each thing deliberately and verify the right alert fires — and that
   nothing else does.

**Done when** Each of three different injected failures produces exactly one
correct, actionable alert.

**Gotcha** Alerting on task failure only catches crashes. The expensive incidents
are the ones where the job succeeds and produces wrong data — volume anomaly and
freshness catch those, and nothing else will.

---

# Coverage map — scenario ↔ real daily work

| What you actually do at work | Scenarios |
|---|---|
| Ingest files / APIs into the lake | E1, E2, E3, E18 |
| Clean, type, and standardise | E5, E6, E19 |
| Deduplicate & guarantee exactly-once | E4, M2, M3, H1 |
| Incremental loading | E20, M11, M19 |
| Dimensional modelling / SCD | M1, M15 |
| CDC from an operational DB | M2, H5 |
| Streaming ingestion | E15, E16, M3, M17, M4 |
| Partitioning & layout decisions | E7, M8, M20, H7 |
| Table maintenance & small files | E10, M7, M9 |
| Schema changes | E12, M18, H10 |
| Orchestration & scheduling | E13, M12, M16 |
| Backfills & restatements | E14, M10, M11, H8 |
| Query performance tuning | M5, M6, M14, M20, H13 |
| Cluster / memory debugging | H2, H6 |
| Data quality & contracts | E18, E19, M16, H12 |
| Incident response & recovery | E11, H9, H11, H14 |
| Monitoring & alerting | H15 |
| Config & environment hygiene | M21 |

## A workable order

Don't go strictly top to bottom — build one pipeline and deepen it.

| Week | Focus |
|---|---|
| 1 | E1–E10 — get data in, understand the table format |
| 2 | E11–E20 — operations, orchestration, quality |
| 3 | M1–M4, M17 — merge patterns and streaming correctness |
| 4 | M5–M9, M14, M20 — performance and maintenance |
| 5 | M10–M13, M16, M18, M21 — orchestration and change management |
| 6+ | H1–H15 — one per sitting; write the runbook every time |

## Scenarios this stack can't cover

Worth knowing what's missing before an interview claims otherwise:

- **REST catalog** (Nessie / Polaris / Unity) — you're on Hive Metastore, so
  catalog-level branching and multi-table transactions are out of reach (H9
  makes the case for why you'd want one).
- **Real Debezium** — H5 hand-rolls the envelope; the connector's snapshot
  handover and schema-change topics are their own subject.
- **True scale** — 4 cores and 4 GB can demonstrate every *pattern* here, but not
  TB-scale shuffle behaviour or cloud-storage throttling.
- **dbt** — the transformation-as-SQL + testing + docs layer sits naturally on
  top of Trino here, and is the most common real-world addition to this shape of
  stack.
- **Multi-tenancy, RBAC, column masking** — single-user playground by design.
