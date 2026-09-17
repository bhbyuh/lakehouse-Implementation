# Lakehouse Playground — Architecture & Configuration Review

**Reviewed:** 2026-09-15 · **Branch:** `main` @ `46730d5` · **Reviewer:** static code/config review

**Scope:** `docker-compose.yaml`, `Dockerfile`, `start.sh`, `requirements.txt`, `hive/`, `hive_custom_conf/`,
`trino/etc/`, `code/*.ipynb`, `Airfllow/`.

**Method & limits:** This is a **static review**. The stack was **not running** at review time
(`docker ps` showed only unrelated containers), so nothing here is "verified live" — findings come from
reading the configs against the documented behaviour of each product. Anything marked **[VERIFY]** is a
claim you should confirm by running the one-liner given, because it depends on what is actually baked
into the images you pull.

---

## 1. Executive verdict

**Short answer to your question:** the wiring is **a working prototype held together by runtime plugins and
hardcoded strings, not a scalable engineering setup.** It is genuinely better than most tutorials — you
picked the right *components* (Iceberg + HMS + Trino + MinIO + Spark + Kafka is exactly the shape of a real
open lakehouse), and a few of your choices are properly modern (Trino's `fs.native-s3`, KRaft-mode Kafka,
Iceberg `SparkSessionCatalog`). But the **connection layer** — how the pieces find, authenticate to, and
version-match each other — is done the "make it work in the notebook" way, not the way a data platform team
would build it.

The single biggest structural problem: **connection configuration is duplicated in five places with no single
source of truth**, and **dependency versions are resolved at runtime from the public internet** instead of
being baked into images.

### Scorecard

| Area | Current state | Verdict |
|---|---|---|
| Component choice (Iceberg/HMS/Trino/MinIO/Kafka/Spark) | Right stack | 🟢 Solid |
| Trino catalog config | Modern native S3, correct HMS wiring | 🟢 Mostly good |
| Hive Metastore setup | Works, but creds on the JVM command line, no schema-init strategy | 🟡 Fragile |
| Spark ↔ Iceberg catalog design | Uses `SparkSessionCatalog` (a migration aid) instead of a named catalog | 🟡 Works, wrong pattern |
| Spark ↔ S3/MinIO wiring | Notebook-local, duplicated, cargo-cult settings | 🔴 Not scalable |
| Dependency management (`spark.jars.packages`) | Resolved from Maven at every session start | 🔴 Not scalable |
| Image pinning | 5 of 9 images on `:latest` | 🔴 Not reproducible |
| Startup ordering / healthchecks | Zero healthchecks in the main compose | 🔴 Race-prone |
| Secrets | Real secrets committed to git; creds hardcoded in 4 files | 🔴 Must fix |
| Streaming design (Kafka → Iceberg) | Single-hop, no schema contract, no compaction | 🟡 Known gap (you listed it) |
| Orchestration (Airflow) | Stack exists but is disconnected — no DAGs, no providers, port clash | 🔴 Not wired in |
| Table maintenance (compaction/expiry) | Absent | 🔴 Will degrade |
| Repo hygiene (`.gitignore`, CI, tests) | Absent | 🔴 |

---

## 2. What you have today

### 2.1 The actual topology

```
                 docker network: lakehouse-network (external)
 ┌──────────────────────────────────────────────────────────────────────┐
 │                                                                      │
 │  jupyter-local ──driver──► spark-master ──► spark-worker-a/-b        │
 │  (:8888)                   (:7077/:8080)    (:8081/:8082)            │
 │       │                                                              │
 │       ├── thrift://hive-metastore:9083 ──► hive-metastore (Hive 4.0) │
 │       │                                        │ JDBC                │
 │       │                                        ▼                     │
 │       │                                   postgres:15 (metastore DB) │
 │       │                                                              │
 │       ├── s3a://buck1/... ───────────────► minio (:9000/:9001)       │
 │       │                                        ▲                     │
 │       └── kafka://broker:9092 ──► broker (KRaft) ──► kafka-ui (:7070)│
 │                                                │                     │
 │  trino (:8085) ── iceberg connector ───────────┴─► HMS + MinIO       │
 │                                                                      │
 └──────────────────────────────────────────────────────────────────────┘

  SEPARATE compose project (Airfllow/):
  airflow apiserver/scheduler/worker/triggerer/dag-processor + its own postgres + redis
  → joined only by the shared external network. No DAGs. Nothing calls Spark or Trino.
```

### 2.2 How each connection is currently made

| Connection | Configured where | How |
|---|---|---|
| Spark → Hive Metastore | `code/lakehouse.ipynb` cell 1 | `spark.sql.catalog.spark_catalog.uri` + `spark.hadoop.hive.metastore.uris` hardcoded |
| Spark → MinIO | `code/lakehouse.ipynb` cell 1, `code/minio.ipynb` cell 1 | `spark.hadoop.fs.s3a.*` with literal `admin`/`12345678` |
| Spark → Kafka | `code/lakehouse.ipynb` cell 17 | `kafka.bootstrap.servers` literal |
| Spark → Iceberg/S3 jars | `code/lakehouse.ipynb` cell 1 | `spark.jars.packages` — Maven download at session start |
| Hive Metastore → Postgres | `docker-compose.yaml` `SERVICE_OPTS` | `-Djavax.jdo.option.ConnectionPassword=hive` on the JVM command line |
| Hive Metastore → MinIO | `hive_custom_conf/hive-site.xml` | `fs.s3a.*` with literal creds |
| Trino → HMS + MinIO | `trino/etc/catalog/iceberg.properties` | literal creds |
| Airflow → anything | *nowhere* | not wired |

**That is the core finding: the same MinIO credential pair is written out by hand in four files, and the
same metastore URI in three.** Change the password and you edit four files, rebuild one image, and restart
a notebook kernel. That is the definition of "temporary plugin type" rather than a platform.

---

## 3. Findings

Ordered by severity. Each has *what it is now* → *why it bites* → *what the standard is*.

### 🔴 F-01 — Real secrets are committed to git

`Airfllow/config/airflow.cfg` is a full generated default config, tracked in git, containing live secret material:

```
Airfllow/config/airflow.cfg:169   fernet_key = EwmsQiXec1NGxWj_kCQOrLNHkztl6y7m68wjJWQy-RI=
Airfllow/config/airflow.cfg:1329  secret_key = 5YYXrqZwsHfce+/TS8m6kw==
Airfllow/config/airflow.cfg:1707  jwt_secret = kpyxclnR7Ql14mHeHJichg==
```

Plus MinIO creds in `hive_custom_conf/hive-site.xml`, `trino/etc/catalog/iceberg.properties`,
`docker-compose.yaml:150-151`, and both notebooks.

**Why it bites:** the repo is (per the README) pushed to `github.com/bhbyuh/lakehouse-Implementation`.
A Fernet key is the thing that decrypts every Airflow Connection password you will ever store. Even in a
playground, this is the habit that gets people fired later. It also means you *cannot* demo this repo to an
employer without them noticing.

**Standard:** secrets never enter the repo. Local dev uses a git-ignored `.env`; the compose file references
`${VAR}`; real environments use Docker/K8s secrets, AWS Secrets Manager, or Vault. `airflow.cfg` should not be
committed at all — configure Airflow via `AIRFLOW__SECTION__KEY` env vars only.

**Action:** rotate all three values, `git rm --cached Airfllow/config/airflow.cfg`, add a `.gitignore`.
Note that rotating does not remove them from history — for a playground, rewriting history or starting a
fresh repo is acceptable; just don't leave the live values reachable.

---

### 🔴 F-02 — `airflow.cfg` silently fights the compose env vars, and the Fernet key resolves to empty

Three settings are declared in *both* places with different values:

| Setting | `airflow.cfg` | compose env | Winner |
|---|---|---|---|
| `executor` | `LocalExecutor` (L51) | `CeleryExecutor` | env |
| `auth_manager` | `SimpleAuthManager` (L57) | `FabAuthManager` | env |
| `load_examples` | `True` (L147) | `'false'` | env |
| `fernet_key` | a real key (L169) | `${FERNET_KEY}` | env |

Env vars win in Airflow, so the file is mostly dead weight — but confusing dead weight: anyone reading
`airflow.cfg` will conclude you're on LocalExecutor when you're on Celery.

The Fernet row is the dangerous one. `Airfllow/.env` is **0 bytes**, so `${FERNET_KEY}` expands to an empty
string, so `AIRFLOW__CORE__FERNET_KEY=""` is set in the container and **overrides the good key in the file**.
Airflow treats an empty Fernet key as "no encryption" and stores Connection passwords in the metadata DB in
plaintext (with a warning in the logs).

**Standard:** pick one configuration mechanism. For containerised Airflow that is env vars, full stop.
Generate the key into a git-ignored `.env`:

```bash
python -c "from cryptography.fernet import Fernet; print('FERNET_KEY='+Fernet.generate_key().decode())" >> Airfllow/.env
```

---

### 🔴 F-03 — Host port 8080 is claimed by two services

`docker-compose.yaml:13` maps Spark Master UI to `8080:8080`.
`Airfllow/docker-compose.yaml:128` maps the Airflow API server to `8080:8080`.

Both compose projects share the `lakehouse-network`, so they are clearly meant to run together — but the
second one up will fail to bind. The README's port table doesn't mention Airflow at all.

**Standard:** one port map per project, documented in a single table. Suggested: Spark `8080`,
Airflow `8090`, Trino `8085`, Kafka UI `7070`, MinIO `9000/9001`, Jupyter `8888`. Better still, collapse the
two compose files into one project with `profiles:` (`docker compose --profile airflow up`) or Compose's
`include:` directive, so port allocation is resolved in one place and `depends_on` can cross stacks.

---

### 🔴 F-04 — Runtime dependency resolution via `spark.jars.packages`

`code/lakehouse.ipynb` cell 1:

```python
.config("spark.jars.packages", ",".join([
    "org.apache.hadoop:hadoop-aws:3.5.0",
    "software.amazon.awssdk:bundle:2.35.4",
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
]))
```

This is the single clearest "prototype, not platform" signal in the repo. Every consequence below is real:

1. **Every kernel restart re-resolves from Maven Central.** There is no Ivy cache volume on `jupyter-local`,
   so the cache dies with the container. That's ~200 MB of downloads and 30–90 s before *any* code runs.
2. **No internet → no Spark session.** Offline, on a plane, behind a corporate proxy, in CI: dead.
3. **It is silently non-deterministic.** Transitive dependency resolution can pull different patch versions
   on different days. Production Spark clusters are never allowed to do this.
4. **The workers don't have the jars either.** `spark-worker-a/-b` run the bare `apache/spark:4.1.0` image.
   They receive the driver-resolved jars over the driver's file server, which works for ordinary UDF code
   but is a well-known source of `ClassNotFoundException` / `NoClassDefFoundError` for *FileSystem
   implementations* like S3A, which get loaded via `ServiceLoader` early in executor startup.

**Standard:** jars are baked into the image at build time, and the Spark image is shared by driver, master
and workers. In production this is an internal registry image or a `--packages`-free `spark-submit` against
a pre-built `--jars` bundle; the Spark Operator on K8s works the same way.

```dockerfile
# spark/Dockerfile  — ONE image used by master, workers and jupyter
FROM apache/spark:4.1.0
USER root
ARG ICEBERG=1.11.0
ARG HADOOP=3.4.1        # must match the image's own hadoop-client jars
ARG AWS_SDK=2.29.52
RUN set -eux; cd /opt/spark/jars; \
    curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG}/iceberg-spark-runtime-4.1_2.13-${ICEBERG}.jar; \
    curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar; \
    curl -fLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP}/hadoop-aws-${HADOOP}.jar; \
    curl -fLO https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK}/bundle-${AWS_SDK}.jar
USER spark
```

Then `spark-master`, `spark-worker-*` and `jupyter-local` all use `build: ./spark`, and the notebook's
`spark.jars.packages` line disappears entirely.

---

### 🔴 F-05 — Version mismatches that will surface as `NoSuchMethodError`

Three suspicious pins in the same block:

| Package | Pinned | Problem |
|---|---|---|
| `spark-sql-kafka-0-10_2.13` | **4.1.1** | Spark image is **4.1.0**. The Kafka connector must match the Spark version *exactly* — it shares internal `sql.execution` APIs that are not stable across patch releases. |
| `hadoop-aws` | **3.5.0** | Must match the `hadoop-client-*` jars bundled in the Spark image, exactly. Spark 4.x ships Hadoop 3.4.x. A mismatched `hadoop-aws` against `hadoop-common` is *the* classic `NoSuchMethodError` in S3A. |
| AWS SDK | `software.amazon.awssdk:bundle` (v2) here, but `com.amazonaws:aws-java-sdk-bundle:1.12.262` (v1) in `code/minio.ipynb` | Two different SDK majors for the same MinIO endpoint across two notebooks. Whichever `hadoop-aws` you land on supports exactly one of them. |

**[VERIFY]** — run this before pinning anything:

```bash
docker run --rm apache/spark:4.1.0 ls /opt/spark/jars | grep -E 'hadoop-client|hadoop-common|scala-library'
```

Whatever Hadoop version that prints is the *only* `hadoop-aws` version you may use. Same rule for Scala 2.13
vs 2.12 suffixes.

**Standard:** every jar version is derived from the runtime, pinned in one place (a Dockerfile `ARG` block or
a `versions.env`), and never `latest`. Teams keep a compatibility matrix in the repo.

> **Sidestep option:** your Hive metastore already carries `hadoop-aws` + AWS SDK v1 (see `hive/Dockerfile`),
> and that pairing (Hive 4.0.0 bundles Hadoop 3.3.6, you fetched `hadoop-aws:3.3.6`) is internally consistent
> — that part is correctly done. For Spark, you can drop the S3A dependency chain almost entirely by using
> Iceberg's own `S3FileIO` instead of `s3a://` (see F-09).

---

### 🔴 F-06 — Five images on `:latest`

```
docker-compose.yaml:62   apache/kafka:latest
docker-compose.yaml:85   provectuslabs/kafka-ui:latest
docker-compose.yaml:148  minio/minio           (no tag = :latest)
docker-compose.yaml:162  minio/mc              (no tag = :latest)
docker-compose.yaml:175  trinodb/trino:latest
```

**Why it bites:** `docker compose pull` six months from now gives you a different lakehouse. Trino ships a
major version roughly monthly and regularly removes deprecated config properties — a `latest` Trino will one
day refuse to start against your `iceberg.properties`. MinIO in particular has shipped breaking console/API
changes under `latest`.

**Standard:** pin every image to an immutable tag, and in regulated environments to a digest
(`trinodb/trino:476@sha256:...`). Renovate/Dependabot bumps them as PRs.

---

### 🔴 F-07 — No healthchecks and no dependency conditions in the main compose

Not one service in `docker-compose.yaml` has a `healthcheck:`. Every `depends_on` is the short-form list
syntax, which only waits for the container to be **created**, not **ready**.

Concretely, this is what breaks on a cold `docker compose up`:

- `hive-metastore` (`:127 depends_on: [postgres]`) starts its schema init while Postgres is still running
  initdb → metastore exits, and because there is no `restart:` policy it stays down.
- `trino` (`:181 depends_on: [minio, hive-metastore]`) starts, fails to reach HMS, and marks the `iceberg`
  catalog unavailable for the life of the process.
- `createbuckets` is the *only* service that handles this, and it does so with a hand-rolled
  `until … do sleep 2; done` loop (`:166`). That's the right instinct, implemented in the wrong layer.

Ironically, your **Airflow** compose does this correctly — `condition: service_healthy` on redis/postgres and
`condition: service_completed_successfully` on `airflow-init`. That file (which came from Apache) is the model.

**Standard:** every stateful service declares a healthcheck; every consumer declares
`depends_on: {svc: {condition: service_healthy}}`.

```yaml
  postgres:
    image: postgres:15
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hive -d metastore"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 10s

  minio:
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      retries: 12

  hive-metastore:
    depends_on:
      postgres: {condition: service_healthy}
    healthcheck:
      test: ["CMD-SHELL", "nc -z localhost 9083 || exit 1"]
      interval: 10s
      retries: 10
      start_period: 40s

  trino:
    depends_on:
      hive-metastore: {condition: service_healthy}
      createbuckets:  {condition: service_completed_successfully}
    healthcheck:
      test: ["CMD-SHELL", "curl -fs http://localhost:8080/v1/info | grep -q '\"starting\":false'"]
      interval: 10s
      retries: 20
```

---

### 🟡 F-08 — Hive Metastore: credentials on the command line, and no schema-init strategy

```yaml
docker-compose.yaml:118-122
    SERVICE_OPTS: >
      -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
      -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore
      -Djavax.jdo.option.ConnectionUserName=hive
      -Djavax.jdo.option.ConnectionPassword=hive
```

Two issues:

1. **The DB password is a JVM argument**, so it is visible to `docker inspect`, `ps aux` inside the
   container, and any crash dump. You already mount `hive_custom_conf/hive-site.xml` — the JDO properties
   belong *there*, alongside the `fs.s3a.*` ones, so there is one metastore config file instead of two
   half-configs.
2. **[VERIFY] Schema init on restart.** The `apache/hive` image entrypoint runs `schematool -initSchema`
   when `DB_DRIVER` is set, and the image documents `IS_RESUME=true` for reusing an already-initialised
   database. Your compose sets `DB_DRIVER` but never `IS_RESUME`. Confirm the behaviour on a second start:

   ```bash
   docker compose up -d postgres hive-metastore
   docker compose restart hive-metastore && docker compose logs hive-metastore | grep -i -E 'schema|already'
   ```

   If it errors on the second start, the standard pattern is a one-shot init service:

   ```yaml
     hive-metastore-init:
       build: ./hive
       environment: {SERVICE_NAME: schematool, DB_DRIVER: postgres, SERVICE_OPTS: *hive-jdbc}
       command: ["-initSchema", "--dbType", "postgres"]
       depends_on: {postgres: {condition: service_healthy}}
       restart: "no"
     hive-metastore:
       environment: {IS_RESUME: "true"}
       depends_on: {hive-metastore-init: {condition: service_completed_successfully}}
   ```

Also missing from `hive-site.xml`, and commonly needed:

```xml
<property><name>metastore.thrift.port</name><value>9083</value></property>
<property><name>hive.metastore.event.db.notification.api.auth</name><value>false</value></property>
<property><name>metastore.storage.schema.reader.impl</name>
         <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value></property>
```

The third one is what lets HMS return schemas for tables it can't deserialize itself (i.e. Iceberg tables
written by Spark) — worth adding before you hit it.

**Good news:** `hive/Dockerfile` itself is well done — pinned versions, correct jar set for Hive 4.0.0
(Hadoop 3.3.6 + AWS SDK v1 + Postgres JDBC), drops back to `USER hive` at the end. That is exactly right.

---

### 🟡 F-09 — Spark's Iceberg catalog uses the migration-aid pattern, not the production pattern

`code/lakehouse.ipynb` cell 1:

```python
.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
.config("spark.sql.catalog.spark_catalog.type", "hive")
.config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083")
```

`SparkSessionCatalog` exists for **migration**: it wraps Spark's built-in Hive catalog so that Iceberg and
non-Iceberg tables can coexist in the same namespace. That's why cell 5 could create `USING PARQUET` tables
and cell 9 `USING ICEBERG` tables side by side — which was a good experiment.

But it has real costs as a permanent choice:

- **No `warehouse` is set on the catalog.** You set `spark.sql.warehouse.dir` instead, which
  `SparkSessionCatalog` will honour via the session catalog — but Iceberg's own catalog `warehouse`
  property is unset, so table-location behaviour depends on which code path creates the table. This is why
  you should always set `spark.sql.catalog.<name>.warehouse` explicitly.
- **Namespace ambiguity.** `demo.events` could be a Hive table or an Iceberg table; you have to
  `DESCRIBE FORMATTED` to know (which is exactly what cells 7 and 10 do).
- **Nothing is portable.** Moving from HMS to an Iceberg REST catalog later means rewriting every table
  reference, because they're all bound to `spark_catalog`.

**Standard:** a **named** Iceberg catalog, with the warehouse and FileIO declared, set as the default:

```python
CATALOG = "lakehouse"
spark = (SparkSession.builder
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
  .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
  .config(f"spark.sql.catalog.{CATALOG}.uri", "thrift://hive-metastore:9083")
  .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://buck1/warehouse")
  # Iceberg's own S3 client — no hadoop-aws, no s3a, no SDK-version roulette
  .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
  .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", os.environ["S3_ENDPOINT"])
  .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
  .config("spark.sql.defaultCatalog", CATALOG)
  .getOrCreate())
```

Tables then live at `lakehouse.bronze.events`, and swapping HMS for a REST catalog later is a two-line change
(`type=rest`, `uri=http://...`) with **zero** changes to your SQL.

Note `S3FileIO` reads credentials from the standard AWS chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
env vars), which is precisely how you get credentials *out* of your source files.

---

### 🟡 F-10 — Spark ↔ MinIO config is duplicated, cargo-culted, and inconsistent

Three separate problems in the S3 wiring.

**(a) Hardcoded credentials in notebooks, committed to git.** `lakehouse.ipynb` cell 1 and `minio.ipynb`
cell 1 both embed `admin` / `12345678`.

**(b) A cargo-cult setting.** Both notebooks and `hive-site.xml` set:

```python
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

This has been unnecessary since Hadoop 2.8 — `S3AFileSystem` is registered through
`META-INF/services` in `hadoop-aws`. If you ever *need* this line, the real problem is that `hadoop-aws`
isn't on the classpath, and the line will not fix it. It's the clearest tell of config copied from a blog
post rather than the Hadoop docs.

**(c) An inconsistency that will bite.** `hive_custom_conf/hive-site.xml` sets
`fs.s3a.connection.ssl.enabled=false`; the notebooks do not. Your endpoint is `http://minio:9000`, so
whether Spark talks plaintext depends on which SDK version resolved in F-05 and whether it honours the
endpoint scheme. Two components, two different answers to the same question.

**(d) `minio.ipynb` sets Hadoop config *after* the session exists:**

```python
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "admin")
```

This mutates the default Hadoop conf, but `FileSystem` instances are **cached per (scheme, authority, user)**.
Any `s3a://` access that already happened in the JVM keeps the old config forever. It works in a fresh kernel
and mysteriously doesn't after you re-run cells out of order.

**Also:** `minio.ipynb` calls `SparkSession.builder.config(...).getOrCreate()` with **no master**. If
`lakehouse.ipynb`'s session is alive in the same JVM, `getOrCreate()` returns *that* session and **silently
discards every `.config()` you just wrote**. If it isn't, you get a `local[*]` session, not the cluster. Either
way the notebook does not do what it reads like it does. Cell 3 also calls `df.write` on a `df` that is never
defined (cell 2 is commented out).

**Standard:** one shared session factory, zero literals:

```python
# code/lakehouse/session.py  — imported by every notebook and every Airflow task
import os
from pyspark.sql import SparkSession

def get_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
        .master(os.environ["SPARK_MASTER_URL"])
        .config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
        ... # catalog config from F-09, all values from os.environ
        .getOrCreate())
```

with the values coming from compose:

```yaml
  jupyter-local:
    hostname: jupyter-local
    environment:
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_DRIVER_HOST: jupyter-local
      HIVE_METASTORE_URI: thrift://hive-metastore:9083
      KAFKA_BOOTSTRAP: broker:9092
      S3_ENDPOINT: http://minio:9000
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
```

and `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` living in a git-ignored `.env` that *also* feeds the `minio`
service — one definition, five consumers.

---

### 🟡 F-11 — The Spark driver's network identity is unpinned

`jupyter-local` has no `hostname:` (contrast `spark-master`, `spark-worker-a`, `spark-worker-b`, which all
set one), and the notebook sets neither `spark.driver.host` nor `spark.driver.bindAddress`.

In Spark standalone **client mode**, which is what `.master("spark://spark-master:7077")` from a notebook
gives you, the executors must connect *back* to the driver. The driver advertises whatever
`InetAddress.getLocalHost()` returns — the container's hostname, which Docker defaults to the short container
ID. Docker's embedded DNS does register that ID on user-defined networks, so **this currently works by
accident**. It stops working the moment you use host networking, a second network, `network_mode`, or run the
driver outside Docker.

**Standard:** always pin the driver identity explicitly.

```yaml
  jupyter-local:
    hostname: jupyter-local
    ports: ["8888:8888", "4040:4040"]
```
```python
.config("spark.driver.host", "jupyter-local")
.config("spark.driver.bindAddress", "0.0.0.0")
.config("spark.driver.port", "7078")
.config("spark.blockManager.port", "7079")
```

Pinning the ports also means you *can* expose them, which you'll need the day the driver isn't in Docker.

---

### 🟡 F-12 — Trino: good bones, three gaps

`trino/etc/catalog/iceberg.properties` is the **best-written config in the repo**. Using
`fs.native-s3.enabled=true` rather than the deprecated `hive.s3.*` properties is the current recommendation,
and the HMS wiring is correct.

Gaps:

**(a) No Hive catalog.** Your `demo.customers` / `demo.orders` tables (cell 5, `USING PARQUET`) are plain
Hive tables. The Iceberg connector **cannot read them** — it will error with "not an Iceberg table". You need
a second catalog file to query the non-Iceberg half of your own warehouse:

```properties
# trino/etc/catalog/hive.properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
hive.non-managed-table-writes-enabled=true
```

**(b) `jvm.config` is missing flags Trino's reference config treats as required.**

```
trino/etc/jvm.config
-server
-Xmx2G
-XX:+UseG1GC
-XX:+UseCompressedOops            # default on <32G heaps; redundant
-XX:+UseCompressedClassPointers   # default; redundant
```

The two `UseCompressed*` flags are JVM defaults at this heap size — harmless, but they signal copied config.
What's *missing* matters more. Trino's documented reference `jvm.config` includes:

```
-XX:+ExitOnOutOfMemoryError        # else Trino limps along in a broken state after OOM
-XX:-OmitStackTraceInFastThrow     # else repeated NPEs lose their stack traces — brutal to debug
-Djdk.attach.allowAttachSelf=true  # required by Trino's memory accounting (JOL self-attach)
-XX:G1HeapRegionSize=32M
-XX:+HeapDumpOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-Dfile.encoding=UTF-8
```

`-Djdk.attach.allowAttachSelf=true` is the one to add today — without it certain queries fail at runtime with
an attach error rather than at startup, which is a confusing failure mode.

**(c) `node.data-dir=/data/trino` is not a volume.** Trino writes spill files, its own logs and plugin
state there; all of it is lost on `docker compose down`, and a spilling query fills the container's writable
layer. Add `- trino_data:/data/trino`.

**Also worth adding** as you grow past one node: `config.properties` currently has
`node-scheduler.include-coordinator=true`, which is correct for a single node but must become `false` the
moment you add a worker — coordinators that also execute splits become the bottleneck. And
`query.max-memory` / `query.max-memory-per-node` are unset, so you're on defaults tuned for a much larger
heap than `-Xmx2G`.

---

### 🟡 F-13 — The streaming pipeline is one hop with no contract, no dedup, and no maintenance

`code/lakehouse.ipynb` cells 17–20:

```python
.option("checkpointLocation", "s3a://buck1/checkpoints/events-v1")
.trigger(processingTime="10 seconds")
.toTable("spark_catalog.demo.events")
```

You already diagnosed the `startingOffsets` trap in `errors.txt` and wrote up root cause + verification —
that's a genuinely professional debugging note, and the "should work on" list in that file is the right
backlog. Adding to it, the things a platform team would treat as non-negotiable:

1. **Checkpointing to object storage.** Spark's docs warn against this: the checkpoint protocol relies on
   atomic rename, which S3-family stores don't provide natively. MinIO's strong consistency makes it *mostly*
   fine at your scale, but the correct pattern is a checkpoint on a POSIX volume (or HDFS), or S3A with the
   committer configured. At minimum, know that this is a known-risky choice, not a default.
2. **A 10-second trigger with no compaction is a small-file bomb.** 10s micro-batches → 8,640 commits/day →
   8,640+ data files and 8,640 Iceberg snapshots per day. Query planning degrades, metadata grows without
   bound, and `expire_snapshots` eventually takes longer than the retention window. **There is no compaction
   or expiry job anywhere in this repo.** This is the #1 thing that separates a demo lakehouse from a real one.
3. **No schema contract.** `from_json` against a hand-written `StructType` means a producer-side schema change
   is discovered as silent `NULL`s in your Iceberg table, days later. And `from_json` on malformed JSON
   returns `NULL` rather than failing — you have no dead-letter path.
4. **No exactly-once story.** Spark + Iceberg gives you at-least-once on the write. `event_id` exists in your
   schema but is never used for deduplication.
5. **No watermark / late-data handling**, and **no partitioning** — `demo.events` is created implicitly by
   `toTable()` with no `PARTITIONED BY (days(event_time))`, so every query is a full scan.
6. **Cell 27** writes the raw Kafka stream back into Kafka with `checkpointLocation="/app/checkpoint/"` —
   a container-local path that vanishes on restart, guaranteeing reprocessing — and then blocks the notebook
   forever on `.awaitTermination()`.

**Standard — medallion + contract + maintenance:**

```
Kafka (Avro/Protobuf + Schema Registry)
  │
  ├─► BRONZE  lakehouse.bronze.events_raw
  │     append-only, partitioned by days(ingest_ts), schema = the contract,
  │     bad records → lakehouse.bronze.events_dlq
  │
  ├─► SILVER  lakehouse.silver.events
  │     deduped on event_id (MERGE INTO), typed, watermarked,
  │     partitioned by days(event_time)
  │
  └─► GOLD    lakehouse.gold.customer_daily_spend
        business aggregates, served to Trino/BI
```

Plus a scheduled maintenance job — this is what Airflow should actually be running:

```sql
CALL lakehouse.system.rewrite_data_files(
  table => 'silver.events',
  strategy => 'binpack',
  options => map('min-input-files','10','target-file-size-bytes','134217728'));
CALL lakehouse.system.rewrite_manifests(table => 'silver.events');
CALL lakehouse.system.expire_snapshots(
  table => 'silver.events', older_than => TIMESTAMP '...', retain_last => 10);
CALL lakehouse.system.remove_orphan_files(table => 'silver.events', older_than => TIMESTAMP '...');
```

Two practical fixes for the write itself, today:

```python
.option("fanout-enabled", "true")   # required when input isn't sorted by partition
```
and create the table explicitly with partitioning before starting the stream, rather than letting
`toTable()` infer an unpartitioned one.

---

### 🟡 F-14 — Airflow is installed but not connected to anything

- `Airfllow/dags/` **does not exist**. There are zero DAGs. Docker will create the directory root-owned on
  first `up`, which then causes permission errors when the worker (UID 50000) tries to write.
- `_PIP_ADDITIONAL_REQUIREMENTS` is empty and there is no custom image, so
  `apache-airflow-providers-apache-spark` and `apache-airflow-providers-trino` are **not installed**. There is
  currently no way for Airflow to talk to Spark or Trino.
- No Airflow **Connections** are provisioned for `spark_default`, `trino_default`, or MinIO — so even with
  providers installed, every DAG would need hardcoded hosts, repeating F-10's mistake in a fourth place.
- The directory is spelled **`Airfllow`** (three l's). Cosmetic, but it's in every path and every command you
  will ever type, and it looks careless in a portfolio repo.

**Standard:** a custom Airflow image with pinned providers, plus connections provisioned declaratively.

```dockerfile
# Airflow/Dockerfile
FROM apache/airflow:3.2.0
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```
```
# Airflow/requirements.txt
apache-airflow-providers-apache-spark==5.*
apache-airflow-providers-trino==6.*
apache-airflow-providers-amazon==9.*
```

Connections as env vars (URI form) so they're reproducible and never stored by hand in the UI:

```yaml
    AIRFLOW_CONN_SPARK_DEFAULT: 'spark://spark-master:7077'
    AIRFLOW_CONN_TRINO_DEFAULT: 'trino://trino@trino:8080/iceberg'
    AIRFLOW_CONN_MINIO_S3: 'aws://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@?endpoint_url=http%3A%2F%2Fminio%3A9000'
```

First DAG to write is the Iceberg maintenance one from F-13 — it's the highest-value, lowest-risk thing
Airflow can do for this stack, and it's the kind of DAG that appears in every real lakehouse.

---

### 🟢 F-15 — Smaller items

| # | File | Issue |
|---|---|---|
| a | `docker-compose.yaml:1` | `version: "3.8"` is obsolete under Compose v2 and emits a warning. Delete the line. |
| b | `docker-compose.yaml:195` | Volume `hive_data` is declared and never mounted anywhere. |
| c | `docker-compose.yaml:17,18` | `./data` is mounted into 4 services but **the directory doesn't exist** in the repo — Docker creates it root-owned. |
| d | `Dockerfile:2` | `as jupyter-local` names a build stage that nothing references — leftover from a multi-stage attempt. |
| e | `Dockerfile:3` | `USER root` is never dropped, so Jupyter runs as root (hence `--allow-root`). Files written to `/code` become root-owned on your Mac. |
| f | `Dockerfile:5` | `WORKDIR /app`, but the volumes mount `/code` and `/data` — Jupyter opens in an empty directory every time. |
| g | `start.sh` | Dead file: nothing references it, and it duplicates the compose `command:` with different flags. |
| h | `requirements.txt` | `pyspark==4.1.0` is installed **on top of** `apache/spark:4.1.0`, which already ships PySpark. Two copies of the same library; `PYTHONPATH` ordering decides which wins. Remove it. |
| i | repo root | **No `.gitignore`.** `.DS_Store` (×3) and `code/.ipynb_checkpoints/` are tracked in git. |
| j | `code/*.ipynb` | Notebooks are committed with outputs and execution counts — every run produces a diff. Use `nbstripout` or `jupytext`. |
| k | `README.md` | Documents an `Airflow/` and a `proj/` directory; neither exists (it's `Airfllow/`, and there's no `proj/`). The Trino examples query `hive.default.*`, but no `hive` catalog is configured (F-12a). |
| l | `errors.txt` | Good content, wrong format — this is a runbook/ADR. Move to `docs/runbook/` as Markdown. |

---

## 4. The architectural question you actually asked

> *"is it connection I have made according to market standards, or just like a temporary plugin type that cannot scale?"*

Here is the honest mapping, connection by connection.

| Connection | Your pattern | What a data platform team does | Gap |
|---|---|---|---|
| Spark → Iceberg jars | `spark.jars.packages` at runtime | Baked into a pinned image | **Large** |
| Spark → MinIO | Literals in notebook cells | Env vars → session factory → secret store | **Large** |
| Spark → HMS | Literal URI in notebook | Env var, one session factory | **Medium** |
| Spark → Kafka | Literal bootstrap in notebook | Env var + Schema Registry | **Medium** |
| Catalog abstraction | `spark_catalog` + `SparkSessionCatalog` | Named catalog, `defaultCatalog`, REST-ready | **Medium** |
| Trino → HMS/MinIO | Static properties file with literals | Same file, but `${ENV:VAR}` substitution | **Small** |
| HMS → Postgres | Password as JVM arg | `hive-site.xml` + secret | **Medium** |
| Service startup order | List-form `depends_on` + a `sleep` loop | Healthchecks + conditions | **Medium** |
| Orchestration | Not wired | Providers + Connections + DAGs | **Large** |

**The pattern behind every "Large":** configuration is written where it is *used* instead of where it is
*defined*. Every one of these collapses into the same fix — **define each fact exactly once** (in `.env`, in
compose `environment:`, in a shared `session.py`) and have every consumer read it from there.

Do that and the same code runs unchanged in a notebook, in an Airflow task, in CI, and against a real S3
bucket instead of MinIO. That portability *is* the market standard. Everything else in this document is
detail.

### Where the market is heading (worth knowing, not worth doing today)

Your stack is the 2023–2024 open lakehouse. Two things have moved since, and both are relevant to how you'd
answer this question in an interview:

1. **HMS → Iceberg REST Catalog.** Hive Metastore is the legacy option; new builds use a REST catalog
   (Apache Polaris, Project Nessie, Lakekeeper, Apache Gravitino, or Databricks Unity). You get
   catalog-level credential vending (engines never see storage keys — which dissolves F-10 entirely),
   multi-table transactions, and no Thrift/JDBC/Postgres to operate. **If you adopt the named-catalog pattern
   in F-09, switching later is a two-line config change.** That alone is a good reason to do F-09.
2. **SQL transformation framework.** Your `bronze → silver → gold` logic belongs in **dbt-trino** or
   **SQLMesh**, not in notebook cells — that's what gives you tests, lineage, docs, and CI on the
   transformation layer, and it's what's actually on the job descriptions.

Also absent and standard in real platforms: data quality gates (dbt tests / Great Expectations / Soda),
observability (Trino + Spark both export Prometheus metrics; Grafana dashboards are a half-day), and CI
(a GitHub Action that runs `docker compose up`, executes a smoke pipeline, and asserts row counts).

---

## 5. Target architecture

```
                         ┌────────────────────────────────────────┐
                         │  .env  (git-ignored)  ← SINGLE SOURCE  │
                         │  MINIO_ROOT_USER / _PASSWORD           │
                         │  FERNET_KEY, versions, ports           │
                         └───────────────┬────────────────────────┘
                                         │ ${VAR}
                         ┌───────────────▼────────────────────────┐
                         │  compose.yaml (one project, profiles)  │
                         │  pinned images · healthchecks ·        │
                         │  service_healthy conditions            │
                         └───────────────┬────────────────────────┘
                                         │ environment:
   ┌─────────────────────────────────────┼─────────────────────────────────┐
   │                                     │                                 │
┌──▼─────────────┐   ┌──────────────┐ ┌──▼──────────┐   ┌──────────────┐  │
│ spark image    │   │ Kafka +      │ │ Catalog     │   │ Airflow      │  │
│ (jars BAKED)   │   │ Schema Reg.  │ │ HMS → REST  │   │ +providers   │  │
│ master/workers │   │              │ │             │   │ +connections │  │
│ /jupyter share │   └──────┬───────┘ └──────┬──────┘   └──────┬───────┘  │
└──┬─────────────┘          │                │                 │          │
   │   code/lakehouse/session.py  ← ONE session factory, zero literals    │
   │                        │                │                 │          │
   │        ┌───────────────▼────────────────▼─────────────────▼───────┐  │
   │        │   BRONZE ──► SILVER ──► GOLD   (Iceberg on MinIO/S3)     │  │
   │        │   + DLQ    + MERGE dedup   + dbt/SQLMesh models          │  │
   │        └───────────────────────────┬──────────────────────────────┘  │
   │                                    │                                 │
   │     Airflow DAGs: maintenance (compaction · expire · orphans)        │
   │                   + quality gates + backfills                        │
   └────────────────────────────────────▼─────────────────────────────────┘
                                   Trino (iceberg + hive catalogs)
                                        │
                                    BI / dbt / clients
```

---

## 6. Roadmap

Ordered so each phase is independently shippable and nothing depends on a later phase.

### Phase 0 — Stop the bleeding (½ day)

- [ ] Rotate `fernet_key`, `secret_key`, `jwt_secret`; `git rm --cached Airfllow/config/airflow.cfg` **(F-01, F-02)**
- [ ] Add `.gitignore`: `.DS_Store`, `.env`, `.ipynb_checkpoints/`, `data/`, `logs/`, `*.log` **(F-15i)**
- [ ] `git rm --cached` the three `.DS_Store` files and `code/.ipynb_checkpoints/`
- [ ] Create `.env` with `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `FERNET_KEY`, image tags **(F-01)**
- [ ] Move Airflow off host port 8080 **(F-03)**
- [ ] Pin all five `:latest` images **(F-06)**

### Phase 1 — Make it reproducible (1–2 days)

- [ ] **[VERIFY]** the bundled Hadoop/Scala versions in `apache/spark:4.1.0` **(F-05)**
- [ ] Build one `spark/Dockerfile` with jars baked in; use it for master, workers and Jupyter **(F-04)**
- [ ] Delete `spark.jars.packages` from both notebooks **(F-04)**
- [ ] Fix the Kafka-connector version mismatch **(F-05)**
- [ ] Drop `pyspark` from `requirements.txt`; fix `WORKDIR`; run as non-root; delete `start.sh` **(F-15d–h)**
- [ ] Add healthchecks + `condition: service_healthy` everywhere **(F-07)**
- [ ] Resolve the HMS schema-init question; move JDBC config into `hive-site.xml` **(F-08)**
- [ ] Add `trino/etc/catalog/hive.properties`; fix `jvm.config`; add the `trino_data` volume **(F-12)**

### Phase 2 — Make the connections real (2–3 days)

- [ ] Write `code/lakehouse/session.py`; both notebooks import it **(F-10)**
- [ ] Switch to a named `lakehouse` catalog + `S3FileIO` + `defaultCatalog` **(F-09)**
- [ ] All endpoints and credentials via compose `environment:` from `.env` **(F-10)**
- [ ] Pin `spark.driver.host` / `bindAddress` / ports; give `jupyter-local` a `hostname:` **(F-11)**
- [ ] Delete `fs.s3a.impl` from all three config locations **(F-10b)**
- [ ] Rewrite `minio.ipynb` — it currently cannot work as written **(F-10d)**

### Phase 3 — Make the pipeline real (3–5 days)

- [ ] Split `demo.events` into `bronze` / `silver` / `gold` namespaces **(F-13)**
- [ ] Partition by `days(event_time)`; create tables explicitly, not via `toTable()` **(F-13)**
- [ ] Dedup silver on `event_id` with `MERGE INTO` **(F-13)**
- [ ] Add a DLQ path for `from_json` failures **(F-13)**
- [ ] Move checkpoints off `s3a://` to a POSIX volume **(F-13)**
- [ ] Add `fanout-enabled=true` to the streaming write **(F-13)**
- [ ] Add Schema Registry + Avro to the Kafka path **(F-13)**

### Phase 4 — Make it operable (3–5 days)

- [ ] Custom Airflow image with pinned providers **(F-14)**
- [ ] `AIRFLOW_CONN_*` env vars for Spark / Trino / MinIO **(F-14)**
- [ ] First DAG: daily Iceberg maintenance — `rewrite_data_files`, `rewrite_manifests`,
      `expire_snapshots`, `remove_orphan_files` **(F-13, F-14)**
- [ ] Merge the two compose projects into one with `profiles:` **(F-03)**
- [ ] Rename `Airfllow/` → `airflow/`; fix the README structure/port tables **(F-14, F-15k)**

### Phase 5 — Make it professional (ongoing)

- [ ] dbt-trino or SQLMesh for silver → gold
- [ ] Data quality tests as an Airflow gate
- [ ] Prometheus + Grafana for Trino and Spark
- [ ] GitHub Actions: `compose up` → smoke pipeline → assert row counts → `compose down`
- [ ] Convert `errors.txt` into `docs/runbook/*.md` and `docs/adr/*.md`
- [ ] Evaluate an Iceberg REST catalog (Polaris / Nessie / Lakekeeper) as the HMS replacement

---

## 7. What you already got right

Worth stating plainly, because most of this review is criticism and the component choices are genuinely good:

- **The stack itself is the right one.** Iceberg + HMS + Trino + MinIO + Spark + Kafka is what an open
  lakehouse actually looks like. Nothing here needs replacing — only the wiring between the pieces.
- **`trino/etc/catalog/iceberg.properties` uses `fs.native-s3.enabled`**, not the deprecated `hive.s3.*`
  properties. That's the current recommendation, and most tutorials still get it wrong.
- **Kafka is in KRaft mode** with an explicit internal/external listener split, and the inline comments show
  you understood *why* `EXTERNAL://localhost:9094` is needed for host-side Python clients. That specific
  thing trips up a lot of people.
- **`hive/Dockerfile` is correct**: pinned versions, the right jar set for Hive 4.0.0's bundled Hadoop, and
  it drops privileges at the end.
- **`createbuckets` bootstraps storage idempotently** (`mc mb --ignore-existing`) — right instinct, and the
  retry loop shows you'd already noticed the ordering problem that F-07 generalises.
- **You proved Iceberg was actually engaged** rather than assuming it — `DESCRIBE FORMATTED` in cells 7 and
  10, plus the comment "currently tables are plain Parquet + Hive Metastore, no Iceberg yet". That's real
  verification discipline.
- **`errors.txt` is written like an engineer's runbook**: title, problem, root cause, solution, *and* how it
  was verified. The `startingOffsets` + checkpoint-interaction diagnosis is a genuinely subtle bug, correctly
  explained. Keep writing these — just move them into `docs/`.

The gap between this repo and a production-grade platform is **entirely in the wiring layer**, and the
wiring layer is the cheapest part to fix.
