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
source of truth**.

### Scorecard

| Area | Current state | Verdict |
|---|---|---|
| Component choice (Iceberg/HMS/Trino/MinIO/Kafka/Spark) | Right stack | 🟢 Solid |
| Trino catalog config | Modern native S3, correct HMS wiring | 🟢 Mostly good |
| Hive Metastore setup | Works, but creds on the JVM command line, no schema-init strategy | 🟡 Fragile |
| Spark ↔ Iceberg catalog design | Uses `SparkSessionCatalog` (a migration aid) instead of a named catalog | 🟡 Works, wrong pattern |
| Spark ↔ S3/MinIO wiring | Notebook-local, duplicated, cargo-cult settings | 🔴 Not scalable |
| Secrets | Real secrets committed to git; creds hardcoded in 4 files | 🔴 Must fix |
| Streaming design (Kafka → Iceberg) | Single-hop, no schema contract, no compaction | 🟡 Known gap (you listed it) |
| Orchestration (Airflow) | Stack exists but is disconnected — no DAGs, no providers | 🔴 Not wired in |
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

### Phase 1 — Make it reproducible (1–2 days)

- [ ] Drop `pyspark` from `requirements.txt`; fix `WORKDIR`; run as non-root; delete `start.sh` **(F-15d–h)**
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
  retry loop shows you'd already noticed the ordering problem.
- **You proved Iceberg was actually engaged** rather than assuming it — `DESCRIBE FORMATTED` in cells 7 and
  10, plus the comment "currently tables are plain Parquet + Hive Metastore, no Iceberg yet". That's real
  verification discipline.
- **`errors.txt` is written like an engineer's runbook**: title, problem, root cause, solution, *and* how it
  was verified. The `startingOffsets` + checkpoint-interaction diagnosis is a genuinely subtle bug, correctly
  explained. Keep writing these — just move them into `docs/`.

The gap between this repo and a production-grade platform is **entirely in the wiring layer**, and the
wiring layer is the cheapest part to fix.
