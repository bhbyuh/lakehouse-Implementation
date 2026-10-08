# Lakehouse Implementation Playground

A self-contained **data lakehouse** that runs on your laptop with Docker. It wires
together Spark, Iceberg, Hive Metastore, Trino, Kafka, MinIO, Airflow and Superset
so you can practise ingestion, streaming, table maintenance, orchestration and BI
end to end.

- Practice tasks: [`SCENARIOS.md`](SCENARIOS.md) (Easy → Medium → Hard)
- Problems hit and how they were fixed: [`errors.txt`](errors.txt)

> Learning project. All credentials are development defaults. Not for production use.

---

## Architecture

```
                         ┌──────────────────────────┐
                         │      Jupyter Lab         │  :8888 (Spark app UI :4040)
                         │  notebooks in ./code     │
                         └────────────┬─────────────┘
                                      │ PySpark (spark://spark-master:7077)
                                      ▼
 ┌──────────────┐  stream   ┌──────────────────────┐
 │ Kafka broker │──────────▶│    Spark cluster     │  master :8080
 │ :9092 / 9094 │           │  master + 2 workers  │  workers :8081 / :8082
 └──────┬───────┘           └──────┬────────┬──────┘
        │                          │        │ table metadata (thrift :9083)
   Kafka UI :7070                  │        ▼
                                   │   ┌──────────────────┐     ┌──────────────┐
                                   │   │  Hive Metastore  │────▶│  PostgreSQL  │
                                   │   │ (Iceberg catalog)│     │ hive-postgres│
                                   │   └────────▲─────────┘     └──────────────┘
                                   │            │
                  Iceberg data +   │            │ same catalog
                  metadata files   ▼            │
                           ┌──────────────┐   ┌─┴──────────┐      ┌────────────┐
                           │    MinIO     │◀──│   Trino    │◀─────│  Superset  │ :8088
                           │ s3://buck1/  │   │   :8085    │      │ (+Postgres,│
                           │ :9000 / 9001 │   └─────▲──────┘      │   Redis)   │
                           └──────────────┘         │             └────────────┘
                                                    │ SQL
                                            ┌───────┴────────┐
                                            │    Airflow     │ :8090
                                            │ (Celery, own   │
                                            │ Postgres+Redis)│
                                            └────────────────┘
```

---

## Project structure

```
lakehouse-Implementation/
├── docker-compose.yaml   # Core stack: Spark, Jupyter, Kafka, Hive, Postgres, MinIO, Trino, Superset
├── spark/                # Image for Spark master/workers and Jupyter
├── hive/                 # Hive Metastore image
├── hive_custom_conf/     # Hive Metastore config (hive-site.xml)
├── trino/                # Trino config and Iceberg catalog
├── superset/             # Superset image and config
├── Airfllow/             # Airflow stack: its own compose file, image and DAGs
├── code/                 # Notebooks and Java UDF, mounted into Jupyter
├── data/                 # Local input files, mounted into Jupyter and Spark
├── SCENARIOS.md          # Practice exercises
└── errors.txt            # Error log
```

---

## Setup

**Prerequisites:** Docker with Compose v2, at least 8 GB RAM and 4 CPUs for Docker, about 15 GB of free disk.

```bash
# 1. Clone
git clone https://github.com/bhbyuh/lakehouse-Implementation.git
cd lakehouse-Implementation

# 2. Create the shared network (once)
docker network create lakehouse-network

# 3. Start the core stack (first build takes a few minutes)
docker compose up -d --build

# 4. Start Airflow (optional, needed for DAGs)
docker compose -f Airfllow/docker-compose.yaml up -d --build

# 5. Check status
docker compose ps
```

`createbuckets` and `superset-init` show **Exited (0)** once they finish their one-time setup. That is expected.

### Service URLs

| Service | URL | Login |
|---|---|---|
| Jupyter Lab | http://localhost:8888 | none |
| Spark Master | http://localhost:8080 | – |
| Trino | http://localhost:8085 | any username |
| Kafka UI | http://localhost:7070 | – |
| MinIO Console | http://localhost:9001 | `admin` / `12345678` |
| Superset | http://localhost:8088 | `admin` / `admin` |
| Airflow | http://localhost:8090 | `airflow` / `airflow` |

### Connection details

| What | Inside Docker | From host |
|---|---|---|
| Spark master | `spark://spark-master:7077` | `spark://localhost:7077` |
| Hive Metastore | `thrift://hive-metastore:9083` | `thrift://localhost:9083` |
| MinIO S3 API | `http://minio:9000` | `http://localhost:9000` |
| Kafka | `broker:9092` | `localhost:9094` |
| Trino | `trino:8080` | `localhost:8085` |
| Iceberg warehouse | `s3://buck1/warehouse` | – |

---

## Everyday commands

```bash
docker compose logs -f <service>                                # follow logs
docker exec -it trino trino --catalog iceberg                    # Trino SQL shell
docker compose -f Airfllow/docker-compose.yaml down && docker compose down        # stop (data kept)
docker compose -f Airfllow/docker-compose.yaml down -v && docker compose down -v  # stop and delete all data
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `network lakehouse-network ... could not be found` | `docker network create lakehouse-network` |
| Tables not found right after startup | Hive Metastore needs ~30 s to start. Check `docker compose logs hive-metastore` |
| Spark job stuck on "Initial job has not accepted any resources" | Another session is holding the cores. Run `spark.stop()` in other notebooks |
| Can't reach Kafka from a host script | Use `localhost:9094`, not `9092` |
| Containers restarting / out of memory | Give Docker more RAM or lower `SPARK_WORKER_MEMORY` |
