# Data Engineering Lakehouse Implementation Playground

A comprehensive Data Engineering playground for testing scenarios, tools, and architectures used in modern data engineering. This project provides a complete containerized environment with Apache Spark, Kafka, Hive, Trino, and other industry-standard tools.

## Project Overview

This is a hands-on learning project designed to explore and practice:
- **Distributed Computing**: Apache Spark for distributed data processing
- **Streaming**: Apache Kafka for real-time data streaming
- **Data Warehouse**: Hive Metastore for metadata management
- **SQL Querying**: Trino for distributed SQL queries
- **Object Storage**: MinIO (S3-compatible) for data lake storage
- **Workflow Orchestration**: Apache Airflow for scheduling and orchestrating data pipelines
- **Interactive Analysis**: Jupyter Lab for exploratory data analysis and prototyping

## Architecture

```
┌─────────────────────────────────────────────────┐
│         Jupyter Lab (Development)               │
│         :8888                                   │
└──────────────────┬──────────────────────────────┘
                   │
        ┌──────────┼──────────┐
        │          │          │
    ┌───▼──┐  ┌───▼──┐  ┌──▼────┐
    │Spark │  │Kafka │  │Trino  │
    │Master│  │Broker│  │:8085  │
    │:8080 │  │:9092 │  └───┬───┘
    └──┬───┘  └──┬───┘      │
       │         │          │
  ┌────▼────┐    │     ┌────▼──────┐
  │Spark    │    │     │Hive       │
  │Workers  │    │     │Metastore  │
  │:8081-82 │    │     │:9083      │
  └─────────┘    │     └────┬──────┘
                 │          │
              ┌──▼──────┐   │
              │MinIO    │───┘
              │S3:9000  │
              └─────────┘
```

## Tools & Services

### Core Data Processing
- **Apache Spark 4.1.0** - Distributed computing framework
  - 1 Master node (port 8080)
  - 2 Worker nodes (ports 8081, 8082)
  
- **Apache Kafka** - Streaming platform
  - Broker (port 9092)
  - Kafka UI (port 7070) - Web UI for monitoring topics and messages

### Data Warehouse & Query
- **Hive Metastore** (port 9083) - Metadata service for tables and schemas
  - PostgreSQL 15 (port 5432) - Backend database for Hive
  
- **Trino** (port 8085) - Distributed SQL query engine
  - Configured with Hive and MinIO connectors

### Storage
- **MinIO** (ports 9000, 9001)
  - S3-compatible object storage
  - Default bucket: `buck1`
  - Credentials: admin/12345678

### Development
- **Jupyter Lab** (port 8888)
  - PySpark integration
  - Connected to Spark cluster

## Prerequisites

- Docker & Docker Compose (version 3.8+)
- Git
- 8GB+ RAM recommended
- 10GB+ free disk space

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd lakehouse-Implementation
```

### 2. Create Docker Network

Before starting services, create the required Docker network:

```bash
docker network create lakehouse-network
```

### 3. Start Services

```bash
docker-compose up -d
```

This will start all services in the background. First startup may take 2-3 minutes as images are built/pulled.

### 4. Verify Services

Check that all services are running:

```bash
docker-compose ps
```

### 5. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter Lab | http://localhost:8888 | No auth required |
| Spark Master UI | http://localhost:8080 | - |
| Spark Worker A | http://localhost:8081 | - |
| Spark Worker B | http://localhost:8082 | - |
| Kafka UI | http://localhost:7070 | - |
| MinIO Console | http://localhost:9001 | admin/12345678 |
| Trino UI | http://localhost:8085 | - |

## Usage Examples

### Working with Jupyter Notebook

1. Open http://localhost:8888 in your browser
2. Navigate to the `/code` directory
3. Open `lakehouse.ipynb` or `minio.ipynb`

#### Example: Simple PySpark Job

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test-app") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Create a simple DataFrame
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()
```

#### Example: Writing to MinIO

```python
# Configure MinIO access
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "12345678")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Write DataFrame to MinIO
df.write.mode("overwrite").parquet("s3a://buck1/my-data")
```

### Working with Kafka

#### Create a Topic

```bash
docker exec -it broker kafka-topics.sh \
  --create \
  --topic my-topic \
  --bootstrap-server broker:9092 \
  --partitions 3 \
  --replication-factor 1
```

#### Publish Messages

```bash
docker exec -it broker kafka-console-producer.sh \
  --topic my-topic \
  --bootstrap-server broker:9092
# Type messages and press Enter to send
```

#### Consume Messages

```bash
docker exec -it broker kafka-console-consumer.sh \
  --topic my-topic \
  --bootstrap-server broker:9092 \
  --from-beginning
```

### Querying with Trino

Access Trino via command line in Jupyter or through the UI at http://localhost:8085

```sql
-- List all catalogs
SHOW CATALOGS;

-- Query data from Hive tables
SELECT * FROM hive.default.my_table;

-- Query data from MinIO
SELECT * FROM hive.default.s3_backed_table;
```

## Project Structure

```
lakehouse-Implementation/
├── README.md                 # This file
├── docker-compose.yaml       # Docker services configuration
├── Dockerfile                # Jupyter image with dependencies
├── requirements.txt          # Python dependencies
├── code/                     # Jupyter notebooks and code
│   ├── lakehouse.ipynb       # Main lakehouse practices
│   └── minio.ipynb           # MinIO integration examples
├── hive/                     # Hive Metastore configuration
├── hive_custom_conf/         # Hive custom configurations
├── trino/etc/                # Trino configuration files
├── Airflow/                  # Airflow DAGs and configuration
└── proj/                     # Additional project files
```

## Common Tasks

### View Spark Cluster Status

```bash
# Access Spark Master UI
open http://localhost:8080
```

### Monitor Kafka Topics

```bash
# Open Kafka UI
open http://localhost:7070
```

### Access MinIO Files

```bash
# Open MinIO Console
open http://localhost:9001
# Login with admin/12345678
# Navigate to buck1 bucket
```

### View Logs

```bash
# View specific service logs
docker-compose logs -f spark-master
docker-compose logs -f kafka-ui
docker-compose logs -f jupyter-local
```

### Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (careful - deletes data!)
docker-compose down -v
```

## Configuration

### Environment Variables

Key environment variables in `docker-compose.yaml`:

- **Spark Worker**
  - `SPARK_WORKER_CORES`: 2
  - `SPARK_WORKER_MEMORY`: 2g

- **Kafka**
  - `KAFKA_NUM_PARTITIONS`: 3
  - `KAFKA_NODE_ID`: 1

- **MinIO**
  - `MINIO_ROOT_USER`: admin
  - `MINIO_ROOT_PASSWORD`: 12345678

### Updating Jupyter Dependencies

Edit `requirements.txt` and rebuild:

```bash
docker-compose build jupyter-local
docker-compose restart jupyter-local
```

## Troubleshooting

### Services Won't Start

```bash
# Check if network exists
docker network ls | grep lakehouse-network

# If not, create it
docker network create lakehouse-network

# Restart services
docker-compose down
docker-compose up -d
```

### Spark Can't Connect to Master

Ensure services are on the same network:

```bash
docker network inspect lakehouse-network
```

### MinIO Bucket Not Found

```bash
# Check buckets
docker exec createbuckets mc ls myminio

# Manually create bucket if needed
docker exec -it minio mc alias set myminio http://localhost:9000 admin 12345678
docker exec -it minio mc mb myminio/buck1
```

### Jupyter Can't Access Spark

Check that Jupyter service is on `spark-network`:

```bash
docker-compose logs jupyter-local | grep -i spark
```

## Performance Tips

1. **Increase Spark Worker Memory**: Edit `docker-compose.yaml`, increase `SPARK_WORKER_MEMORY`
2. **Add More Workers**: Duplicate `spark-worker-b` service with new hostname/ports
3. **Kafka Partitions**: Increase `KAFKA_NUM_PARTITIONS` for better parallelism
4. **MinIO Performance**: Mount data volume on SSD if possible

## Learning Resources

### Notebooks in This Project

- **`lakehouse.ipynb`** - Main notebook covering:
  - Spark DataFrame operations
  - Data lake patterns
  - Lakehouse concepts
  
- **`minio.ipynb`** - MinIO and object storage integration:
  - S3-compatible operations
  - Data ingestion patterns

### External Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Trino Documentation](https://trino.io/docs/current/)
- [MinIO Documentation](https://docs.min.io/)
- [Hive Documentation](https://cwiki.apache.org/confluence/display/Hive)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

## Next Steps

1. Explore the Jupyter notebooks to understand the current setup
2. Try creating your own data pipeline
3. Experiment with Kafka streaming
4. Build Airflow DAGs for orchestration
5. Practice writing Spark jobs and Trino queries

## License

This is a personal learning project.

## Notes

- This playground uses development/default credentials for all services
- **Not suitable for production use**
- All data is persisted in Docker volumes; use `docker-compose down -v` to clear everything
- Memory limits can be adjusted based on your hardware
