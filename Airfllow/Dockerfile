# Custom Airflow image: pinned providers instead of _PIP_ADDITIONAL_REQUIREMENTS,
# which would reinstall on every container start of every Airflow service.
FROM apache/airflow:3.2.0

USER root
# SparkSubmitOperator shells out to spark-submit, which needs a JVM.
# Drop this block if every DAG talks to Trino instead of Spark.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/current
ENV JAVA_HOME=/usr/lib/jvm/current

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The Spark driver runs here (client mode), so pyspark needs the same extra
# jars as the cluster image (../spark/Dockerfile): Iceberg, S3, Kafka.
# Keep these versions in sync with that file.
ARG SCALA=2.13
ARG SPARK=4.1.0
ARG ICEBERG=1.11.0
ARG HADOOP=3.4.2
ARG AWS_SDK=2.29.52
ARG KAFKA_CLIENTS=3.9.1
ARG COMMONS_POOL2=2.12.1
ARG M=https://repo1.maven.org/maven2
RUN set -eux; cd "$(python -c 'import os,pyspark;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')"; \
    curl -fsSLO $M/org/apache/iceberg/iceberg-spark-runtime-4.1_${SCALA}/${ICEBERG}/iceberg-spark-runtime-4.1_${SCALA}-${ICEBERG}.jar; \
    curl -fsSLO $M/org/apache/hadoop/hadoop-aws/${HADOOP}/hadoop-aws-${HADOOP}.jar; \
    curl -fsSLO $M/software/amazon/awssdk/bundle/${AWS_SDK}/bundle-${AWS_SDK}.jar; \
    curl -fsSLO $M/org/apache/spark/spark-sql-kafka-0-10_${SCALA}/${SPARK}/spark-sql-kafka-0-10_${SCALA}-${SPARK}.jar; \
    curl -fsSLO $M/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA}/${SPARK}/spark-token-provider-kafka-0-10_${SCALA}-${SPARK}.jar; \
    curl -fsSLO $M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS}/kafka-clients-${KAFKA_CLIENTS}.jar; \
    curl -fsSLO $M/org/apache/commons/commons-pool2/${COMMONS_POOL2}/commons-pool2-${COMMONS_POOL2}.jar
