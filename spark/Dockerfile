FROM apache/spark:4.1.0
USER root
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ARG SPARK=4.1.0
ARG SCALA=2.13
ARG ICEBERG=1.11.0
ARG HADOOP=3.4.2
ARG AWS_SDK=2.29.52
ARG KAFKA_CLIENTS=3.9.1
ARG COMMONS_POOL2=2.12.1
ARG M=https://repo1.maven.org/maven2

RUN set -eux; cd /opt/spark/jars; \
    curl -fLO $M/org/apache/iceberg/iceberg-spark-runtime-4.1_${SCALA}/${ICEBERG}/iceberg-spark-runtime-4.1_${SCALA}-${ICEBERG}.jar; \
    curl -fLO $M/org/apache/hadoop/hadoop-aws/${HADOOP}/hadoop-aws-${HADOOP}.jar; \
    curl -fLO $M/software/amazon/awssdk/bundle/${AWS_SDK}/bundle-${AWS_SDK}.jar; \
    curl -fLO $M/org/apache/spark/spark-sql-kafka-0-10_${SCALA}/${SPARK}/spark-sql-kafka-0-10_${SCALA}-${SPARK}.jar; \
    curl -fLO $M/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA}/${SPARK}/spark-token-provider-kafka-0-10_${SCALA}-${SPARK}.jar; \
    curl -fLO $M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS}/kafka-clients-${KAFKA_CLIENTS}.jar; \
    curl -fLO $M/org/apache/commons/commons-pool2/${COMMONS_POOL2}/commons-pool2-${COMMONS_POOL2}.jar

RUN mkdir -p /home/spark /app && chown -R spark:spark /home/spark /app
ENV HOME=/home/spark

USER spark
WORKDIR /app
EXPOSE 8888


