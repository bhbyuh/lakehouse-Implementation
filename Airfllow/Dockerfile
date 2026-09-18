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
