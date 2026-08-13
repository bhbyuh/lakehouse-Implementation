FROM apache/hive:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y curl

RUN curl -L \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
-o /opt/hive/lib/hadoop-aws-3.3.6.jar


RUN curl -L \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
-o /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar


RUN curl -L \
https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
-o /opt/hive/lib/postgresql-42.7.3.jar


USER hive