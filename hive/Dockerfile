FROM apache/hive:4.0.0
USER root
# Use the S3A jars that match the image's own Hadoop 3.3.6
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar \
       /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar /opt/hive/lib/
ADD --chmod=644 https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/hive/lib/postgresql-42.7.3.jar
USER hive