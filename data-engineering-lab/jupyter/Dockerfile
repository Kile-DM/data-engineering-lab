FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# Jar'ы, чтобы Spark из коробки умел S3A (MinIO) и JDBC (ClickHouse)
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
# 0.6.0-all: самодостаточный JavaCC-парсер, нет конфликта ANTLR со Spark 3.5
# (0.8.x требует ANTLR 4.13, а Spark тащит 4.9.3)
ARG CLICKHOUSE_JDBC_VERSION=0.6.0

ARG MAVEN_REPO=https://maven-central.storage-download.googleapis.com/maven2

RUN set -eux; \
    cd "${SPARK_HOME}/jars"; \
    rm -f clickhouse-jdbc-*.jar; \
    curl -4 -fsSLO "${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -4 -fsSLO "${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -4 -fsSLO "${MAVEN_REPO}/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"; \
    chmod 644 *.jar

USER jovyan

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

RUN pip install --no-cache-dir \
    clickhouse-connect \
    boto3 \
    pyarrow \
    pandas \
    psycopg2-binary

WORKDIR /home/jovyan/work
