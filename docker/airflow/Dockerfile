# =============================================================================
# Airflow image — bao gồm spark-submit từ lakehouse-spark image
# Cần build lakehouse-spark:3.5.0 TRƯỚC khi build image này:
#   docker compose build spark-master
#   docker compose build airflow-webserver
# =============================================================================

# Stage 1: lấy Spark binaries từ image đã build
FROM lakehouse-spark:3.5.0 AS spark-stage

# Stage 2: Airflow chính thức + Spark client
FROM apache/airflow:2.10.0

ARG ORACLE_JDBC_VERSION=21.9.0.0
ARG PG_JDBC_VERSION=42.7.3

USER root

# Java 17 — openjdk-11 không có trong Debian bookworm (base của airflow:2.10.0)
# Spark 3.5.0 tương thích Java 8/11/17
# Tạo symlink /usr/local/lib/jvm-17 → thư mục JVM thực để JAVA_HOME không phụ thuộc arch
# (amd64: .../java-17-openjdk-amd64 | arm64: .../java-17-openjdk-arm64)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /usr/local/lib \
    && JAVA_BIN=$(readlink -f /usr/bin/java) \
    && JAVA_DIR=$(dirname "$(dirname "$JAVA_BIN")") \
    && ln -sfn "$JAVA_DIR" /usr/local/lib/jvm-17

# Copy toàn bộ Spark (bao gồm JARs Iceberg + JDBC drivers) từ spark stage
COPY --from=spark-stage /opt/spark /opt/spark

# JAVA_HOME trỏ qua symlink — hoạt động trên cả amd64 và arm64 (Apple Silicon)
ENV JAVA_HOME=/usr/local/lib/jvm-17
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download JDBC JARs cho Airflow JDBC Connection (Oracle + PostgreSQL)
RUN set -eux; \
    MAVEN="https://repo1.maven.org/maven2"; \
    mkdir -p /opt/airflow/setup/lib; \
    \
    curl -fsSL \
      "${MAVEN}/com/oracle/database/jdbc/ojdbc11/${ORACLE_JDBC_VERSION}/ojdbc11-${ORACLE_JDBC_VERSION}.jar" \
      -o "/opt/airflow/setup/lib/ojdbc11-${ORACLE_JDBC_VERSION}.jar"; \
    \
    curl -fsSL \
      "${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
      -o "/opt/airflow/setup/lib/postgresql-${PG_JDBC_VERSION}.jar"; \
    \
    chmod 644 /opt/airflow/setup/lib/*.jar; \
    echo "JDBC JARs:" && ls -lh /opt/airflow/setup/lib/

USER airflow

# Cài providers tại build time — không pip lại mỗi lần container start
# PIP_CONSTRAINT="" — clear constraint URL mà Airflow base image inject vào,
# tránh hash mismatch khi PyPI re-publish package (vd pyspark 4.1.1 bị re-upload).
# pyspark==3.5.3 — pin cứng để match Spark 3.5.0 cluster, ngăn pip kéo pyspark 4.x.
RUN PIP_CONSTRAINT="" pip install --no-cache-dir \
    pyspark==3.5.3 \
    apache-airflow-providers-apache-spark==4.8.0 \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-common-sql==1.12.0 \
    apache-airflow-providers-jdbc==4.3.0 \
    apache-airflow-providers-amazon==8.23.0 \
    jaydebeapi==1.2.3
