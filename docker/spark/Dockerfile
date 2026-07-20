# =============================================================================
# Custom Spark Image — Spark 3.5.0 + Iceberg 1.4.3 + JDBC drivers
# Base: apache/spark:3.5.0 (Hadoop 3.3.4, Java 11, Scala 2.12)
# =============================================================================
FROM apache/spark:3.5.0

USER root

# Phiên bản các dependency — thay đổi ở đây nếu cần nâng cấp
ARG ICEBERG_VERSION=1.4.3
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG ORACLE_JDBC_VERSION=21.9.0.0
ARG PG_JDBC_VERSION=42.7.3

# iceberg-aws-bundle chứa AWS SDK v2 mà S3FileIO yêu cầu.
# Không thể dùng aws-java-sdk-bundle (v1) thay thế — S3FileIO chỉ dùng SDK v2.

ENV SPARK_JARS_DIR=/opt/spark/jars

# -----------------------------------------------------------------------------
# Download JARs từ Maven Central
# - iceberg-spark-runtime: Iceberg Spark extensions + REST catalog support
# - hadoop-aws: S3A filesystem connector (s3a://)
# - aws-java-sdk-bundle: AWS SDK cho hadoop-aws
# - ojdbc11: Oracle JDBC driver (Java 11+)
# - postgresql: PostgreSQL JDBC driver
# -----------------------------------------------------------------------------
RUN set -eux; \
    MAVEN="https://repo1.maven.org/maven2"; \
    \
    # Iceberg Spark Runtime (bao gồm Iceberg core + Spark extensions)
    curl -fsSL \
      "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    \
    # Iceberg AWS Bundle — AWS SDK v2 cho S3FileIO (bắt buộc, khác với aws-java-sdk-bundle v1)
    # Nếu thiếu JAR này sẽ gặp: IllegalArgumentException: Cannot initialize FileIO, missing no-arg constructor
    curl -fsSL \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    \
    # Hadoop AWS S3A connector
    curl -fsSL \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    \
    # AWS Java SDK Bundle (dependency của hadoop-aws)
    curl -fsSL \
      "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    \
    # Oracle JDBC driver (ojdbc11 cho Java 11+, Oracle 21c)
    curl -fsSL \
      "${MAVEN}/com/oracle/database/jdbc/ojdbc11/${ORACLE_JDBC_VERSION}/ojdbc11-${ORACLE_JDBC_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/ojdbc11-${ORACLE_JDBC_VERSION}.jar"; \
    \
    # PostgreSQL JDBC driver
    curl -fsSL \
      "${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/postgresql-${PG_JDBC_VERSION}.jar"; \
    \
    echo "JARs downloaded:" && ls -lh "${SPARK_JARS_DIR}/"*.jar | grep -E "iceberg|hadoop-aws|aws-java|ojdbc|postgresql"

# Kiểm tra iceberg-aws-bundle có mặt (nếu thiếu sẽ báo lỗi khi build)
RUN ls "${SPARK_JARS_DIR}/iceberg-aws-bundle-"*.jar || (echo "FATAL: iceberg-aws-bundle missing" && exit 1)

# Tắt WARN "Cannot locate configuration" của Hadoop S3A metrics
# Hadoop tìm file này trong classpath; tạo file rỗng là đủ để dừng warning
RUN mkdir -p /opt/spark/conf \
    && touch /opt/spark/conf/hadoop-metrics2-s3a-file-system.properties \
    && touch /opt/spark/conf/hadoop-metrics2.properties

# Thư mục event log dùng chung giữa tất cả containers qua Docker volume.
# 777 để cả spark user và airflow user đều ghi được khi dùng bind mount.
RUN mkdir -p /opt/spark/spark-events && chmod 777 /opt/spark/spark-events

# Thư mục work phải do spark user sở hữu — worker tạo executor subdirs ở đây.
# Nếu root sở hữu (mặc định khi container restart), worker sẽ báo
# "Failed to create directory /opt/spark/work/<app-id>/<executor-id>"
RUN mkdir -p /opt/spark/work && chown spark:spark /opt/spark/work && chmod 755 /opt/spark/work

# Cấu hình mặc định: event logging + history server log dir
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Copy project code vào image (dùng bind mount khi dev, build vào image khi deploy)
# COPY ../../code_etl /opt/project/code_etl

# Fallback defaults — bị override bởi docker-compose env (SPARK_WORKER_MEMORY/CORES trong .env)
ENV SPARK_WORKER_MEMORY=8g
ENV SPARK_WORKER_CORES=8

USER spark
