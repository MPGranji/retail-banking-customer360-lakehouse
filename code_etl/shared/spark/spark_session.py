"""
SparkSession factory — Iceberg catalog với MinIO backend.
Đọc cấu hình từ biến môi trường để tránh hardcode credential.
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "lakehouse-job") -> SparkSession:
    """
    Tạo SparkSession với Iceberg REST catalog và MinIO S3A.

    Biến môi trường bắt buộc:
        ICEBERG_CATALOG_URI   — REST catalog URI, vd http://iceberg-rest:8181
        ICEBERG_WAREHOUSE     — s3a://lakehouse/lakehouse
        MINIO_ENDPOINT        — http://minio:9000
        MINIO_ACCESS_KEY      — minio access key
        MINIO_SECRET_KEY      — minio secret key
    """
    catalog_uri = os.environ["ICEBERG_CATALOG_URI"]
    warehouse = os.environ["ICEBERG_WAREHOUSE"]
    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access_key = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key = os.environ["MINIO_SECRET_KEY"]

    spark = (
        SparkSession.builder
        .appName(app_name)
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Iceberg REST catalog (catalog name = "lakehouse")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", catalog_uri)
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", "lakehouse")
        # S3FileIO — Iceberg dùng AWS SDK v2 để đọc/ghi data files trực tiếp qua MinIO.
        # Bắt buộc khai báo io-impl và endpoint, nếu không REST catalog sẽ tự suy luận
        # dẫn đến lỗi "Cannot initialize FileIO, missing no-arg constructor: S3FileIO"
        # khi iceberg-aws-bundle chưa được load vào classpath đúng lúc.
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", minio_secret_key)
        # Múi giờ Việt Nam — đảm bảo timestamp hiển thị và tính toán đúng theo VN time
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        # S3A filesystem (hadoop-aws) — dùng cho spark-submit path resolution và các job
        # cần đọc file config/script từ s3a://, song song với S3FileIO ở trên.
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
