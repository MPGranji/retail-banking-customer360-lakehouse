"""
Auto-khởi tạo SparkSession khi chọn kernel "PySpark (Lakehouse)".
Sau khi kernel start, các biến sau có sẵn trong mọi cell:
    spark  — SparkSession
    sc     — SparkContext
"""
import os
import sys

sys.path.insert(0, "/opt/project")

try:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("jupyter-lakehouse")
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        # Iceberg
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type",      "rest")
        .config("spark.sql.catalog.lakehouse.uri",       os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"))
        .config("spark.sql.catalog.lakehouse.warehouse", os.environ.get("ICEBERG_WAREHOUSE",   "s3a://lakehouse/lakehouse"))
        .config("spark.sql.defaultCatalog",              "lakehouse")
        .config("spark.sql.session.timeZone",            "Asia/Ho_Chi_Minh")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",                   os.environ.get("MINIO_ENDPOINT",    "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key",                 os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key",                 os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access",          "true")
        .config("spark.hadoop.fs.s3a.impl",                       "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("✅ SparkSession ready!")
    print(f"   Version  : {spark.version}")
    print(f"   Master   : {sc.master}")
    print(f"   Catalog  : lakehouse  (Iceberg REST → http://iceberg-rest:8181)")
    print(f"   Storage  : MinIO S3A  (http://minio:9000)")
    print()
    print("Dùng ngay: spark  /  sc  /  spark.sql(...)")

except Exception as exc:
    print(f"⚠️  SparkSession init failed: {exc}")
    print("   Kiểm tra spark-master và iceberg-rest đang chạy.")
