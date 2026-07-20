"""
IPython startup script — tự động khởi tạo SparkSession cho kernel "PySpark (Lakehouse)".

Script này nằm trong ~/.ipython/profile_default/startup/ và được IPython
chạy tự động khi kernel khởi động. Chỉ khởi tạo Spark khi INIT_PYSPARK=true
(env var này được kernel.json của PySpark kernel set, không phải Python thường).

Sau khi init xong, các biến sau có sẵn trong mọi cell:
    spark  — SparkSession (default catalog: lakehouse / Iceberg REST)
    sc     — SparkContext
"""
import os
import sys

if os.environ.get("INIT_PYSPARK") != "true":
    # Không phải PySpark (Lakehouse) kernel — bỏ qua, không làm gì
    pass
else:
    sys.path.insert(0, "/opt/project")

    # Đọc config từ env vars (được inject bởi docker-compose)
    _spark_master   = os.environ.get("SPARK_MASTER_URL",    "spark://spark-master:7077")
    _catalog_uri    = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
    _warehouse      = os.environ.get("ICEBERG_WAREHOUSE",   "s3a://lakehouse/lakehouse")
    _minio_endpoint = os.environ.get("MINIO_ENDPOINT",      "http://minio:9000")
    # Hỗ trợ cả MINIO_ACCESS_KEY và AWS_ACCESS_KEY_ID
    _access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ["AWS_ACCESS_KEY_ID"]
    _secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ["AWS_SECRET_ACCESS_KEY"]

    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .appName("jupyter-lakehouse")
            .master(_spark_master)

            # --- Iceberg Spark extensions ---
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )

            # --- Iceberg REST Catalog ---
            .config("spark.sql.catalog.lakehouse",             "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type",        "rest")
            .config("spark.sql.catalog.lakehouse.uri",         _catalog_uri)
            .config("spark.sql.catalog.lakehouse.warehouse",   _warehouse)

            # --- Iceberg S3 FileIO (đọc/ghi file Iceberg trực tiếp vào MinIO) ---
            # Thiếu phần này → Iceberg tìm được catalog nhưng không đọc/ghi được data
            .config("spark.sql.catalog.lakehouse.io-impl",                  "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lakehouse.s3.endpoint",              _minio_endpoint)
            .config("spark.sql.catalog.lakehouse.s3.path-style-access",     "true")
            .config("spark.sql.catalog.lakehouse.s3.access-key-id",         _access_key)
            .config("spark.sql.catalog.lakehouse.s3.secret-access-key",     _secret_key)

            .config("spark.sql.defaultCatalog",    "lakehouse")
            .config("spark.sql.session.timeZone",  "Asia/Ho_Chi_Minh")

            # --- Hadoop S3A filesystem (cho spark.read.format("parquet").load("s3a://...")) ---
            .config("spark.hadoop.fs.s3a.endpoint",                  _minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key",                _access_key)
            .config("spark.hadoop.fs.s3a.secret.key",                _secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access",         "true")
            .config("spark.hadoop.fs.s3a.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )

            .getOrCreate()
        )

        sc = spark.sparkContext
        sc.setLogLevel("WARN")

        print("✅ SparkSession ready!")
        print(f"   Spark    : {spark.version}")
        print(f"   Master   : {sc.master}")
        print(f"   Catalog  : lakehouse → Iceberg REST ({_catalog_uri})")
        print(f"   Storage  : MinIO S3A ({_minio_endpoint})")
        print()
        print("Biến có sẵn: spark  |  sc")
        print("Thử ngay  : spark.sql('SHOW NAMESPACES').show()")

    except Exception as _exc:
        print(f"⚠️  SparkSession init thất bại: {_exc}")
        print("   Kiểm tra: docker ps | grep 'spark-master\\|iceberg-rest'")
        print("   Hoặc tạo SparkSession thủ công trong notebook.")
