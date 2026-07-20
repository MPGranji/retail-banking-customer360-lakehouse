"""
Iceberg-specific utilities for Bronze layer writes.
Enforces overwritePartitions convention and pre-created table assumption.
"""

from pyspark.sql import DataFrame


def get_iceberg_table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def write_to_iceberg(df: DataFrame, table_name: str, logger) -> None:
    """
    Write DataFrame to Iceberg table using overwritePartitions.

    Không gọi df.count() trước write — sẽ gây executor OOM với bảng lớn
    (java.io.EOFException). cob_dt được thêm vào DataFrame trước khi gọi hàm này.
    """
    spark = df.sparkSession
    df.writeTo(table_name).overwritePartitions()

    # row_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    # logger.info(f"Successfully wrote {row_count} rows to {table_name}")
