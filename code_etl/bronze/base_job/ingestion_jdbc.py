"""
Framework nạp dữ liệu vào tầng Bronze qua kết nối JDBC.
Được điều khiển bằng file YAML (metadata-driven), dùng lại được cho nhiều bảng.
Hỗ trợ 3 chiến lược nạp: full_snapshot, incremental, initial_full.
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

from spark.spark_session import get_spark_session
from spark.iceberg_utils import get_iceberg_table_name, write_to_iceberg
from utils.yaml_loader import load_config
from utils.sql_renderer import render_sql
from utils.logger import get_logger


def parse_arguments():
    """Đọc các tham số dòng lệnh khi chạy job."""
    parser = argparse.ArgumentParser(description="Bronze JDBC Ingestion Job")
    parser.add_argument("--config",      required=True,            help="Đường dẫn đến file cấu hình YAML")
    parser.add_argument("--cob_dt",      required=True,            help="Ngày xử lý dữ liệu (định dạng YYYY-MM-DD)")
    parser.add_argument("--jdbc_url",    required=True,            help="Chuỗi kết nối JDBC đến database nguồn")
    parser.add_argument("--db_user",     required=True,            help="Tên đăng nhập database")
    parser.add_argument("--db_password", required=True,            help="Mật khẩu database")
    parser.add_argument("--fetchsize",   type=int, default=10000,  help="Số dòng mỗi lần JDBC kéo về (mặc định: 10000)")
    return parser.parse_args()


def validate_config(config):
    """
    Kiểm tra file YAML có đủ các mục bắt buộc không.
    Nếu thiếu bất kỳ mục nào sẽ báo lỗi ngay, tránh chạy dở dang.
    """
    # Kiểm tra 4 section cấp cao nhất phải có mặt
    required_sections = ["source", "target", "load", "sql"]
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {section}")

    # Section source phải khai báo loại database (oracle, postgresql, ...)
    required_source_fields = ["type"]
    for field in required_source_fields:
        if field not in config["source"]:
            raise ValueError(f"Thiếu trường bắt buộc trong source: {field}")

    # Section target phải có đủ 3 phần để xác định bảng đích trong Iceberg
    required_target_fields = ["catalog", "schema", "table"]
    for field in required_target_fields:
        if field not in config["target"]:
            raise ValueError(f"Thiếu trường bắt buộc trong target: {field}")

    # Chiến lược nạp phải được khai báo và phải là một trong 3 loại hợp lệ
    if "strategy" not in config["load"]:
        raise ValueError("Thiếu load.strategy trong config")

    strategy = config["load"]["strategy"]
    if strategy not in ["full_snapshot", "incremental", "initial_full"]:
        raise ValueError(f"Chiến lược nạp không hợp lệ: {strategy}")


def extract_from_source(spark, config, cob_dt, jdbc_url, db_user, db_password, fetchsize, logger):
    """
    Kết nối JDBC đến database nguồn, chạy SQL và trả về DataFrame.

    Các bước:
    1. Render câu SQL từ template (thay biến {{cob_dt}} bằng ngày thực tế).
    2. Tự động nhận diện driver JDBC phù hợp theo prefix của jdbc_url.
    3. Nếu config có khai báo jdbc_partition → đọc song song nhiều partition (tránh OOM).
    4. Gắn cột cob_dt vào mỗi dòng dữ liệu.
    """
    sql = config["sql"]
    template_vars = {"cob_dt": cob_dt}
    logic_sql = render_sql(sql, template_vars)
    logger.info(f"SQL sau khi render:\n{logic_sql}")

    # fetchsize trong YAML (riêng từng bảng) sẽ ưu tiên hơn tham số dòng lệnh (global)
    effective_fetchsize = config["source"].get("fetchsize", fetchsize)

    # Bản đồ prefix URL → tên class driver JDBC tương ứng
    _DRIVER_MAP = {
        "jdbc:oracle:":     "oracle.jdbc.OracleDriver",
        "jdbc:postgresql:": "org.postgresql.Driver",
        "jdbc:mysql:":      "com.mysql.cj.jdbc.Driver",
        "jdbc:sqlserver:":  "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    # Tìm driver phù hợp dựa vào tiền tố của jdbc_url
    driver_class = next(
        (cls for prefix, cls in _DRIVER_MAP.items() if jdbc_url.startswith(prefix)),
        None,
    )

    # Cấu hình JDBC reader cơ bản
    reader = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({logic_sql}) t")  # Wrap SQL thành subquery để Spark đọc
        .option("user", db_user)
        .option("password", db_password)
        .option("fetchsize", effective_fetchsize)
    )
    if driver_class:
        reader = reader.option("driver", driver_class)

    # JDBC partitioning: chia nhỏ dữ liệu ra nhiều partition để đọc song song.
    # Mỗi Spark executor đọc một đoạn dữ liệu theo khoảng giá trị của partition_column.
    # Chỉ nên dùng cho bảng lớn (> ~100k dòng) với cột số tăng dần (PK/sequence).
    # Nếu không dùng, mặc định 1 executor kéo toàn bộ bảng → có thể bị OOM.
    partition_cfg = config["source"].get("jdbc_partition")
    if partition_cfg:
        logger.info(
            f"Đọc song song JDBC: cột={partition_cfg['partition_column']}, "
            f"khoảng=[{partition_cfg['lower_bound']}, {partition_cfg['upper_bound']}], "
            f"số partition={partition_cfg['num_partitions']}"
        )
        reader = (
            reader
            .option("partitionColumn", partition_cfg["partition_column"])
            .option("lowerBound",      str(partition_cfg["lower_bound"]))
            .option("upperBound",      str(partition_cfg["upper_bound"]))
            .option("numPartitions",   str(partition_cfg["num_partitions"]))
        )

    df = reader.load()

    # Gắn cột cob_dt để biết dữ liệu thuộc ngày nào:
    # - initial_full mode: mỗi dòng tự suy cob_dt từ cột ngày thực tế trong data
    #   → đúng với ngày dữ liệu, không phải ngày chạy job
    # - full_snapshot / incremental: dùng cob_dt tĩnh truyền vào từ dòng lệnh
    cob_dt_col = config["load"].get("cob_dt_from_column")
    if cob_dt_col:
        logger.info(f"Lấy cob_dt từ cột trong data: {cob_dt_col}")
        return df.withColumn("cob_dt", F.col(cob_dt_col).cast("date"))
    return df.withColumn("cob_dt", F.lit(cob_dt).cast("date"))


def run_ingestion(spark, config, cob_dt, jdbc_url, db_user, db_password, fetchsize, logger):
    """
    Luồng chính của job: đọc dữ liệu từ nguồn rồi ghi vào bảng Iceberg tầng Bronze.
    """
    logger.info("Bắt đầu job nạp dữ liệu Bronze")
    logger.info(f"Bảng đích: {config['target']}")
    logger.info(f"Chiến lược nạp: {config['load']['strategy']}")

    # Bước 1: Kéo dữ liệu từ database nguồn qua JDBC
    df = extract_from_source(spark, config, cob_dt, jdbc_url, db_user, db_password, fetchsize, logger)

    # Bước 2: Xây dựng tên bảng Iceberg đầy đủ (catalog.schema.table)
    target = config["target"]
    table_name = get_iceberg_table_name(
        catalog=target["catalog"],
        schema=target["schema"],
        table=target["table"]
    )

    logger.info(f"Đang ghi vào bảng Iceberg: {table_name}")

    # Bước 3: Ghi DataFrame vào Iceberg
    write_to_iceberg(df, table_name, logger)

    logger.info(f"Nạp dữ liệu hoàn tất cho bảng: {table_name}")


def main():
    """Điểm vào của chương trình: parse args → load config → chạy job → dọn dẹp."""
    args = parse_arguments()

    logger = get_logger(__name__)

    spark = None
    try:
        # Đọc và kiểm tra file cấu hình YAML
        config = load_config(args.config)
        validate_config(config)

        # Khởi tạo Spark Session
        spark = get_spark_session()

        # Chạy luồng nạp dữ liệu chính
        run_ingestion(
            spark=spark,
            config=config,
            cob_dt=args.cob_dt,
            jdbc_url=args.jdbc_url,
            db_user=args.db_user,
            db_password=args.db_password,
            fetchsize=args.fetchsize,
            logger=logger
        )

    except Exception as e:
        logger.error(f"Job nạp dữ liệu thất bại: {str(e)}", exc_info=True)
        raise
    finally:
        # Đảm bảo Spark luôn được dừng dù có lỗi hay không, tránh rò rỉ tài nguyên
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
