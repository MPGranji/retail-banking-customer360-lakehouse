"""
Job dùng chung cho tầng Gold.
Được điều khiển bằng file YAML (metadata-driven).

Chạy SQL transform từ tầng Silver → ghi kết quả vào bảng Iceberg tầng Gold.
Chiến lược ghi được khai báo bắt buộc trong YAML:
  - overwrite_partitions : ghi đè partition của ngày cob_dt
  - overwrite_all        : thay toàn bộ serving table hiện hành

Hỗ trợ các loại job (job.type trong YAML):
  - mart360        : Bảng Customer 360 mart (tổng hợp thông tin khách hàng)
  - segment        : Bảng phân khúc khách hàng
  - time_analytics : Bảng phân tích theo chiều thời gian

Đây là job chạy hàng ngày trên production — bảng đích phải đã tồn tại.
Để khởi tạo lần đầu, dùng: gold/bootstrap/initial_load.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent))

from utils.yaml_loader import load_config
from utils.logger import get_logger
from spark.spark_session import get_spark_session
from common_utils import parse_arguments, get_target_table, load_source_df
from pyspark.sql.functions import lit

# Danh sách loại job hợp lệ trong tầng Gold
VALID_JOB_TYPES = {"mart360", "segment", "time_analytics"}
VALID_WRITE_STRATEGIES = {"overwrite_partitions", "overwrite_all"}


def validate_config(config: dict):
    """
    Kiểm tra file YAML có đủ các section bắt buộc không.
    Gold job cần: job, source, target, write, sql.
    """
    for field in ["job", "source", "target", "write", "sql"]:
        if field not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {field}")
    # Kiểm tra job.type phải là một trong các loại hợp lệ
    job_type = config["job"].get("type")
    if job_type not in VALID_JOB_TYPES:
        raise ValueError(f"Loại job không hợp lệ '{job_type}'. Phải là một trong: {VALID_JOB_TYPES}")

    strategy = config["write"].get("strategy")
    if strategy not in VALID_WRITE_STRATEGIES:
        raise ValueError(
            f"Chiến lược ghi không hợp lệ '{strategy}'. "
            f"Phải là một trong: {VALID_WRITE_STRATEGIES}"
        )


def run_gold_job(spark, config: dict, cob_dt: str, logger):
    """
    Thực thi Gold job theo write.strategy đã khai báo trong YAML.

    ``overwrite_partitions`` giữ lịch sử các ngày khác và idempotent theo cob_dt.
    ``overwrite_all`` thay toàn bộ bảng serving để luôn chỉ còn snapshot hiện hành.
    """
    target   = get_target_table(config)
    job_type = config["job"]["type"]
    strategy = config["write"]["strategy"]

    # Chạy SQL transform để tính toán dữ liệu tầng Gold
    result_df = load_source_df(spark, config, cob_dt)
    logger.info("[%s] Đang ghi vào %s bằng %s", job_type, target, strategy)
    if strategy == "overwrite_partitions":
        result_df.writeTo(target).overwritePartitions()
    else:
        result_df.writeTo(target).overwrite(lit(True))
    logger.info(f"[{job_type}] Ghi hoàn tất cho {target}")


def main():
    """Điểm vào của chương trình: parse args → validate config → chạy job → dọn dẹp."""
    args = parse_arguments("Gold Layer Job")
    logger = get_logger(__name__)
    spark = None
    try:
        config = load_config(args.config)
        validate_config(config)
        spark = get_spark_session(app_name=f"gold-{config['job']['type']}-{config['target']['table']}")
        run_gold_job(spark, config, args.cob_dt, logger)
    except Exception as e:
        logger.error(f"Gold job thất bại: {str(e)}", exc_info=True)
        raise
    finally:
        # Luôn dừng Spark khi xong để giải phóng tài nguyên cluster
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
