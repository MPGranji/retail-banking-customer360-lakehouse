"""
Job Fact Table dùng chung cho tầng Silver.
Được điều khiển bằng file YAML (metadata-driven).

Chiến lược ghi được khai báo bằng load.mode:
- daily: predicate overwrite đúng partition cob_dt;
- initial: dynamic overwrite toàn bộ historical partitions có trong input.

Đây là job chạy hàng ngày trên production — bảng đích phải đã tồn tại.
Để bootstrap lịch sử lần đầu, dùng `silver_initial_dag` và config trong
`silver/initial/`.
"""

import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent))

from utils.yaml_loader import load_config
from utils.logger import get_logger
from spark.spark_session import get_spark_session
from common_utils import parse_arguments, get_target_table, load_source_df


def validate_config(config: dict):
    """
    Kiểm tra file YAML có đủ các section bắt buộc không.
    Job Fact cần: job, source, target, business_key, load và sql.
    """
    for field in ["job", "source", "target", "business_key", "load", "sql"]:
        if field not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {field}")
    # Đảm bảo đúng loại job, tránh chạy nhầm config SCD vào fact job
    if config["job"]["type"] != "fact_txn":
        raise ValueError(f"Sai loại job, mong đợi job.type=fact_txn, nhận được: {config['job']['type']}")
    if not config["business_key"] or not all(isinstance(x, str) for x in config["business_key"]):
        raise ValueError("business_key phải là danh sách cột không rỗng")

    mode = config["load"].get("mode")
    if mode not in {"daily", "initial"}:
        raise ValueError("load.mode phải là daily hoặc initial")
    if config["load"].get("partition_column") != "cob_dt":
        raise ValueError("Fact job bắt buộc khai báo load.partition_column=cob_dt")
    if mode == "daily" and "{{ cob_dt }}" not in config["sql"]:
        raise ValueError("Daily fact SQL phải filter bằng {{ cob_dt }}")


def _validate_cob_dt(cob_dt: str) -> str:
    try:
        return datetime.strptime(cob_dt, "%Y-%m-%d").date().isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"cob_dt phải đúng định dạng YYYY-MM-DD, nhận được: {cob_dt!r}") from exc


def _assert_output_contract(fact_df, config: dict, cob_dt: str) -> None:
    partition_col = config["load"]["partition_column"]
    missing = set(config["business_key"] + [partition_col]) - set(fact_df.columns)
    if missing:
        raise ValueError(f"Fact output thiếu cột bắt buộc: {sorted(missing)}")

    if fact_df.filter(F.col(partition_col).isNull()).limit(1).collect():
        raise ValueError(f"Fact output có {partition_col} NULL")

    duplicate = (
        fact_df.groupBy(*config["business_key"])
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .collect()
    )
    if duplicate:
        raise ValueError(f"Fact output trùng business key {config['business_key']}")

    if config["load"]["mode"] == "daily":
        wrong_partition = fact_df.filter(
            F.col(partition_col) != F.to_date(F.lit(cob_dt))
        ).limit(1).collect()
        if wrong_partition:
            raise ValueError(f"Daily fact output chứa partition ngoài cob_dt={cob_dt}")


def run_fact_txn(spark, config: dict, cob_dt: str, logger):
    """
    Thực thi job Fact Table: chạy SQL transform rồi ghi đè partition ngày cob_dt.

    Dùng overwritePartitions thay vì overwrite toàn bảng để:
    - Chỉ xóa và ghi lại partition của ngày cob_dt
    - Các ngày khác trong bảng không bị mất dữ liệu
    - An toàn khi chạy lại (idempotent): chạy lại cùng ngày cho ra kết quả như nhau
    """
    cob_dt = _validate_cob_dt(cob_dt)
    target = get_target_table(config)

    # Đọc và transform dữ liệu nguồn theo SQL trong YAML
    fact_df = load_source_df(spark, config, cob_dt).cache()
    try:
        _assert_output_contract(fact_df, config, cob_dt)
        mode = config["load"]["mode"]
        if mode == "daily":
            logger.info("Ghi đè đúng partition cob_dt=%s vào %s", cob_dt, target)
            fact_df.writeTo(target).overwrite(F.col("cob_dt") == F.to_date(F.lit(cob_dt)))
        else:
            logger.info("Initial load: ghi đè các historical partitions hiện diện vào %s", target)
            fact_df.writeTo(target).overwritePartitions()
        logger.info("Ghi Fact table hoàn tất")
    finally:
        fact_df.unpersist()


def main():
    """Điểm vào của chương trình: parse args → validate config → chạy job → dọn dẹp."""
    args = parse_arguments("Silver Fact Table Job")
    logger = get_logger(__name__)
    config = load_config(args.config)
    validate_config(config)
    spark = None
    try:
        spark = get_spark_session(app_name=f"silver-fact-{config['target']['table']}")
        run_fact_txn(spark, config, args.cob_dt, logger)
    except Exception as e:
        logger.error(f"Job Fact table thất bại: {str(e)}", exc_info=True)
        raise
    finally:
        # Luôn dừng Spark khi xong để giải phóng tài nguyên cluster
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
