"""Generic, metadata-driven SCD Type 2 job for Silver Iceberg tables.

The job treats ``business_key + cob_dt`` as a deterministic run marker. A
rerun removes only versions created by that run and reopens only their direct
predecessors. This makes a retry safe after a failure between append and
expire without reopening unrelated historical rows.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent))

from common_utils import get_target_table, load_source_df, parse_arguments
from spark.spark_session import get_spark_session
from utils.logger import get_logger
from utils.yaml_loader import load_config


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_OPEN_END_DATE = "9999-12-31"
_METADATA_CONFIG_FIELDS = (
    "effective_from_column",
    "effective_to_column",
    "current_flag_column",
    "sk_column",
)


def _require_identifier(value: str, field: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{field} phải là SQL identifier hợp lệ, nhận được: {value!r}")


def _require_non_empty_column_list(value, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} phải là list không rỗng")
    if len(value) != len(set(value)):
        raise ValueError(f"{field} chứa cột trùng lặp: {value}")
    for column in value:
        _require_identifier(column, field)
    return value


def validate_config(config: dict) -> None:
    """Fail before Spark starts when the YAML contract is incomplete or unsafe."""
    if not isinstance(config, dict):
        raise ValueError("Config phải là YAML mapping")

    for field in ("job", "source", "target", "business_key", "scd", "tracked_columns", "sql"):
        if field not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {field}")

    if config["job"].get("type") != "scd_type2":
        raise ValueError(
            "Sai loại job, mong đợi job.type=scd_type2, "
            f"nhận được: {config['job'].get('type')}"
        )

    for target_field in ("catalog", "schema", "table"):
        if target_field not in config["target"]:
            raise ValueError(f"Thiếu cấu hình target.{target_field}")
        _require_identifier(config["target"][target_field], f"target.{target_field}")

    business_keys = _require_non_empty_column_list(config["business_key"], "business_key")
    tracked_columns = _require_non_empty_column_list(config["tracked_columns"], "tracked_columns")

    source_tables = config["source"].get("tables")
    if not isinstance(source_tables, list) or not source_tables or not all(
        isinstance(table, str) and table.strip() for table in source_tables
    ):
        raise ValueError("source.tables phải là list tên bảng không rỗng")

    scd = config["scd"]
    if scd.get("type") != 2:
        raise ValueError(f"scd.type phải bằng 2, nhận được: {scd.get('type')!r}")
    for scd_field in _METADATA_CONFIG_FIELDS:
        if scd_field not in scd:
            raise ValueError(f"Thiếu cấu hình scd.{scd_field}")
        _require_identifier(scd[scd_field], f"scd.{scd_field}")

    metadata_columns = {scd[field] for field in _METADATA_CONFIG_FIELDS}
    if len(metadata_columns) != len(_METADATA_CONFIG_FIELDS):
        raise ValueError("Các scd metadata columns phải có tên khác nhau")
    overlap = set(tracked_columns) & (set(business_keys) | metadata_columns | {"last_updated", "cob_dt"})
    if overlap:
        raise ValueError(
            "tracked_columns không được chứa business key hoặc metadata kỹ thuật: "
            f"{sorted(overlap)}"
        )

    detect_deletes = scd.get("detect_deletes", False)
    if not isinstance(detect_deletes, bool):
        raise ValueError("scd.detect_deletes phải là boolean")
    if detect_deletes and config["source"].get("snapshot_mode") != "full":
        raise ValueError("Chỉ được detect delete khi source.snapshot_mode=full")

    if not isinstance(config["sql"], str) or "{{ cob_dt }}" not in config["sql"]:
        raise ValueError("sql phải là string và có biến {{ cob_dt }}")


def _parse_cob_dt(cob_dt: str) -> tuple[str, str]:
    try:
        parsed = datetime.strptime(cob_dt, "%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"cob_dt phải đúng định dạng YYYY-MM-DD, nhận được: {cob_dt!r}") from exc
    previous = (parsed - timedelta(days=1)).strftime("%Y-%m-%d")
    return parsed.strftime("%Y-%m-%d"), previous


def _build_sk_col_name(config: dict) -> str:
    return config["scd"]["sk_column"]


def _sk_expression(business_keys: list[str], cob_dt: str):
    key_parts = [F.coalesce(F.col(key).cast("string"), F.lit("__NULL__")) for key in business_keys]
    return F.sha2(F.concat_ws("|", *key_parts, F.lit(cob_dt)), 256)


def _attach_scd2_columns(
    source_df,
    business_keys: list[str],
    cob_dt: str,
    sk_col: str,
    effective_col: str,
    expiry_col: str,
    current_flag: str,
):
    return (
        source_df
        .withColumn(effective_col, F.to_date(F.lit(cob_dt)))
        .withColumn(expiry_col, F.to_date(F.lit(_OPEN_END_DATE)))
        .withColumn(current_flag, F.lit(1))
        .withColumn(sk_col, _sk_expression(business_keys, cob_dt))
    )


def _sample_rows(df, columns: list[str], limit: int = 5) -> list[dict]:
    return [row.asDict(recursive=True) for row in df.select(*columns).limit(limit).collect()]


def _validate_source_df(source_df, config: dict) -> None:
    business_keys = config["business_key"]
    tracked_columns = config["tracked_columns"]
    scd = config["scd"]
    metadata_columns = {scd[field] for field in _METADATA_CONFIG_FIELDS}

    if len(source_df.columns) != len(set(source_df.columns)):
        raise ValueError(f"Source SELECT tạo tên cột trùng lặp: {source_df.columns}")

    required = set(business_keys) | set(tracked_columns)
    missing = sorted(required - set(source_df.columns))
    if missing:
        raise ValueError(f"Source thiếu business/tracked columns: {missing}")

    leaked_metadata = sorted(set(source_df.columns) & metadata_columns)
    if leaked_metadata:
        raise ValueError(f"Source không được tự cung cấp SCD metadata: {leaked_metadata}")

    null_condition = reduce(lambda left, right: left | right, [F.col(key).isNull() for key in business_keys])
    null_rows = source_df.filter(null_condition)
    if null_rows.limit(1).count():
        raise ValueError(
            "Source có business key NULL, ví dụ: "
            f"{_sample_rows(null_rows, business_keys)}"
        )

    duplicates = source_df.groupBy(*business_keys).count().filter(F.col("count") > 1)
    if duplicates.limit(1).count():
        raise ValueError(
            "Source có duplicate business key; job dừng thay vì dropDuplicates: "
            f"{_sample_rows(duplicates, business_keys + ['count'])}"
        )


def _validate_target_schema(spark, target: str, source_columns: list[str], config: dict) -> None:
    target_columns = spark.table(target).columns
    scd = config["scd"]
    expected = source_columns + [
        scd["effective_from_column"],
        scd["effective_to_column"],
        scd["current_flag_column"],
        scd["sk_column"],
    ]
    missing = [column for column in expected if column not in target_columns]
    unexpected = [column for column in target_columns if column not in expected]
    if missing or unexpected:
        raise ValueError(
            f"Schema target {target} không khớp output SCD2; "
            f"missing={missing}, unexpected={unexpected}"
        )


def _assert_not_backdated(spark, target: str, effective_col: str, cob_dt: str) -> None:
    future_rows = spark.table(target).filter(F.col(effective_col) > F.to_date(F.lit(cob_dt)))
    if future_rows.limit(1).count():
        raise ValueError(
            f"Không được chạy lùi cob_dt={cob_dt}; target đã có version hiệu lực sau ngày này"
        )


def _recover_run(
    spark,
    target: str,
    business_keys: list[str],
    sk_col: str,
    effective_col: str,
    expiry_col: str,
    current_flag: str,
    cob_dt: str,
    prev_dt: str,
    logger,
) -> None:
    """Remove only deterministic versions from this run and reopen their predecessors."""
    target_df = spark.table(target)
    run_versions = (
        target_df
        .withColumn("__expected_sk", _sk_expression(business_keys, cob_dt))
        .filter(
            (F.col(effective_col) == F.to_date(F.lit(cob_dt)))
            & (F.col(sk_col) == F.col("__expected_sk"))
        )
        .select(*business_keys, sk_col)
        .distinct()
        .cache()
    )
    view_name = f"scd2_recovery_{target.replace('.', '_')}_{cob_dt.replace('-', '')}"
    try:
        run_count = run_versions.count()
        if not run_count:
            return

        run_versions.createOrReplaceTempView(view_name)
        key_join = " AND ".join([f"t.{key} = r.{key}" for key in business_keys])

        spark.sql(f"""
            MERGE INTO {target} t
            USING {view_name} r
            ON {key_join}
               AND t.{sk_col} = r.{sk_col}
               AND t.{effective_col} = DATE '{cob_dt}'
            WHEN MATCHED THEN DELETE
        """)

        spark.sql(f"""
            MERGE INTO {target} t
            USING {view_name} r
            ON {key_join}
               AND t.{effective_col} < DATE '{cob_dt}'
               AND t.{expiry_col} = DATE '{prev_dt}'
               AND t.{current_flag} = 0
            WHEN MATCHED THEN UPDATE SET
                t.{expiry_col} = DATE '{_OPEN_END_DATE}',
                t.{current_flag} = 1
        """)
        logger.info("Recovered %d business keys for cob_dt=%s", run_count, cob_dt)
    finally:
        spark.catalog.dropTempView(view_name)
        run_versions.unpersist()


def _history_validation_errors(
    target_df,
    business_keys: list[str],
    sk_col: str,
    effective_col: str,
    expiry_col: str,
    current_flag: str,
) -> list[str]:
    errors = []
    required_columns = business_keys + [sk_col, effective_col, expiry_col, current_flag]
    null_condition = reduce(lambda left, right: left | right, [F.col(column).isNull() for column in required_columns])
    if target_df.filter(null_condition).limit(1).count():
        errors.append("business key/SCD metadata contains NULL")

    if target_df.filter(~F.col(current_flag).isin(0, 1)).limit(1).count():
        errors.append(f"{current_flag} contains a value outside 0/1")

    if target_df.groupBy(sk_col).count().filter(F.col("count") > 1).limit(1).count():
        errors.append(f"duplicate surrogate key {sk_col}")

    if (
        target_df.filter(F.col(current_flag) == 1)
        .groupBy(*business_keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        errors.append("more than one current row per business key")

    invalid_dates = target_df.filter(F.col(effective_col) > F.col(expiry_col))
    if invalid_dates.limit(1).count():
        errors.append("effective_from is after effective_to")

    invalid_open_end = target_df.filter(
        ((F.col(current_flag) == 1) & (F.col(expiry_col) != F.to_date(F.lit(_OPEN_END_DATE))))
        | ((F.col(current_flag) == 0) & (F.col(expiry_col) == F.to_date(F.lit(_OPEN_END_DATE))))
    )
    if invalid_open_end.limit(1).count():
        errors.append("current flag and effective_to sentinel are inconsistent")

    history_window = Window.partitionBy(*business_keys).orderBy(
        F.col(effective_col), F.col(expiry_col), F.col(sk_col)
    )
    overlap = (
        target_df
        .withColumn("__previous_effective_to", F.lag(F.col(expiry_col)).over(history_window))
        .filter(
            F.col("__previous_effective_to").isNotNull()
            & (F.col(effective_col) <= F.col("__previous_effective_to"))
        )
    )
    if overlap.limit(1).count():
        errors.append("effective date ranges overlap")

    return errors


def _validate_target_history(
    spark,
    target: str,
    source_df,
    config: dict,
    require_source_sync: bool,
) -> None:
    business_keys = config["business_key"]
    tracked_columns = config["tracked_columns"]
    scd = config["scd"]
    effective_col = scd["effective_from_column"]
    expiry_col = scd["effective_to_column"]
    current_flag = scd["current_flag_column"]
    sk_col = scd["sk_column"]
    detect_deletes = scd.get("detect_deletes", False)
    target_df = spark.table(target)

    errors = _history_validation_errors(
        target_df,
        business_keys,
        sk_col,
        effective_col,
        expiry_col,
        current_flag,
    )

    if require_source_sync:
        source_keys = source_df.select(*business_keys).distinct()
        current_target = target_df.filter(F.col(current_flag) == 1)
        current_keys = current_target.select(*business_keys).distinct()

        if source_keys.join(current_keys, business_keys, "left_anti").limit(1).count():
            errors.append("a source key has no current target row")
        if detect_deletes and current_keys.join(source_keys, business_keys, "left_anti").limit(1).count():
            errors.append("a deleted source key is still current in target")

        source_alias = source_df.alias("s")
        target_alias = current_target.alias("t")
        join_expr = [F.col(f"s.{key}") == F.col(f"t.{key}") for key in business_keys]
        mismatch_expr = reduce(
            lambda left, right: left | right,
            [
                F.coalesce(F.col(f"s.{column}").cast("string"), F.lit("__NULL__"))
                != F.coalesce(F.col(f"t.{column}").cast("string"), F.lit("__NULL__"))
                for column in tracked_columns
            ],
        )
        if source_alias.join(target_alias, join_expr, "inner").filter(mismatch_expr).limit(1).count():
            errors.append("current target values do not match tracked source values")

    if errors:
        raise RuntimeError(f"SCD2 validation failed for {target}: " + "; ".join(errors))


def _expire_keys(
    spark,
    target: str,
    keys_df,
    business_keys: list[str],
    effective_col: str,
    expiry_col: str,
    current_flag: str,
    cob_dt: str,
    prev_dt: str,
) -> None:
    view_name = f"scd2_expire_{target.replace('.', '_')}_{cob_dt.replace('-', '')}"
    try:
        keys_df.createOrReplaceTempView(view_name)
        join_on = " AND ".join([f"t.{key} = k.{key}" for key in business_keys])
        spark.sql(f"""
            MERGE INTO {target} t
            USING {view_name} k
            ON {join_on}
               AND t.{current_flag} = 1
               AND t.{effective_col} < DATE '{cob_dt}'
            WHEN MATCHED THEN UPDATE SET
                t.{expiry_col} = DATE '{prev_dt}',
                t.{current_flag} = 0
        """)
    finally:
        spark.catalog.dropTempView(view_name)


def run_scd_type2(spark, config: dict, cob_dt: str, logger) -> None:
    """Apply one full-snapshot SCD2 batch and validate the resulting history."""
    cob_dt, prev_dt = _parse_cob_dt(cob_dt)
    target = get_target_table(config)
    business_keys = config["business_key"]
    tracked_columns = config["tracked_columns"]
    scd = config["scd"]
    effective_col = scd["effective_from_column"]
    expiry_col = scd["effective_to_column"]
    current_flag = scd["current_flag_column"]
    sk_col = _build_sk_col_name(config)
    detect_deletes = scd.get("detect_deletes", False)

    cached_dfs = []
    source_df = None
    try:
        source_df = load_source_df(spark, config, cob_dt).cache()
        cached_dfs.append(source_df)
        _validate_source_df(source_df, config)
        _validate_target_schema(spark, target, source_df.columns, config)
        _assert_not_backdated(spark, target, effective_col, cob_dt)
        logger.info("Source rows: %d", source_df.count())

        _recover_run(
            spark,
            target,
            business_keys,
            sk_col,
            effective_col,
            expiry_col,
            current_flag,
            cob_dt,
            prev_dt,
            logger,
        )
        _validate_target_history(spark, target, source_df, config, require_source_sync=False)

        target_current = spark.table(target).filter(F.col(current_flag) == 1)
        source_alias = source_df.alias("s")
        target_alias = target_current.alias("t")
        join_expr = [F.col(f"s.{key}") == F.col(f"t.{key}") for key in business_keys]
        joined = source_alias.join(target_alias, join_expr, "left")

        exists_in_target = reduce(
            lambda left, right: left & right,
            [F.col(f"t.{key}").isNotNull() for key in business_keys],
        )
        change_expr = reduce(
            lambda left, right: left | right,
            [
                F.coalesce(F.col(f"s.{column}").cast("string"), F.lit("__NULL__"))
                != F.coalesce(F.col(f"t.{column}").cast("string"), F.lit("__NULL__"))
                for column in tracked_columns
            ],
        )

        changed_keys = (
            joined
            .filter(exists_in_target & change_expr)
            .select(*[F.col(f"s.{key}").alias(key) for key in business_keys])
            .distinct()
            .cache()
        )
        cached_dfs.append(changed_keys)

        new_or_changed = (
            joined
            .filter(~exists_in_target | change_expr)
            .select(*[F.col(f"s.{column}").alias(column) for column in source_df.columns])
            .cache()
        )
        cached_dfs.append(new_or_changed)

        if detect_deletes:
            deleted_keys = (
                target_current.alias("t")
                .join(source_df.alias("s"), join_expr, "left_anti")
                .select(*[F.col(f"t.{key}").alias(key) for key in business_keys])
                .distinct()
                .cache()
            )
        else:
            deleted_keys = target_current.filter(F.lit(False)).select(*business_keys).cache()
        cached_dfs.append(deleted_keys)

        changed_count = changed_keys.count()
        deleted_count = deleted_keys.count()
        new_version_count = new_or_changed.count()
        logger.info(
            "Classified rows: new_or_changed=%d, changed=%d, deleted=%d",
            new_version_count,
            changed_count,
            deleted_count,
        )

        if new_version_count:
            versions_to_insert = _attach_scd2_columns(
                new_or_changed,
                business_keys,
                cob_dt,
                sk_col,
                effective_col,
                expiry_col,
                current_flag,
            )
            versions_to_insert.writeTo(target).append()
            logger.info("Appended %d deterministic SCD2 versions", new_version_count)

        expire_keys = changed_keys.unionByName(deleted_keys).distinct().cache()
        cached_dfs.append(expire_keys)
        expire_count = expire_keys.count()
        if expire_count:
            _expire_keys(
                spark,
                target,
                expire_keys,
                business_keys,
                effective_col,
                expiry_col,
                current_flag,
                cob_dt,
                prev_dt,
            )
            logger.info("Expired %d previous versions", expire_count)

        _validate_target_history(spark, target, source_df, config, require_source_sync=True)
        logger.info("SCD Type 2 completed and validated for %s", target)
    finally:
        for data_frame in reversed(cached_dfs):
            data_frame.unpersist()


def main() -> None:
    args = parse_arguments("Silver SCD Type 2 Job")
    if args.cob_dt is None:
        raise ValueError("--cob_dt là bắt buộc cho SCD Type 2 job")

    logger = get_logger(__name__)
    config = load_config(args.config)
    validate_config(config)
    spark = None
    try:
        spark = get_spark_session(app_name=f"silver-scd2-{config['target']['table']}")
        run_scd_type2(spark, config, args.cob_dt, logger)
    except Exception as exc:
        logger.error("Job SCD Type 2 thất bại: %s", str(exc), exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
