"""Metadata-driven daily Data Quality runner for Lakehouse serving tables."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent.parent))

from spark.spark_session import get_spark_session
from utils.logger import get_logger
from utils.sql_renderer import render_sql
from utils.yaml_loader import load_config


VALID_SEVERITIES = {"critical", "warning"}
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lakehouse daily Data Quality")
    parser.add_argument("--config", required=True)
    parser.add_argument("--cob_dt", required=True)
    parser.add_argument("--pipeline_run_id", required=True)
    parser.add_argument("--dq_run_id", required=True)
    parser.add_argument("--jdbc_url", required=True)
    parser.add_argument("--db_user", required=True)
    parser.add_argument("--db_password", required=True)
    return parser.parse_args()


def validate_config(config: dict) -> None:
    checks = config.get("checks")
    if not isinstance(checks, list) or not checks:
        raise ValueError("DQ config phải có danh sách checks không rỗng")

    names = set()
    for check in checks:
        required = {"name", "schema", "table", "severity", "check_type"}
        missing = required - set(check)
        if missing:
            raise ValueError(f"DQ check thiếu fields {sorted(missing)}: {check}")
        if check["name"] in names:
            raise ValueError(f"DQ check name bị trùng: {check['name']}")
        names.add(check["name"])
        if check["severity"] not in VALID_SEVERITIES:
            raise ValueError(f"Severity không hợp lệ: {check['severity']}")
        if check["check_type"] == "schema_columns_absent":
            if not check.get("forbidden_columns"):
                raise ValueError(f"{check['name']} thiếu forbidden_columns")
        elif not isinstance(check.get("sql"), str):
            raise ValueError(f"{check['name']} thiếu SQL")


def _run_check(spark, check: dict, cob_dt: str) -> tuple[int, int, str | None]:
    if check["check_type"] == "schema_columns_absent":
        target = f"lakehouse.{check['schema']}.{check['table']}"
        columns = {column.lower() for column in spark.table(target).columns}
        forbidden = {column.lower() for column in check["forbidden_columns"]}
        exposed = sorted(columns & forbidden)
        return len(exposed), len(columns), ", ".join(exposed) or None

    sql = render_sql(check["sql"], {"cob_dt": cob_dt})
    rows = spark.sql(sql).limit(2).collect()
    if len(rows) != 1:
        raise ValueError(f"{check['name']} phải trả đúng một row, nhận {len(rows)}")
    values = {key.lower(): value for key, value in rows[0].asDict().items()}
    if "failed_count" not in values or "total_count" not in values:
        raise ValueError(f"{check['name']} phải trả failed_count và total_count")
    return int(values["failed_count"] or 0), int(values["total_count"] or 0), None


def execute_checks(spark, config: dict, cob_dt: str, pipeline_run_id: str, dq_run_id: str, logger):
    try:
        parsed_date = datetime.strptime(cob_dt, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"cob_dt phải đúng YYYY-MM-DD: {cob_dt!r}") from exc

    results = []
    for check in config["checks"]:
        error_detail = None
        try:
            failed_count, total_count, detail = _run_check(spark, check, cob_dt)
            expected = int(check.get("expected_failed_count", 0))
            passed = failed_count == expected
            error_detail = detail
            if not passed and not error_detail:
                error_detail = check.get("failure_message") or (
                    f"expected failed_count={expected}, actual={failed_count}"
                )
        except Exception as exc:  # record query/schema errors before failing the job
            failed_count, total_count, passed = -1, 0, False
            error_detail = f"{type(exc).__name__}: {exc}"[:4000]

        logger.info(
            "DQ %-45s severity=%s passed=%s failed_count=%s total_count=%s",
            check["name"], check["severity"], passed, failed_count, total_count,
        )
        results.append(
            {
                "check_name": check["name"],
                "table_name": check["table"],
                "schema_name": check["schema"],
                "cob_dt": parsed_date,
                "pipeline_run_id": pipeline_run_id,
                "dq_run_id": dq_run_id,
                "severity": check["severity"],
                "check_type": check["check_type"],
                "passed": passed,
                "failed_count": failed_count,
                "total_count": total_count,
                "error_detail": error_detail,
                "executed_at": datetime.now(),
            }
        )
    return results


def write_results(spark, results: list[dict], config: dict, jdbc_url: str, user: str, password: str) -> None:
    target = config.get("result_table", "opslakehouse.dq_check_result")
    # Do not use spark.createDataFrame(list[dict]) here.  That path serializes
    # Python rows on executors and requires the Airflow driver and Spark worker
    # to have the same Python minor version.  JVM-native literal projections
    # keep this operational write independent from the container Python images.
    frames = []
    for row in results:
        frames.append(
            spark.range(1).select(
                F.lit(row["check_name"]).cast("string").alias("check_name"),
                F.lit(row["table_name"]).cast("string").alias("table_name"),
                F.lit(row["schema_name"]).cast("string").alias("schema_name"),
                F.lit(row["cob_dt"]).cast("date").alias("cob_dt"),
                F.lit(row["pipeline_run_id"]).cast("string").alias("pipeline_run_id"),
                F.lit(row["dq_run_id"]).cast("string").alias("dq_run_id"),
                F.lit(row["severity"]).cast("string").alias("severity"),
                F.lit(row["check_type"]).cast("string").alias("check_type"),
                F.lit(row["passed"]).cast("boolean").alias("passed"),
                F.lit(row["failed_count"]).cast("long").alias("failed_count"),
                F.lit(row["total_count"]).cast("long").alias("total_count"),
                F.lit(row["error_detail"]).cast("string").alias("error_detail"),
                F.lit(row["executed_at"]).cast("timestamp").alias("executed_at"),
            )
        )

    result_df = frames[0]
    for frame in frames[1:]:
        result_df = result_df.unionByName(frame)

    (
        result_df
        .write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", target)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def raise_for_critical_failures(results: list[dict]) -> None:
    failed = [row["check_name"] for row in results if row["severity"] == "critical" and not row["passed"]]
    if failed:
        raise RuntimeError(f"Critical DQ checks failed: {', '.join(failed)}")


def main() -> None:
    args = parse_arguments()
    logger = get_logger(__name__)
    config = load_config(args.config)
    validate_config(config)
    spark = None
    try:
        spark = get_spark_session("daily-data-quality")
        results = execute_checks(
            spark, config, args.cob_dt, args.pipeline_run_id, args.dq_run_id, logger
        )
        write_results(spark, results, config, args.jdbc_url, args.db_user, args.db_password)
        raise_for_critical_failures(results)
        logger.info("Data Quality hoàn tất: %d checks", len(results))
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
