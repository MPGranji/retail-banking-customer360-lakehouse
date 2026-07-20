"""
Gold layer DAG — time_analytics: hiệu suất chi nhánh theo tháng.

Bảng được tạo:
  - mart_branch_monthly_summary: Hiệu suất chi nhánh theo tháng

Luồng thực thi:
  1. dag_start ghi cờ R
  2. Kiểm tra silver_all_dag đã hoàn thành
  3. Chạy mart_branch_monthly_summary
  4. dag_end ghi cờ S
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

from etl_flag import make_start_flag_task, make_end_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID           = "gold_time_analytics_dag"
DATA_COB_DT      = "2025-12-31"   # ngày cuối của đợt data fake — cập nhật khi re-gen
POSTGRES_CONN_ID = "postgres-etl"
SPARK_CONN_ID    = "spark_default"
GOLD_BASE        = "/opt/project/code_etl/gold"
GOLD_BASE_JOB    = f"{GOLD_BASE}/base_job"
GOLD_TIME        = f"{GOLD_BASE}/time_analytics"

DEFAULT_ARGS = {
    "owner": "Granji",
    "start_date": pendulum.datetime(2025, 12, 31, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONF = {
    "spark.driver.memory":   "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores":  "1",
}

TIME_ANALYTICS_JOBS = [
    ("mart_branch_monthly_summary", "branch_monthly_summary.yml"),
]


def _check_dag_flag_sql(upstream_dag_id: str) -> str:
    return (
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        f"WHERE job_name = '{upstream_dag_id}' "
        "  AND status = 'S' "
        f"  AND cob_dt = DATE '{DATA_COB_DT}' "
        "LIMIT 1"
    )


# ─── DAG ──────────────────────────────────────────────────────────────────────
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Gold time_analytics — branch monthly summary (manual build)",
    schedule_interval=None,   # trigger thủ công
    catchup=False,
    max_active_tasks=1,
    tags=["gold", "time_analytics", "manual"],
)

# ── 1. Cờ DAG-level start ─────────────────────────────────────────────────────
dag_start = make_start_flag_task("dag_start", DAG_ID, "gold", dag, cob_dt=DATA_COB_DT)

# ── 2. Kiểm tra silver_all_dag đã hoàn thành ─────────────────────────────────
check_silver = SqlSensor(
    task_id="check_silver_all_dag",
    conn_id=POSTGRES_CONN_ID,
    sql=_check_dag_flag_sql("silver_all_dag"),
    poke_interval=30,
    timeout=1800,
    mode="reschedule",
    dag=dag,
)

# ── 3. Time analytics job ─────────────────────────────────────────────────────
with TaskGroup("time_analytics", dag=dag) as time_analytics_group:
    for table_name, config_file in TIME_ANALYTICS_JOBS:
        SparkSubmitOperator(
            task_id=f"run_{table_name}",
            application=f"{GOLD_BASE_JOB}/gold_job.py",
            conn_id=SPARK_CONN_ID,
            conf=SPARK_CONF,
            application_args=[
                "--config", f"{GOLD_TIME}/{config_file}",
                "--cob_dt", DATA_COB_DT,
            ],
            dag=dag,
        )

# ── 4. Cờ DAG-level end ───────────────────────────────────────────────────────
dag_end = make_end_flag_task("dag_end", DAG_ID, "gold", dag, cob_dt=DATA_COB_DT)

# ─── Task dependencies ────────────────────────────────────────────────────────
dag_start >> check_silver >> time_analytics_group >> dag_end
