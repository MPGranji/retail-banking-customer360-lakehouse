"""
Gold layer DAG — tổng hợp Customer 360 history/current và 4 bảng summary.

Luồng thực thi:
  1. dag_start ghi cờ R
  2. Kiểm tra silver_all_dag đã hoàn thành (theo dag_id)
  3. Chạy history song song với 4 summary jobs
  4. Refresh current mart sau khi history hoàn thành
  5. dag_end ghi cờ S

DATA_COB_DT nhận từ manual Param ``cob_dt``.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

from etl_flag import make_start_flag_task, make_end_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID           = "gold_mart360_dag"
DEFAULT_COB_DT   = "2025-12-31"
DATA_COB_DT      = "{{ params.cob_dt }}"
POSTGRES_CONN_ID = "postgres-etl"
SPARK_CONN_ID    = "spark_default"
GOLD_BASE        = "/opt/project/code_etl/gold"
GOLD_BASE_JOB    = f"{GOLD_BASE}/base_job"
GOLD_MART360     = f"{GOLD_BASE}/mart360"

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

# (table_name, config_file) — không phụ thuộc history/current
SUMMARY_JOBS = [
    ("customer_balance_summary",     "customer_balance_summary.yml"),
    ("customer_card_summary",        "customer_card_summary.yml"),
    ("customer_product_summary",     "customer_product_summary.yml"),
    ("customer_transaction_summary", "customer_transaction_summary.yml"),
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
    description="Gold mart360 — 5 bảng mart (manual build)",
    schedule_interval=None,   # trigger thủ công
    catchup=False,
    max_active_tasks=1,
    params={
        "cob_dt": Param(DEFAULT_COB_DT, type="string", format="date"),
    },
    tags=["gold", "mart360", "manual"],
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

# ── 3. Bốn summary jobs độc lập ──────────────────────────────────────────────
with TaskGroup("summaries", dag=dag) as summaries_group:
    for table_name, config_file in SUMMARY_JOBS:
        SparkSubmitOperator(
            task_id=f"run_{table_name}",
            application=f"{GOLD_BASE_JOB}/gold_job.py",
            conn_id=SPARK_CONN_ID,
            conf=SPARK_CONF,
            application_args=[
                "--config", f"{GOLD_MART360}/{config_file}",
                "--cob_dt", DATA_COB_DT,
            ],
            verbose=True,
            dag=dag,
        )

run_history = SparkSubmitOperator(
    task_id="run_mart_customer_360_history",
    application=f"{GOLD_BASE_JOB}/gold_job.py",
    conn_id=SPARK_CONN_ID,
    conf=SPARK_CONF,
    application_args=[
        "--config", f"{GOLD_MART360}/customer_360_history.yml",
        "--cob_dt", DATA_COB_DT,
    ],
    verbose=True,
    dag=dag,
)

# Current chỉ đọc partition history vừa hoàn thành và full-overwrite serving table.
run_current = SparkSubmitOperator(
    task_id="run_mart_customer_360",
    application=f"{GOLD_BASE_JOB}/gold_job.py",
    conn_id=SPARK_CONN_ID,
    conf=SPARK_CONF,
    application_args=[
        "--config", f"{GOLD_MART360}/customer_360.yml",
        "--cob_dt", DATA_COB_DT,
    ],
    verbose=True,
    dag=dag,
)

# ── 4. Cờ DAG-level end ───────────────────────────────────────────────────────
dag_end = make_end_flag_task("dag_end", DAG_ID, "gold", dag, cob_dt=DATA_COB_DT)

# ─── Task dependencies ────────────────────────────────────────────────────────
dag_start >> check_silver
check_silver >> [summaries_group, run_history]
run_history >> run_current
[summaries_group, run_current] >> dag_end
