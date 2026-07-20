"""
Gold layer DAG — 4 bảng segmentation.

Luồng thực thi:
  1. dag_start ghi cờ R
  2. Kiểm tra silver_all_dag đã hoàn thành
  3. Chạy song song 3 seg jobs (rfm, churn, cross_sell)
  4. Kiểm tra gold_mart360_dag đã hoàn thành (campaign_target cần mart_customer_360_history)
  5. Chạy campaign_target
  6. dag_end ghi cờ S

DATA_COB_DT nhận từ manual Param ``cob_dt``.
Trigger sau khi gold_mart360_dag đã hoàn thành.
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
DAG_ID           = "gold_segmentation_dag"
DEFAULT_COB_DT   = "2025-12-31"
DATA_COB_DT      = "{{ params.cob_dt }}"
POSTGRES_CONN_ID = "postgres-etl"
SPARK_CONN_ID    = "spark_default"
GOLD_BASE        = "/opt/project/code_etl/gold"
GOLD_BASE_JOB    = f"{GOLD_BASE}/base_job"
GOLD_SEG         = f"{GOLD_BASE}/segmentation"

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

# (table_name, config_file) — chạy song song trước campaign_target
SEG_JOBS = [
    ("rfm_segment",       "rfm_segment.yml"),
    ("churn_prediction",  "churn_prediction.yml"),
    ("cross_sell_segment","cross_sell_segment.yml"),
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
    description="Gold segmentation — rfm, churn, cross_sell → campaign_target (manual build)",
    schedule_interval=None,   # trigger thủ công, sau khi gold_mart360_dag xong
    catchup=False,
    max_active_tasks=1,
    params={
        "cob_dt": Param(DEFAULT_COB_DT, type="string", format="date"),
    },
    tags=["gold", "segmentation", "manual"],
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

# ── 3. Seg jobs (rfm, churn, cross_sell — song song) ─────────────────────────
with TaskGroup("segments", dag=dag) as segments_group:
    for table_name, config_file in SEG_JOBS:
        SparkSubmitOperator(
            task_id=f"run_{table_name}",
            application=f"{GOLD_BASE_JOB}/gold_job.py",
            conn_id=SPARK_CONN_ID,
            conf=SPARK_CONF,
            application_args=[
                "--config", f"{GOLD_SEG}/{config_file}",
                "--cob_dt", DATA_COB_DT,
            ],
            verbose=True,
            dag=dag,
        )

# ── 4. Kiểm tra gold_mart360_dag (campaign_target cần history snapshot) ──────
check_mart360 = SqlSensor(
    task_id="check_gold_mart360_dag",
    conn_id=POSTGRES_CONN_ID,
    sql=_check_dag_flag_sql("gold_mart360_dag"),
    poke_interval=30,
    timeout=1800,
    mode="reschedule",
    dag=dag,
)

# ── 5. Campaign target ────────────────────────────────────────────────────────
run_campaign = SparkSubmitOperator(
    task_id="run_campaign_target",
    application=f"{GOLD_BASE_JOB}/gold_job.py",
    conn_id=SPARK_CONN_ID,
    conf=SPARK_CONF,
    application_args=[
        "--config", f"{GOLD_SEG}/campaign_target.yml",
        "--cob_dt", DATA_COB_DT,
    ],
    verbose=True,
    dag=dag,
)

# ── 6. Cờ DAG-level end ───────────────────────────────────────────────────────
dag_end = make_end_flag_task("dag_end", DAG_ID, "gold", dag, cob_dt=DATA_COB_DT)

# ─── Task dependencies ────────────────────────────────────────────────────────
# Thứ tự: silver xong → 3 seg jobs song song → mart360 xong → campaign_target → end
# check_mart360 đặt SAU segments_group vì gold_mart360_dag phải chờ silver xong trước,
# nếu đặt song song với check_silver thì timeout trước khi gold_mart360_dag kịp chạy.
dag_start >> check_silver >> segments_group >> check_mart360 >> run_campaign >> dag_end
