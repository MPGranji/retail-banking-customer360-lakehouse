"""
Bronze Initial Load DAG — nạp toàn bộ dữ liệu lịch sử cho 3 bảng giao dịch.

Vấn đề cần giải quyết:
  - txn_account / card_txn / crm_interaction dùng chiến lược 'incremental'
    trong DAG thường ngày (lọc last_updated = T-1).
  - Lần đầu chạy pipeline, dữ liệu lịch sử 365 ngày cần được nạp đầy đủ.
  - Nếu chỉ chạy DAG incremental, mỗi lần chỉ lấy 1 ngày → bỏ sót toàn bộ lịch sử.

Giải pháp:
  - Job đọc toàn bộ bảng (không có WHERE last_updated).
  - cob_dt của mỗi row được suy từ last_updated của chính row đó
    → 365 ngày dữ liệu → 365 partition Iceberg riêng biệt.
  - DAG này chỉ trigger 1 lần (schedule_interval=None), SAU KHI:
      1. Data generator đã chạy xong
      2. bronze_core_banking_dag và bronze_card_crm_dag đã chạy xong
         (để dim tables đã có data trước khi silver chạy)

Thứ tự trigger toàn pipeline lần đầu:
  bronze_core_banking_dag  ─┐
  bronze_card_crm_dag       ├─> bronze_initial_dag ─> silver_all_dag ─> gold_*
  (dim tables)              ┘   (txn tables)
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from jdbc_conn_utils import jdbc_jinja_args
from etl_flag import make_start_flag_task, make_end_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID            = "bronze_initial_dag"
SPARK_APPLICATION = "/opt/project/code_etl/bronze/base_job/ingestion_jdbc.py"
REMOTE_CONFIG_DIR = "/opt/project/code_etl/bronze/initial"

# Ngày ghi vào flag_job_etl — đại diện cho đợt initial load này.
# Đặt trùng với ngày cuối của dữ liệu fake để silver có thể check flag đúng.
INITIAL_COB_DT = "2025-12-31"

DEFAULT_ARGS = {
    "owner": "Granji",
    "start_date": pendulum.datetime(2025, 12, 31, tz="Asia/Ho_Chi_Minh"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONF = {
    "spark.driver.memory":   "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores":  "1",
}

# (config_file, conn_id)
# Oracle: txn_account; Postgres: card_txn, crm_interaction
INITIAL_JOBS = [
    ("txn_account_initial.yml",    "oracle-core"),
    ("card_txn_initial.yml",       "postgres-card-crm"),
    ("crm_interaction_initial.yml","postgres-card-crm"),
]

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Bronze initial load — toàn bộ lịch sử 1 năm (Jan–Dec 2025) cho 3 bảng giao dịch (chạy 1 lần)",
    schedule_interval=None,   # trigger thủ công 1 lần duy nhất
    catchup=False,
    max_active_tasks=1,
    tags=["bronze", "initial", "one-time"],
)

# ── Cờ DAG-level ──────────────────────────────────────────────────────────────
dag_start = make_start_flag_task(
    "dag_start", DAG_ID, "bronze", dag, cob_dt=INITIAL_COB_DT
)

# ── Initial load tasks ────────────────────────────────────────────────────────
with TaskGroup("initial_load", dag=dag) as initial_load:
    for config_file, conn_id in INITIAL_JOBS:
        table_name = config_file.replace(".yml", "")
        conn_tmpl  = jdbc_jinja_args(conn_id)

        SparkSubmitOperator(
            task_id=f"ingest_{table_name}",
            application=SPARK_APPLICATION,
            conn_id="spark_default",
            conf=SPARK_CONF,
            application_args=[
                "--config",      f"{REMOTE_CONFIG_DIR}/{config_file}",
                # --cob_dt vẫn truyền vào nhưng job sẽ bỏ qua khi cob_dt_from_column
                # được khai báo trong YAML → cob_dt suy từ last_updated của mỗi row.
                "--cob_dt",      INITIAL_COB_DT,
                "--jdbc_url",    conn_tmpl["jdbc_url"],
                "--db_user",     conn_tmpl["db_user"],
                "--db_password", conn_tmpl["db_password"],
            ],
            verbose=True,
            dag=dag,
        )

dag_end = make_end_flag_task(
    "dag_end", DAG_ID, "bronze", dag, cob_dt=INITIAL_COB_DT
)

dag_start >> initial_load >> dag_end
