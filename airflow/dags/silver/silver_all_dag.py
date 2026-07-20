"""
Silver layer DAG — tổng hợp toàn bộ 10 bảng silver (7 dims + 3 facts).

Luồng thực thi:
  1. dag_start ghi cờ R
  2. Kiểm tra 2 bronze DAG đã hoàn thành (theo dag_id)
  3. Chạy song song 7 dim jobs (SCD1/SCD2)
  4. Sau khi dims xong, chạy song song 3 fact jobs
  5. dag_end ghi cờ S

DATA_COB_DT: hard-coded theo ngày load dữ liệu fake — thay đổi khi re-generate data.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

from etl_flag import make_start_flag_task, make_end_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID            = "silver_all_dag"
DATA_COB_DT       = "2025-12-31"   # ngày cuối của đợt data fake — cập nhật khi re-gen
POSTGRES_CONN_ID  = "postgres-etl"
SPARK_CONN_ID     = "spark_default"
SILVER_BASE       = "/opt/project/code_etl/silver"
SILVER_BASE_JOB   = f"{SILVER_BASE}/base_job"

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

# ─── Metadata bảng dim ────────────────────────────────────────────────────────
# (table_name, job_script, config_file)
# Các task trong TaskGroup có thể chạy song song khi tăng max_active_tasks.
# Với max_active_tasks=1 (chỉ 1 Spark executor), chúng chạy tuần tự.
DIM_JOBS = [
    ("dim_branch",   "scd_type1.py", "dim_branch.yml"),
    ("dim_product",  "scd_type1.py", "dim_product.yml"),
    ("dim_customer", "scd_type2.py", "dim_customer.yml"),
    ("dim_account",  "scd_type2.py", "dim_account.yml"),
    ("dim_deposit",  "scd_type1.py", "dim_deposit.yml"),
    ("dim_loan",     "scd_type1.py", "dim_loan.yml"),
    ("dim_card",     "scd_type1.py", "dim_card.yml"),
]

# (table_name, job_script, config_file)
FACT_JOBS = [
    ("fact_txn_account",     "fact_txn.py", "fact_txn_account.yml"),
    ("fact_card_txn",        "fact_txn.py", "fact_card_txn.yml"),
    ("fact_crm_interaction", "fact_txn.py", "fact_crm_interaction.yml"),
]

# Bronze DAG IDs cần check trước khi chạy.
# bronze_initial_dag: phải hoàn thành để fact tables (txn, card_txn, crm) có data.
BRONZE_DAG_IDS = ["bronze_core_banking_dag", "bronze_card_crm_dag", "bronze_initial_dag"]


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
    description="Silver layer — toàn bộ 7 dims + 3 facts (manual build)",
    schedule_interval=None,   # trigger thủ công
    catchup=False,
    max_active_tasks=1,
    tags=["silver", "all", "manual"],
)

# ── 1. Cờ DAG-level start ─────────────────────────────────────────────────────
dag_start = make_start_flag_task("dag_start", DAG_ID, "silver", dag, cob_dt=DATA_COB_DT)

# ── 2. Kiểm tra bronze DAGs đã hoàn thành ────────────────────────────────────
with TaskGroup("check_bronze", dag=dag) as check_bronze:
    for upstream_dag_id in BRONZE_DAG_IDS:
        SqlSensor(
            task_id=f"check_{upstream_dag_id}",
            conn_id=POSTGRES_CONN_ID,
            sql=_check_dag_flag_sql(upstream_dag_id),
            poke_interval=30,
            timeout=1800,
            mode="reschedule",
            dag=dag,
        )

# ── 3. Dim jobs (tất cả song song) ────────────────────────────────────────────
with TaskGroup("dims", dag=dag) as dims_group:
    for table_name, script, config_file in DIM_JOBS:
        SparkSubmitOperator(
            task_id=f"run_{table_name}",
            application=f"{SILVER_BASE_JOB}/{script}",
            conn_id=SPARK_CONN_ID,
            conf=SPARK_CONF,
            application_args=[
                "--config", f"{SILVER_BASE}/{config_file}",
                "--cob_dt", DATA_COB_DT,
            ],
            verbose=True,
            dag=dag,
        )

# ── 4. Fact jobs (tất cả song song, sau khi dims xong) ────────────────────────
with TaskGroup("facts", dag=dag) as facts_group:
    for table_name, script, config_file in FACT_JOBS:
        SparkSubmitOperator(
            task_id=f"run_{table_name}",
            application=f"{SILVER_BASE_JOB}/{script}",
            conn_id=SPARK_CONN_ID,
            conf=SPARK_CONF,
            application_args=[
                "--config", f"{SILVER_BASE}/{config_file}",
                "--cob_dt", DATA_COB_DT,
            ],
            verbose=True,
            dag=dag,
        )

# ── 5. Cờ DAG-level end ───────────────────────────────────────────────────────
dag_end = make_end_flag_task("dag_end", DAG_ID, "silver", dag, cob_dt=DATA_COB_DT)

# ─── Task dependencies ────────────────────────────────────────────────────────
dag_start >> check_bronze >> dims_group >> facts_group >> dag_end
