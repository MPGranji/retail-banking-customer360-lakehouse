"""
Ops DAG — Implement Iceberg tables (DROP PURGE + CREATE).

Mục đích:
    Reset sạch bảng Iceberg cho tầng được chọn, dùng khi:
    - Thay đổi schema (add/drop column)
    - Muốn xóa toàn bộ data và bắt đầu lại
    - Debug / test environment

Param (trigger thủ công):
    layer: bronze | silver | gold | all

Thứ tự xử lý:
    DROP PURGE — gold → silver → bronze (dependency-safe)
    CREATE     — bronze → silver → gold

CẢNH BÁO: Thao tác này XÓA TOÀN BỘ DATA trong tầng được chọn và không thể hoàn tác.
"""

from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID       = "implement_iceberg_table_dag"
APPLICATION_PATH = "/opt/project/code_etl/shared/ops/reset_iceberg_tables.py"

DEFAULT_ARGS = {
    "owner":            "Granji",
    "depends_on_past":  False,
    "start_date":       pendulum.datetime(2024, 12, 30, tz="Asia/Ho_Chi_Minh"),
    "retries":          0,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONF = {
    "spark.driver.memory":   "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores":  "1",
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="[OPS] DROP PURGE + CREATE Iceberg tables theo layer được chọn",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    tags=["ops", "iceberg", "ddl", "reset"],
    params={
        "layer": Param(
            default="bronze",
            enum=["bronze", "silver", "gold", "all"],
            type="string",
            description=(
                "Tầng cần reset (DROP PURGE + CREATE):\n"
                "  bronze — 10 bảng bronze\n"
                "  silver — 10 bảng silver\n"
                "  gold   — 11 bảng gold\n"
                "  all    — toàn bộ 31 bảng\n"
                "CẢNH BÁO: Xóa toàn bộ data, không thể hoàn tác."
            ),
        )
    },
)

start = make_start_flag_task("start", DAG_ID, "ops", dag)

drop_and_recreate = SparkSubmitOperator(
    task_id="drop_purge_and_recreate",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    application_args=["--layer", "{{ params.layer }}"],
    execution_timeout=timedelta(minutes=30),
    verbose=True,
    dag=dag,
)

end = make_end_flag_task("end", DAG_ID, "ops", dag)

start >> drop_and_recreate >> end
