"""
Util DAG — Thực thi SparkSQL tùy ý qua trigger param.

Mục đích:
    Chạy một câu SQL bất kỳ trên cụm Spark (dùng spark-sql CLI).
    Hữu ích để:
    - Kiểm tra dữ liệu trong bảng Iceberg
    - Chạy DDL thủ công (ALTER TABLE, REFRESH, ...)
    - Debug nhanh mà không cần notebook

Param (trigger thủ công):
    sql: Câu SQL cần thực thi (ví dụ: SELECT * FROM bronze.txn_account LIMIT 10)

Lưu ý:
    - SQL truyền qua biến môi trường SPARK_SQL để tránh lỗi shell quoting
    - Không ghi ETL flag — DAG này không thuộc pipeline chính
"""

from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum

DAG_ID = "util_spark_sql"

DEFAULT_ARGS = {
    "owner":            "Granji",
    "depends_on_past":  False,
    "start_date":       pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries":          0,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="[UTIL] Thực thi SparkSQL tùy ý qua trigger param (spark-sql -e)",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    tags=["util", "spark", "sql", "iceberg", "admin"],
    params={
        "sql": Param(
            default="SELECT current_timestamp()",
            type="string",
            description=(
                "Câu SQL cần thực thi trên Spark.\n"
                "Ví dụ:\n"
                "  SELECT * FROM bronze.txn_account LIMIT 10\n"
                "  SHOW TABLES IN silver\n"
                "  DESCRIBE EXTENDED gold.mart_customer_360"
            ),
        )
    },
)

start_job = EmptyOperator(task_id="start_job", dag=dag)

run_sql = BashOperator(
    task_id="run_sql",
    bash_command=(
        "/opt/spark/bin/spark-sql"
        " --master spark://spark-master:7077"
        " --driver-memory 512m"
        " --conf spark.ui.enabled=false"
        ' -e "$SPARK_SQL"'
    ),
    env={"SPARK_SQL": "{{ params.sql }}"},
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

end_job = EmptyOperator(task_id="end_job", dag=dag)

start_job >> run_sql >> end_job
