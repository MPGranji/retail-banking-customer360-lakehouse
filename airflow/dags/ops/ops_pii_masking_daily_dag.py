"""
Ops DAG - PII Masking (daily).
Tao/refresh cac bang masked sau khi gold layer hoan tat.
sandbox.mart_customer_360_masked va sandbox.dim_customer_masked.
"""

from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID               = "ops_pii_masking_daily_dag"
APPLICATION_PATH           = "/opt/project/code_etl/shared/ops/pii_masking.py"
POSTGRES_ETL_CONN_ID = "postgres-etl"
DEFAULT_COB_DT       = "2025-12-31"
COB_DT               = "{{ params.cob_dt }}"

DEFAULT_ARGS = {
    "owner": "Granji",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONF = {
    "spark.driver.memory":   "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores":  "1",
}

PII_ENV = {"PII_HASH_SALT": Variable.get("pii_hash_salt", default_var="")}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Daily PII masking - tao sandbox.mart_customer_360_masked va sandbox.dim_customer_masked",
    schedule_interval=None,   # sau khi gold hoan tat
    catchup=False,
    max_active_tasks=1,
    params={
        "cob_dt": Param(DEFAULT_COB_DT, type="string", format="date"),
    },
    tags=["ops", "pii", "masking", "compliance"],
)

# Cho gold.mart_customer_360 va silver.dim_customer
wait_gold_360 = SqlSensor(
    task_id="wait_gold_mart360_dag",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=(
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        "WHERE job_name = 'gold_mart360_dag' "
        "  AND status = 'S' "
        f"  AND cob_dt = '{COB_DT}' "
        "LIMIT 1"
    ),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

wait_silver_customer = SqlSensor(
    task_id="wait_silver_all_dag",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=(
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        "WHERE job_name = 'silver_all_dag' "
        "  AND status = 'S' "
        f"  AND cob_dt = '{COB_DT}' "
        "LIMIT 1"
    ),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)

mask_dim_customer = SparkSubmitOperator(
    task_id="mask_silver_dim_customer",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    env_vars=PII_ENV,
    application_args=["--cob_dt", COB_DT, "--target", "dim_customer"],
    verbose=True,
    dag=dag,
)

mask_mart_360 = SparkSubmitOperator(
    task_id="mask_gold_mart_customer_360",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    env_vars=PII_ENV,
    application_args=["--cob_dt", COB_DT, "--target", "mart_360"],
    verbose=True,
    dag=dag,
)

end = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

[wait_gold_360, wait_silver_customer] >> start >> [mask_dim_customer, mask_mart_360] >> end
