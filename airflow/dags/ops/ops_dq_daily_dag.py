"""Daily Data Quality DAG; critical violations fail the pipeline."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from etl_flag import (
    PIPELINE_RUN_ID_TEMPLATE,
    PROCESSING_DATE_TEMPLATE,
    latest_success_sql,
    make_end_flag_task,
    make_failure_callback,
    make_start_flag_task,
    processing_run_params,
)
from jdbc_conn_utils import jdbc_jinja_args


DAG_ID = "ops_dq_daily_dag"
COB_DT = PROCESSING_DATE_TEMPLATE
PIPELINE_RUN_ID = PIPELINE_RUN_ID_TEMPLATE
DQ_RUN_ID = "{{ run_id }}::try-{{ ti.try_number }}"
POSTGRES_CONN_ID = "postgres-etl"
APPLICATION = "/opt/project/code_etl/shared/ops/data_quality.py"
CONFIG = "/opt/project/code_etl/shared/ops/dq_checks.yml"

DEFAULT_ARGS = {
    "owner": "Granji",
    "start_date": pendulum.datetime(2025, 12, 31, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONF = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores": "1",
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Daily metadata-driven Data Quality controls",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    params=processing_run_params(),
    on_failure_callback=make_failure_callback(DAG_ID, "ops"),
    tags=["ops", "data_quality", "daily"],
)

start = make_start_flag_task("dag_start", DAG_ID, "ops", dag, cob_dt=COB_DT)

wait_masking = SqlSensor(
    task_id="wait_ops_pii_masking_daily_dag",
    conn_id=POSTGRES_CONN_ID,
    sql=latest_success_sql("ops_pii_masking_daily_dag", COB_DT, PIPELINE_RUN_ID),
    poke_interval=30,
    timeout=1800,
    mode="reschedule",
    dag=dag,
)

jdbc = jdbc_jinja_args("postgres-card-crm")
run_dq = SparkSubmitOperator(
    task_id="run_data_quality",
    application=APPLICATION,
    conn_id="spark_default",
    conf=SPARK_CONF,
    application_args=[
        "--config", CONFIG,
        "--cob_dt", COB_DT,
        "--pipeline_run_id", PIPELINE_RUN_ID,
        "--dq_run_id", DQ_RUN_ID,
        "--jdbc_url", jdbc["jdbc_url"],
        "--db_user", jdbc["db_user"],
        "--db_password", jdbc["db_password"],
    ],
    verbose=True,
    dag=dag,
)

end = make_end_flag_task("dag_end", DAG_ID, "ops", dag, cob_dt=COB_DT)

start >> wait_masking >> run_dq >> end
