"""One-time Silver fact bootstrap over the complete Bronze history."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

from etl_flag import (
    PIPELINE_RUN_ID_TEMPLATE,
    PROCESSING_DATE_TEMPLATE,
    latest_success_sql,
    make_end_flag_task,
    make_failure_callback,
    make_start_flag_task,
    processing_run_params,
)


DAG_ID = "silver_initial_dag"
COB_DT = PROCESSING_DATE_TEMPLATE
PIPELINE_RUN_ID = PIPELINE_RUN_ID_TEMPLATE
POSTGRES_CONN_ID = "postgres-etl"
SILVER_BASE = "/opt/project/code_etl/silver"
FACT_JOB = f"{SILVER_BASE}/base_job/fact_txn.py"
INITIAL_CONFIG = f"{SILVER_BASE}/initial"

INITIAL_FACTS = [
    ("fact_txn_account", "fact_txn_account_initial.yml"),
    ("fact_card_txn", "fact_card_txn_initial.yml"),
    ("fact_crm_interaction", "fact_crm_interaction_initial.yml"),
]

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
    description="One-time Silver historical fact bootstrap",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    params=processing_run_params(),
    on_failure_callback=make_failure_callback(DAG_ID, "silver"),
    tags=["silver", "initial", "one-time"],
)

start = make_start_flag_task("dag_start", DAG_ID, "silver", dag, cob_dt=COB_DT)

with TaskGroup("check_bootstrap_dependencies", dag=dag) as dependencies:
    for upstream in ("bronze_initial_dag", "silver_all_dag"):
        SqlSensor(
            task_id=f"check_{upstream}",
            conn_id=POSTGRES_CONN_ID,
            sql=latest_success_sql(upstream, COB_DT, PIPELINE_RUN_ID),
            poke_interval=30,
            timeout=1800,
            mode="reschedule",
            dag=dag,
        )

with TaskGroup("initial_facts", dag=dag) as initial_facts:
    for table_name, config_name in INITIAL_FACTS:
        SparkSubmitOperator(
            task_id=f"load_{table_name}_history",
            application=FACT_JOB,
            conn_id="spark_default",
            conf=SPARK_CONF,
            application_args=[
                "--config", f"{INITIAL_CONFIG}/{config_name}",
                "--cob_dt", COB_DT,
            ],
            verbose=True,
            dag=dag,
        )

end = make_end_flag_task("dag_end", DAG_ID, "silver", dag, cob_dt=COB_DT)

start >> dependencies >> initial_facts >> end
