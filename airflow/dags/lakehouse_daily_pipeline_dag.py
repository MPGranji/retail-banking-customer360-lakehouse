"""Scheduled end-to-end batch pipeline for one processing date."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from etl_flag import (
    PIPELINE_RUN_ID_TEMPLATE,
    PROCESSING_DATE_TEMPLATE,
    make_end_flag_task,
    make_failure_callback,
    make_start_flag_task,
    processing_run_params,
)


DAG_ID = "lakehouse_daily_pipeline_dag"
COB_DT = PROCESSING_DATE_TEMPLATE
PIPELINE_RUN_ID = PIPELINE_RUN_ID_TEMPLATE

DEFAULT_ARGS = {
    "owner": "Granji",
    "start_date": pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Bronze → Silver daily → Gold → Masking → Data Quality",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    params=processing_run_params(),
    on_failure_callback=make_failure_callback(DAG_ID, "ops"),
    tags=["lakehouse", "daily", "end_to_end"],
)


def trigger_child(task_id: str, child_dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=child_dag_id,
        trigger_run_id=f"{{{{ run_id }}}}__{child_dag_id}",
        conf={"cob_dt": COB_DT, "pipeline_run_id": PIPELINE_RUN_ID},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
        dag=dag,
    )


start = make_start_flag_task(
    "pipeline_start",
    DAG_ID,
    "ops",
    dag,
    cob_dt=COB_DT,
    pipeline_run_id=PIPELINE_RUN_ID,
)

bronze_core = trigger_child("trigger_bronze_core", "bronze_core_banking_dag")
bronze_card = trigger_child("trigger_bronze_card_crm", "bronze_card_crm_dag")
silver = trigger_child("trigger_silver_daily", "silver_all_dag")
gold_mart = trigger_child("trigger_gold_mart360", "gold_mart360_dag")
gold_time = trigger_child("trigger_gold_time_analytics", "gold_time_analytics_dag")
gold_segments = trigger_child("trigger_gold_segmentation", "gold_segmentation_dag")
masking = trigger_child("trigger_pii_masking", "ops_pii_masking_daily_dag")
dq = trigger_child("trigger_data_quality", "ops_dq_daily_dag")

end = make_end_flag_task(
    "pipeline_end",
    DAG_ID,
    "ops",
    dag,
    cob_dt=COB_DT,
    pipeline_run_id=PIPELINE_RUN_ID,
)

start >> [bronze_core, bronze_card] >> silver
silver >> [gold_mart, gold_time] >> gold_segments >> masking >> dq >> end
