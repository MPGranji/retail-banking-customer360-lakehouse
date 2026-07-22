"""Run-aware ETL flag helpers shared by all business DAGs.

Every child DAG in one end-to-end pipeline receives the same
``pipeline_run_id``.  Sensors inspect the latest flag for that exact pipeline,
job and processing date, so a success left by an older run cannot hide a new
running or failed attempt.
"""

from __future__ import annotations

from functools import partial

from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


POSTGRES_ETL_CONN_ID = "postgres-etl"

# Manual runs may provide cob_dt/pipeline_run_id in dag_run.conf.  Scheduled
# runs naturally fall back to Airflow's logical processing date and run id.
PROCESSING_DATE_TEMPLATE = "{{ dag_run.conf.get('cob_dt') or ds }}"
PIPELINE_RUN_ID_TEMPLATE = "{{ dag_run.conf.get('pipeline_run_id') or run_id }}"
DAG_RUN_ID_TEMPLATE = "{{ run_id }}"
TRY_NUMBER_TEMPLATE = "{{ ti.try_number }}"


def processing_run_params() -> dict:
    """Return fresh Param objects for a child/standalone business DAG."""
    return {
        "cob_dt": Param(None, type=["null", "string"], format="date"),
        "pipeline_run_id": Param(None, type=["null", "string"]),
    }


_FLAG_SQL_RUNNING = """
    INSERT INTO opslakehouse.flag_job_etl
        (job_name, schema_name, table_name, status, start_time, end_time,
         cob_dt, dag_run_id, pipeline_run_id, try_number, error_detail)
    VALUES
        (%(dag_id)s, %(layer)s, %(dag_id)s, 'R',
         (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh'), NULL, %(cob_dt)s::date,
         %(dag_run_id)s, %(pipeline_run_id)s, %(try_number)s::int, NULL);
"""

_FLAG_SQL_SUCCESS = """
    INSERT INTO opslakehouse.flag_job_etl
        (job_name, schema_name, table_name, status, start_time, end_time,
         cob_dt, dag_run_id, pipeline_run_id, try_number, error_detail)
    VALUES
        (%(dag_id)s, %(layer)s, %(dag_id)s, 'S',
         NULL, (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh'), %(cob_dt)s::date,
         %(dag_run_id)s, %(pipeline_run_id)s, %(try_number)s::int, NULL);
"""

_FLAG_SQL_FAILURE = """
    INSERT INTO opslakehouse.flag_job_etl
        (job_name, schema_name, table_name, status, start_time, end_time,
         cob_dt, dag_run_id, pipeline_run_id, try_number, error_detail)
    VALUES
        (%(dag_id)s, %(layer)s, %(dag_id)s, 'F',
         NULL, (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh'), %(cob_dt)s::date,
         %(dag_run_id)s, %(pipeline_run_id)s, %(try_number)s::int,
         %(error_detail)s);
"""


def _flag_parameters(
    dag_id: str,
    layer: str,
    cob_dt: str,
    dag_run_id: str,
    pipeline_run_id: str,
) -> dict:
    return {
        "dag_id": dag_id,
        "layer": layer,
        "cob_dt": cob_dt,
        "dag_run_id": dag_run_id,
        "pipeline_run_id": pipeline_run_id,
        "try_number": TRY_NUMBER_TEMPLATE,
    }


def make_start_flag_task(
    task_id: str,
    dag_id: str,
    layer: str,
    dag,
    cob_dt: str = PROCESSING_DATE_TEMPLATE,
    dag_run_id: str = DAG_RUN_ID_TEMPLATE,
    pipeline_run_id: str = PIPELINE_RUN_ID_TEMPLATE,
) -> PostgresOperator:
    """Insert an immutable Running flag for this concrete DAG run."""
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id=POSTGRES_ETL_CONN_ID,
        sql=_FLAG_SQL_RUNNING,
        parameters=_flag_parameters(dag_id, layer, cob_dt, dag_run_id, pipeline_run_id),
        dag=dag,
    )


def make_end_flag_task(
    task_id: str,
    dag_id: str,
    layer: str,
    dag,
    cob_dt: str = PROCESSING_DATE_TEMPLATE,
    dag_run_id: str = DAG_RUN_ID_TEMPLATE,
    pipeline_run_id: str = PIPELINE_RUN_ID_TEMPLATE,
) -> PostgresOperator:
    """Insert an immutable Success flag for this concrete DAG run."""
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id=POSTGRES_ETL_CONN_ID,
        sql=_FLAG_SQL_SUCCESS,
        parameters=_flag_parameters(dag_id, layer, cob_dt, dag_run_id, pipeline_run_id),
        dag=dag,
    )


def latest_success_sql(upstream_dag_id: str, cob_dt: str, pipeline_run_id: str) -> str:
    """SqlSensor query that accepts only the latest status in this pipeline."""
    return f"""
        SELECT CASE WHEN (
            SELECT status
            FROM opslakehouse.flag_job_etl
            WHERE job_name = '{upstream_dag_id}'
              AND cob_dt = DATE '{cob_dt}'
              AND pipeline_run_id = '{pipeline_run_id}'
            ORDER BY id DESC
            LIMIT 1
        ) = 'S' THEN 1 END
    """


def _record_dag_failure(context, *, dag_id: str, layer: str) -> None:
    """DAG-level callback that records a failed run with its real identity."""
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    logical_date = context.get("logical_date")
    cob_dt = conf.get("cob_dt") or (logical_date.date().isoformat() if logical_date else None)
    dag_run_id = dag_run.run_id if dag_run else context.get("run_id", "unknown")
    pipeline_run_id = conf.get("pipeline_run_id") or dag_run_id
    task_instance = context.get("task_instance")
    try_number = getattr(task_instance, "try_number", 1)
    error_detail = str(context.get("exception") or context.get("reason") or "DAG failed")[:4000]

    hook = PostgresHook(postgres_conn_id=POSTGRES_ETL_CONN_ID)
    hook.run(
        _FLAG_SQL_FAILURE,
        parameters={
            "dag_id": dag_id,
            "layer": layer,
            "cob_dt": cob_dt,
            "dag_run_id": dag_run_id,
            "pipeline_run_id": pipeline_run_id,
            "try_number": try_number,
            "error_detail": error_detail,
        },
    )


def make_failure_callback(dag_id: str, layer: str):
    """Build a serializable DAG failure callback for one DAG/layer."""
    return partial(_record_dag_failure, dag_id=dag_id, layer=layer)
