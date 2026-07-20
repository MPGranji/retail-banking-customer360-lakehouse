"""
Bronze ingestion DAG — core_banking domain (Oracle).
Tasks discovered dynamically from code_etl/bronze/core_banking/*.yml.
Cờ ghi theo DAG: dag_start(R) → ingest_all → dag_end(S).
"""

import yaml
from pathlib import Path
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
import pendulum

from jdbc_conn_utils import jdbc_jinja_args
from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID             = "bronze_core_banking_dag"
ETL_PATH           = Variable.get("ETL_PATH", default_var="/opt/project/code_etl")
SPARK_APPLICATION  = f"{ETL_PATH}/bronze/base_job/ingestion_jdbc.py"
CONFIG_DIR         = Path(ETL_PATH) / "bronze" / "core_banking"
CONN_ID            = "oracle-core"
DEFAULT_COB_DT     = "2025-12-31"
COB_DT             = "{{ params.cob_dt }}"

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

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Bronze ingestion — core_banking (Oracle)",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    params={
        "cob_dt": Param(DEFAULT_COB_DT, type="string", format="date"),
    },
    tags=["bronze", "core_banking", "oracle"],
)

conn_tmpl = jdbc_jinja_args(CONN_ID)

# ── Cờ DAG-level ──────────────────────────────────────────────────────────────
dag_start = make_start_flag_task(
    "dag_start", DAG_ID, "bronze", dag, cob_dt=COB_DT
)

with TaskGroup("ingest_all", dag=dag) as ingest_all:
    for config_file in CONFIG_DIR.glob("*.yml"):
        config     = yaml.safe_load(config_file.read_text(encoding="utf-8"))
        table_name = config["target"]["table"]
        remote_cfg = f"{CONFIG_DIR}/{config_file.name}"

        SparkSubmitOperator(
            task_id=f"ingest_{table_name}",
            application=SPARK_APPLICATION,
            conn_id="spark_default",
            conf=SPARK_CONF,
            application_args=[
                "--config",      remote_cfg,
                "--cob_dt",      COB_DT,
                "--jdbc_url",    conn_tmpl["jdbc_url"],
                "--db_user",     conn_tmpl["db_user"],
                "--db_password", conn_tmpl["db_password"],
            ],
            verbose=True,
            dag=dag,
        )

dag_end = make_end_flag_task(
    "dag_end", DAG_ID, "bronze", dag, cob_dt=COB_DT
)

dag_start >> ingest_all >> dag_end
