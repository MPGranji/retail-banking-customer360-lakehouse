"""Static contract tests for the four SCD2 dimensions."""

import copy
import re
import sys
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_DIR = PROJECT_ROOT / "code_etl" / "silver"
sys.path.insert(0, str(SILVER_DIR / "base_job"))

from scd_type2 import validate_config


SCD2_DIMENSIONS = {
    "dim_branch": ("branch_code", "branch_sk"),
    "dim_product": ("product_code", "product_sk"),
    "dim_customer": ("customer_id", "customer_sk"),
    "dim_account": ("account_id", "account_sk"),
}


def _load_config(table: str) -> dict:
    return yaml.safe_load((SILVER_DIR / f"{table}.yml").read_text(encoding="utf-8"))


class Scd2ConfigContractTest(unittest.TestCase):
    def test_all_required_dimensions_have_a_valid_contract(self):
        for table, (business_key, sk_column) in SCD2_DIMENSIONS.items():
            with self.subTest(table=table):
                config = _load_config(table)
                validate_config(config)
                self.assertEqual(config["job"]["type"], "scd_type2")
                self.assertEqual(config["business_key"], [business_key])
                self.assertEqual(config["scd"]["sk_column"], sk_column)
                self.assertTrue(config["scd"]["detect_deletes"])
                self.assertEqual(config["source"]["snapshot_mode"], "full")
                self.assertNotIn("last_updated", config["tracked_columns"])
                self.assertNotIn(business_key, config["tracked_columns"])

                selected_sql = config["sql"].lower()
                for column in config["business_key"] + config["tracked_columns"]:
                    self.assertRegex(selected_sql, rf"\b{re.escape(column.lower())}\b")

    def test_empty_tracked_columns_fail(self):
        config = copy.deepcopy(_load_config("dim_customer"))
        config["tracked_columns"] = []
        with self.assertRaisesRegex(ValueError, "tracked_columns"):
            validate_config(config)

    def test_delete_detection_requires_full_snapshot(self):
        config = copy.deepcopy(_load_config("dim_customer"))
        config["source"]["snapshot_mode"] = "incremental"
        with self.assertRaisesRegex(ValueError, "snapshot_mode=full"):
            validate_config(config)

    def test_silver_dag_uses_scd2_engine_for_all_four_dimensions(self):
        dag_text = (PROJECT_ROOT / "airflow" / "dags" / "silver" / "silver_all_dag.py").read_text(
            encoding="utf-8"
        )
        for table in SCD2_DIMENSIONS:
            self.assertIn(f'("{table}",', dag_text)
            self.assertRegex(
                dag_text,
                rf'\("{table}",\s+"scd_type2\.py",\s+"{table}\.yml"\)',
            )

    def test_daily_dags_use_the_same_manual_cob_date_parameter(self):
        dag_paths = [
            PROJECT_ROOT / "airflow" / "dags" / "bronze" / "bronze_core_banking_dag.py",
            PROJECT_ROOT / "airflow" / "dags" / "bronze" / "bronze_card_crm_dag.py",
            PROJECT_ROOT / "airflow" / "dags" / "silver" / "silver_all_dag.py",
        ]
        for path in dag_paths:
            with self.subTest(dag=path.name):
                dag_text = path.read_text(encoding="utf-8")
                self.assertIn("PROCESSING_DATE_TEMPLATE", dag_text)
                self.assertIn("processing_run_params()", dag_text)
                self.assertNotIn("{{ params.cob_dt }}", dag_text)
                self.assertNotIn("DEFAULT_COB_DT", dag_text)

        silver_dag = dag_paths[-1].read_text(encoding="utf-8")
        self.assertNotIn('"bronze_initial_dag"', silver_dag)

    def test_canonical_and_runtime_ddl_contain_scd2_metadata(self):
        canonical = (PROJECT_ROOT / "ddl" / "Lakehouse" / "Silver" / "01_create_silver_tables.sql").read_text(
            encoding="utf-8"
        )
        runtime = (
            PROJECT_ROOT / "code_etl" / "shared" / "ops" / "reset_iceberg_tables.py"
        ).read_text(encoding="utf-8")

        for table, (_, sk_column) in SCD2_DIMENSIONS.items():
            for source in (canonical, runtime):
                with self.subTest(table=table, source="canonical" if source is canonical else "runtime"):
                    start = source.find(f"lakehouse.silver.{table} (")
                    self.assertGreaterEqual(start, 0)
                    block = source[start : start + 1800]
                    for column in ("effective_from", "effective_to", "is_current", sk_column):
                        self.assertRegex(block, rf"\b{column}\b")
                    self.assertIn("PARTITIONED BY (is_current)", block)


if __name__ == "__main__":
    unittest.main(verbosity=2)
