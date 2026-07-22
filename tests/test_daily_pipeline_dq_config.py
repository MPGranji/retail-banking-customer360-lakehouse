"""Static contracts for daily orchestration, fact loading and Data Quality."""

import copy
import sys
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_DIR = PROJECT_ROOT / "code_etl" / "silver"
OPS_DIR = PROJECT_ROOT / "code_etl" / "shared" / "ops"
sys.path.insert(0, str(SILVER_DIR / "base_job"))
sys.path.insert(0, str(OPS_DIR))

from fact_txn import validate_config as validate_fact_config
from data_quality import raise_for_critical_failures, validate_config as validate_dq_config


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class DailyPipelineContractTest(unittest.TestCase):
    def test_daily_facts_are_date_scoped_and_asof(self):
        event_columns = {
            "fact_txn_account.yml": "txn_date",
            "fact_card_txn.yml": "txn_date",
            "fact_crm_interaction.yml": "interaction_date",
        }
        for name, event_column in event_columns.items():
            with self.subTest(config=name):
                config = load_yaml(SILVER_DIR / name)
                validate_fact_config(config)
                self.assertEqual(config["load"]["mode"], "daily")
                self.assertIn("WHERE", config["sql"])
                self.assertIn("cob_dt = DATE '{{ cob_dt }}'", config["sql"])
                self.assertIn(f"CAST({name.startswith('fact_crm') and 'i' or 't'}.{event_column} AS DATE)", config["sql"])
                self.assertIn("BETWEEN c.effective_from AND c.effective_to", config["sql"])

    def test_initial_facts_use_explicit_bootstrap_rule(self):
        configs = sorted((SILVER_DIR / "initial").glob("*.yml"))
        self.assertEqual(len(configs), 3)
        for path in configs:
            with self.subTest(config=path.name):
                config = load_yaml(path)
                validate_fact_config(config)
                self.assertEqual(config["load"]["mode"], "initial")
                self.assertEqual(
                    config["load"]["bootstrap_dimension_rule"],
                    "earliest_known_version_before_first_effective_date",
                )
                self.assertIn("first_effective_from", config["sql"])

    def test_invalid_fact_mode_fails(self):
        config = load_yaml(SILVER_DIR / "fact_card_txn.yml")
        invalid = copy.deepcopy(config)
        invalid["load"]["mode"] = "full"
        with self.assertRaisesRegex(ValueError, "load.mode"):
            validate_fact_config(invalid)

    def test_flags_are_run_aware_and_support_failure(self):
        text = (PROJECT_ROOT / "airflow" / "plugins" / "etl_flag.py").read_text(encoding="utf-8")
        for token in ("pipeline_run_id", "dag_run_id", "'F'", "ORDER BY id DESC"):
            self.assertIn(token, text)
        self.assertIn("make_failure_callback", text)

        for path in (
            PROJECT_ROOT / "docker" / "init_postgres" / "02_ddl_ops.sql",
            PROJECT_ROOT / "ddl" / "Source" / "init_postgres" / "02_ddl_ops.sql",
        ):
            ddl = path.read_text(encoding="utf-8")
            self.assertIn("pipeline_run_id", ddl)
            self.assertIn("dag_run_id", ddl)
            self.assertIn("('R', 'S', 'F')", ddl)
            self.assertIn("dq_check_result", ddl)

    def test_master_dag_has_full_daily_dependency_chain(self):
        text = (PROJECT_ROOT / "airflow" / "dags" / "lakehouse_daily_pipeline_dag.py").read_text(
            encoding="utf-8"
        )
        for dag_id in (
            "bronze_core_banking_dag",
            "bronze_card_crm_dag",
            "silver_all_dag",
            "gold_mart360_dag",
            "gold_time_analytics_dag",
            "gold_segmentation_dag",
            "ops_pii_masking_daily_dag",
            "ops_dq_daily_dag",
        ):
            self.assertIn(dag_id, text)
        self.assertIn("wait_for_completion=True", text)
        self.assertIn('schedule_interval="0 2 * * *"', text)

    def test_all_daily_child_dags_use_run_templates(self):
        relative_paths = (
            "bronze/bronze_core_banking_dag.py",
            "bronze/bronze_card_crm_dag.py",
            "silver/silver_all_dag.py",
            "gold/gold_mart360_dag.py",
            "gold/gold_segmentation_dag.py",
            "gold/gold_time_analytics_dag.py",
            "ops/ops_pii_masking_daily_dag.py",
            "ops/ops_dq_daily_dag.py",
        )
        for relative in relative_paths:
            with self.subTest(dag=relative):
                text = (PROJECT_ROOT / "airflow" / "dags" / relative).read_text(encoding="utf-8")
                self.assertIn("PROCESSING_DATE_TEMPLATE", text)
                self.assertIn("make_failure_callback", text)
                self.assertNotIn("{{ params.cob_dt }}", text)


class DataQualityContractTest(unittest.TestCase):
    def test_dq_config_covers_required_controls(self):
        config = load_yaml(OPS_DIR / "dq_checks.yml")
        validate_dq_config(config)
        self.assertGreaterEqual(len(config["checks"]), 12)
        names = {check["name"] for check in config["checks"]}
        for required in (
            "mart_customer_360_current_contract",
            "scd2_single_current_row",
            "scd2_no_effective_range_overlap",
            "fact_surrogate_keys_not_orphan",
            "rfm_scores_in_valid_range",
            "campaign_unique_customer_date",
            "nbo_contract_is_valid",
            "aum_reconciles_to_independent_sources",
            "masked_mart_has_no_raw_pii_columns",
        ):
            self.assertIn(required, names)

    def test_critical_failure_raises_but_warning_does_not(self):
        warning = [{"check_name": "warning", "severity": "warning", "passed": False}]
        raise_for_critical_failures(warning)

        critical = [{"check_name": "critical", "severity": "critical", "passed": False}]
        with self.assertRaisesRegex(RuntimeError, "critical"):
            raise_for_critical_failures(critical)


if __name__ == "__main__":
    unittest.main(verbosity=2)
