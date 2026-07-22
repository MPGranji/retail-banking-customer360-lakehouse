"""Static contracts for Customer 360 Gold correctness and current/history semantics."""

import copy
import sys
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "code_etl" / "gold"
sys.path.insert(0, str(GOLD_DIR / "base_job"))

from gold_job import validate_config


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class GoldProjectContractTest(unittest.TestCase):
    def test_every_gold_config_declares_a_valid_write_strategy(self):
        configs = sorted(GOLD_DIR.glob("**/*.yml"))
        self.assertGreaterEqual(len(configs), 11)

        for path in configs:
            with self.subTest(config=path.name):
                config = _load(path)
                validate_config(config)
                strategy = config["write"]["strategy"]
                if config["target"]["table"] == "mart_customer_360":
                    self.assertEqual(strategy, "overwrite_all")
                else:
                    self.assertEqual(strategy, "overwrite_partitions")

    def test_invalid_or_missing_write_strategy_fails_fast(self):
        config = _load(GOLD_DIR / "mart360" / "customer_360.yml")

        missing = copy.deepcopy(config)
        missing.pop("write")
        with self.assertRaisesRegex(ValueError, "write"):
            validate_config(missing)

        invalid = copy.deepcopy(config)
        invalid["write"]["strategy"] = "append"
        with self.assertRaisesRegex(ValueError, "Chiến lược ghi"):
            validate_config(invalid)

    def test_customer_360_current_reads_only_the_requested_history_snapshot(self):
        current = _load(GOLD_DIR / "mart360" / "customer_360.yml")
        history = _load(GOLD_DIR / "mart360" / "customer_360_history.yml")

        self.assertEqual(current["source"]["tables"], ["gold.mart_customer_360_history"])
        self.assertIn("FROM lakehouse.gold.mart_customer_360_history", current["sql"])
        self.assertIn("WHERE cob_dt = DATE '{{ cob_dt }}'", current["sql"])
        self.assertNotIn("lakehouse.silver", current["sql"])
        self.assertEqual(history["target"]["table"], "mart_customer_360_history")
        self.assertIn("BETWEEN effective_from AND effective_to", history["sql"])

    def test_fanout_prone_sources_are_aggregated_before_join(self):
        expected_ctes = {
            "customer_balance_summary.yml": ("account_agg", "deposit_agg", "loan_agg"),
            "customer_card_summary.yml": ("card_agg", "transaction_agg"),
            "customer_product_summary.yml": ("account_agg", "deposit_agg", "loan_agg", "card_agg"),
            "rfm_segment.yml": ("account_rfm", "card_rfm"),
            "churn_prediction.yml": ("account_activity", "card_activity"),
            "cross_sell_segment.yml": ("card_holding", "deposit_holding", "loan_holding"),
        }

        for filename, ctes in expected_ctes.items():
            subdir = "mart360" if filename.startswith("customer_") else "segmentation"
            sql = _load(GOLD_DIR / subdir / filename)["sql"].lower()
            with self.subTest(config=filename):
                for cte in ctes:
                    self.assertIn(f"{cte.lower()} as (", sql)

    def test_rfm_direction_dictionary_and_campaign_date_joins_are_consistent(self):
        rfm_sql = _load(GOLD_DIR / "segmentation" / "rfm_segment.yml")["sql"]
        mart_sql = _load(GOLD_DIR / "mart360" / "customer_360_history.yml")["sql"]
        campaign_sql = _load(GOLD_DIR / "segmentation" / "campaign_target.yml")["sql"]
        campaign_config = _load(GOLD_DIR / "segmentation" / "campaign_target.yml")

        self.assertIn("ORDER BY recency_days DESC", rfm_sql)
        self.assertIn("ORDER BY frequency ASC", rfm_sql)
        self.assertIn("ORDER BY monetary ASC", rfm_sql)
        self.assertIn("ORDER BY DATEDIFF", mart_sql)
        self.assertIn("ORDER BY rfm_frequency ASC", mart_sql)
        self.assertIn("ORDER BY rfm_monetary ASC", mart_sql)

        for sql in (rfm_sql, mart_sql):
            for segment in (
                "Champions",
                "Loyal Customers",
                "Potential Loyalists",
                "New Customers",
                "At Risk",
                "Hibernating",
                "Lost",
            ):
                self.assertIn(f"'{segment}'", sql)

        for join in ("r.cob_dt = ch.cob_dt", "r.cob_dt = cs.cob_dt", "r.cob_dt = m.cob_dt"):
            self.assertIn(join, campaign_sql)
        self.assertIn("gold.mart_customer_360_history", campaign_config["source"]["tables"])
        self.assertIn("lakehouse.gold.mart_customer_360_history", campaign_sql)

    def test_ddl_dag_and_trino_templates_follow_current_history_contract(self):
        for path in (
            PROJECT_ROOT / "ddl" / "Lakehouse" / "Gold" / "01_create_gold_tables.sql",
            PROJECT_ROOT / "code_etl" / "shared" / "ops" / "reset_iceberg_tables.py",
        ):
            text = path.read_text(encoding="utf-8")
            current_start = text.index("lakehouse.gold.mart_customer_360 (")
            history_start = text.index("lakehouse.gold.mart_customer_360_history (")
            current_block = text[current_start:history_start]
            history_block = text[history_start:history_start + 2200]
            self.assertNotIn("PARTITIONED BY (days(cob_dt))", current_block)
            self.assertIn("PARTITIONED BY (days(cob_dt))", history_block)

        dag_text = (PROJECT_ROOT / "airflow" / "dags" / "gold" / "gold_mart360_dag.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("PROCESSING_DATE_TEMPLATE", dag_text)
        self.assertIn("processing_run_params()", dag_text)
        self.assertIn("run_history >> run_current", dag_text)
        self.assertIn('customer_360_history.yml', dag_text)

        for path in sorted((PROJECT_ROOT / "sql_templates" / "trino").glob("0[1-6]_*.sql")):
            with self.subTest(template=path.name):
                self.assertNotIn("DATE '2025-12-31'", path.read_text(encoding="utf-8"))

    def test_campaign_target_exposes_explainable_nbo_contract(self):
        config = _load(GOLD_DIR / "segmentation" / "campaign_target.yml")
        sql = config["sql"]

        for column in (
            "cross_sell_score",
            "recommended_product",
            "recommendation_reason",
            "campaign_priority",
            "contact_eligible_flag",
            "suppression_reason",
        ):
            self.assertIn(column, sql)
        for score in ("THEN 30", "THEN 25", "THEN 20", "THEN 15", "THEN 10"):
            self.assertIn(score, sql)
        self.assertIn("GREATEST(0, LEAST(100", sql)
        self.assertIn("silver.fact_crm_interaction", config["source"]["tables"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
