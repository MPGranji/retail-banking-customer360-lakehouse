"""Static contracts for the masked historical dashboard serving layer."""

import re
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE_NAME = "mart_customer_360_dashboard"
DASHBOARD_SCHEMA = (
    ("customer_id", "BIGINT"),
    ("customer_sk", "STRING"),
    ("full_name_masked", "STRING"),
    ("age", "INT"),
    ("age_group", "STRING"),
    ("gender", "STRING"),
    ("primary_branch_code", "STRING"),
    ("customer_segment", "STRING"),
    ("kyc_status", "STRING"),
    ("register_date", "DATE"),
    ("total_accounts", "INT"),
    ("total_cards", "INT"),
    ("total_loans", "INT"),
    ("has_credit_card", "INT"),
    ("has_savings", "INT"),
    ("has_loan", "INT"),
    ("total_deposit_balance", "DECIMAL(18,2)"),
    ("total_loan_outstanding", "DECIMAL(18,2)"),
    ("aum_total", "DECIMAL(18,2)"),
    ("aum_bucket", "STRING"),
    ("txn_count_30d", "BIGINT"),
    ("txn_amount_30d", "DECIMAL(18,2)"),
    ("last_txn_date", "TIMESTAMP"),
    ("days_since_last_txn", "INT"),
    ("primary_channel", "STRING"),
    ("interaction_count_90d", "BIGINT"),
    ("last_interaction_date", "TIMESTAMP"),
    ("rfm_recency_score", "INT"),
    ("rfm_frequency_score", "INT"),
    ("rfm_monetary_score", "INT"),
    ("rfm_segment", "STRING"),
    ("churn_flag", "INT"),
    ("churn_risk", "STRING"),
    ("is_churn_candidate", "INT"),
    ("cross_sell_credit_card_flag", "INT"),
    ("no_credit_card", "INT"),
    ("no_deposit", "INT"),
    ("no_loan", "INT"),
    ("cross_sell_score", "INT"),
    ("recommended_product", "STRING"),
    ("recommendation_reason", "STRING"),
    ("campaign_priority", "STRING"),
    ("contact_eligible_flag", "INT"),
    ("suppression_reason", "STRING"),
    ("campaign_type", "STRING"),
    ("cob_dt", "DATE"),
)
RAW_PII_COLUMNS = {"full_name", "phone", "email", "cccd", "address"}


def _dashboard_schema(text: str) -> tuple[tuple[str, str], ...]:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS\s+"
        rf"(?:lakehouse\.sandbox\.{TABLE_NAME}|\{{DASHBOARD_TABLE\}})\s*"
        r"\((.*?)\)\s*(?:USING|WITH)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise AssertionError(f"Không tìm thấy DDL của {TABLE_NAME}")

    aliases = {"VARCHAR": "STRING", "INTEGER": "INT", "TIMESTAMP(6)": "TIMESTAMP"}
    columns = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        name, data_type = line.split(None, 1)
        normalized_type = re.sub(r"\s+", "", data_type).upper()
        columns.append((name.lower(), aliases.get(normalized_type, normalized_type)))
    return tuple(columns)


class DashboardServingContractTest(unittest.TestCase):
    def test_schema_contract_is_synced_across_ddl_reset_and_migration(self):
        paths = (
            PROJECT_ROOT / "ddl" / "Lakehouse" / "Sandbox" / "01_create_sandbox_tables.sql",
            PROJECT_ROOT / "code_etl" / "shared" / "ops" / "pii_masking.py",
            PROJECT_ROOT / "code_etl" / "shared" / "ops" / "reset_iceberg_tables.py",
            PROJECT_ROOT / "docker" / "migrations" / "003_dashboard_serving_table.sql",
        )
        for path in paths:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path):
                self.assertIn(TABLE_NAME, text)
                schema = _dashboard_schema(text)
                self.assertEqual(schema, DASHBOARD_SCHEMA)
                self.assertTrue(RAW_PII_COLUMNS.isdisjoint(dict(schema)))
                if path.suffix == ".sql" and "migrations" in path.parts:
                    self.assertIn("partitioning = ARRAY['day(cob_dt)']", text)
                else:
                    self.assertIn("PARTITIONED BY (days(cob_dt))", text)

    def test_publisher_reads_history_and_nbo_then_overwrites_one_partition(self):
        text = (
            PROJECT_ROOT / "code_etl" / "shared" / "ops" / "pii_masking.py"
        ).read_text(encoding="utf-8")
        for expected in (
            "lakehouse.gold.mart_customer_360_history",
            "lakehouse.gold.campaign_target",
            "m.customer_id = c.customer_id",
            "m.cob_dt = c.cob_dt",
            "overwritePartitions()",
            'choices=["dim_customer", "mart_360", "dashboard", "all"]',
            "campaign_priority IS NULL",
            "contact_eligible_flag IS NULL",
        ):
            self.assertIn(expected, text)

        selected_sql = text[text.index("def create_dashboard_mart"):]
        for forbidden in ("m.phone", "m.email", "m.cccd", "m.address", "m.full_name,"):
            self.assertNotIn(forbidden, selected_sql)

    def test_masking_dag_waits_for_segmentation_and_publishes_dashboard(self):
        text = (
            PROJECT_ROOT / "airflow" / "dags" / "ops" / "ops_pii_masking_daily_dag.py"
        ).read_text(encoding="utf-8")
        for expected in (
            'latest_success_sql("gold_segmentation_dag", COB_DT, PIPELINE_RUN_ID)',
            'task_id="publish_dashboard_mart"',
            '"--target", "dashboard"',
            "wait_gold_segmentation",
        ):
            self.assertIn(expected, text)

    def test_dashboard_dq_suite_has_grain_population_nbo_and_pii_controls(self):
        config = yaml.safe_load(
            (PROJECT_ROOT / "code_etl" / "shared" / "ops" / "dq_checks.yml").read_text(
                encoding="utf-8"
            )
        )
        checks = {check["name"]: check for check in config["checks"]}
        required = {
            "dashboard_customer_date_is_unique",
            "dashboard_population_matches_gold_history",
            "dashboard_nbo_contract_is_valid",
            "dashboard_has_no_raw_pii_columns",
        }
        self.assertTrue(required.issubset(checks))
        self.assertGreaterEqual(len(checks), 19)
        for name in required:
            self.assertEqual(checks[name]["severity"], "critical")
            self.assertEqual(checks[name]["table"], TABLE_NAME)

        pii = checks["dashboard_has_no_raw_pii_columns"]
        self.assertEqual(set(pii["forbidden_columns"]), RAW_PII_COLUMNS)
        nbo_sql = checks["dashboard_nbo_contract_is_valid"]["sql"]
        self.assertIn("campaign_priority IS NULL", nbo_sql)
        self.assertIn("contact_eligible_flag IS NULL", nbo_sql)

    def test_acceptance_assets_reference_dashboard_contract(self):
        sql = (
            PROJECT_ROOT / "sql_templates" / "trino" / "11_dashboard_serving_acceptance.sql"
        ).read_text(encoding="utf-8")
        notebook = (
            PROJECT_ROOT / "notebooks" / "06_dashboard_data_acceptance.ipynb"
        ).read_text(encoding="utf-8")
        for text in (sql, notebook):
            self.assertIn("mart_customer_360_dashboard", text)
            self.assertIn("recommended_product", text)
            self.assertIn("contact_eligible_flag", text)
            self.assertIn("campaign_priority IS NULL", text)
            self.assertIn("contact_eligible_flag IS NULL", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
