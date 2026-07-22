import json
import os
from pathlib import Path
from urllib.parse import quote


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise RuntimeError(f"{name} must be configured")
    return value


def metric(name: str, expression: str, verbose_name: str | None = None) -> dict:
    return {
        "metric_name": name,
        "verbose_name": verbose_name or name,
        "expression": expression,
    }


trino_user = env("SUPERSET_TRINO_USER", "marketing")
trino_password = quote(env("SUPERSET_TRINO_PASSWORD"), safe="")
trino_host = env("SUPERSET_TRINO_HOST", "trino")
trino_port = env("SUPERSET_TRINO_PORT", "8443")
trino_catalog = env("SUPERSET_TRINO_CATALOG", "lakehouse")
trino_schema = env("SUPERSET_TRINO_SCHEMA", "sandbox")
verify_ssl = os.environ.get("SUPERSET_TRINO_VERIFY_SSL", "false").lower() == "true"

sqlalchemy_uri = (
    f"trino://{quote(trino_user, safe='')}:{trino_password}"
    f"@{trino_host}:{trino_port}/{trino_catalog}/{trino_schema}"
)

columns = [
    ("customer_id", "BIGINT", False),
    ("customer_sk", "STRING", False),
    ("full_name_masked", "STRING", False),
    ("age", "INT", False),
    ("age_group", "STRING", False),
    ("gender", "STRING", False),
    ("primary_branch_code", "STRING", False),
    ("customer_segment", "STRING", False),
    ("kyc_status", "STRING", False),
    ("register_date", "DATE", True),
    ("total_accounts", "INT", False),
    ("total_cards", "INT", False),
    ("total_loans", "INT", False),
    ("has_credit_card", "INT", False),
    ("has_savings", "INT", False),
    ("has_loan", "INT", False),
    ("total_deposit_balance", "DECIMAL", False),
    ("total_loan_outstanding", "DECIMAL", False),
    ("aum_total", "DECIMAL", False),
    ("aum_bucket", "STRING", False),
    ("txn_count_30d", "BIGINT", False),
    ("txn_amount_30d", "DECIMAL", False),
    ("last_txn_date", "TIMESTAMP", True),
    ("days_since_last_txn", "INT", False),
    ("primary_channel", "STRING", False),
    ("interaction_count_90d", "BIGINT", False),
    ("last_interaction_date", "TIMESTAMP", True),
    ("rfm_recency_score", "INT", False),
    ("rfm_frequency_score", "INT", False),
    ("rfm_monetary_score", "INT", False),
    ("rfm_segment", "STRING", False),
    ("churn_flag", "INT", False),
    ("churn_risk", "STRING", False),
    ("is_churn_candidate", "INT", False),
    ("cross_sell_credit_card_flag", "INT", False),
    ("no_credit_card", "INT", False),
    ("no_deposit", "INT", False),
    ("no_loan", "INT", False),
    ("cross_sell_score", "INT", False),
    ("recommended_product", "STRING", False),
    ("recommendation_reason", "STRING", False),
    ("campaign_priority", "STRING", False),
    ("contact_eligible_flag", "INT", False),
    ("suppression_reason", "STRING", False),
    ("campaign_type", "STRING", False),
    ("cob_dt", "DATE", True),
]

payload = {
    "databases": [
        {
            "database_name": "Customer 360 Lakehouse - Trino Sandbox",
            "sqlalchemy_uri": sqlalchemy_uri,
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
            "extra": json.dumps(
                {
                    "engine_params": {
                        "connect_args": {
                            "http_scheme": "https",
                            "verify": verify_ssl,
                        }
                    },
                    "metadata_params": {},
                    "schemas_allowed_for_file_upload": [],
                }
            ),
            "tables": [
                {
                    "table_name": "mart_customer_360_dashboard",
                    "schema": trino_schema,
                    "main_dttm_col": "cob_dt",
                    "columns": [
                        {
                            "column_name": name,
                            "type": col_type,
                            "is_dttm": is_dttm,
                        }
                        for name, col_type, is_dttm in columns
                    ],
                    "metrics": [
                        metric("customer_count", "COUNT(DISTINCT customer_id)", "Customer count"),
                        metric("total_aum", "SUM(aum_total)", "Total AUM"),
                        metric("avg_aum", "AVG(aum_total)", "Average AUM"),
                        metric("active_30d_customers", "SUM(CASE WHEN txn_count_30d > 0 THEN 1 ELSE 0 END)", "Active 30d customers"),
                        metric("eligible_customers", "SUM(contact_eligible_flag)", "Eligible campaign customers"),
                        metric("avg_cross_sell_score", "AVG(cross_sell_score)", "Average cross-sell score"),
                        metric("churn_candidates", "SUM(is_churn_candidate)", "Churn candidates"),
                    ],
                }
            ],
        }
    ]
}

output_path = Path(env("SUPERSET_DATASOURCE_FILE", "/app/superset_home/customer360_datasources.yaml"))
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
print(output_path)
