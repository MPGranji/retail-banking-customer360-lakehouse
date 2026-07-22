"""
PII Masking Job
---------------
Tạo các Iceberg views (hoặc bảng masked) cho phép team Marketing/CRM
truy vấn dữ liệu mà không lộ thông tin cá nhân nhạy cảm.

Quy tắc masking:
  full_name  : "Nguyễn Văn An" → "Nguyễn V** A**" (giữ họ, che phần giữa)
  phone      : "0912345678" → "091****678"
  email      : "an.nguyen@gmail.com" → "a*****n@gmail.com"
  cccd       : SHA256(cccd + salt) — deterministic để vẫn có thể join
  address    : chỉ giữ city + district, ẩn số nhà/đường

Output schemas:
  sandbox.mart_customer_360_masked   (view/bảng từ gold.mart_customer_360)
  sandbox.dim_customer_masked        (view/bảng từ silver.dim_customer)
  sandbox.mart_customer_360_dashboard (history + NBO, 1 row/customer/cob_dt)

"""

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from spark.spark_session import get_spark_session

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("pii_masking")

# Salt cho hash PII — phải set PII_HASH_SALT env var trước khi chạy
_pii_salt_raw = os.environ.get("PII_HASH_SALT")
if not _pii_salt_raw:
    raise EnvironmentError("PII_HASH_SALT environment variable is required but not set")
PII_SALT = _pii_salt_raw
# Escape single-quotes để tránh SQL injection khi nhúng salt vào string literal SQL
PII_SALT_SQL = PII_SALT.replace("'", "''")

DASHBOARD_TABLE = "lakehouse.sandbox.mart_customer_360_dashboard"
DASHBOARD_DDL = f"""
    CREATE TABLE IF NOT EXISTS {DASHBOARD_TABLE} (
        customer_id                 BIGINT,
        customer_sk                 STRING,
        full_name_masked            STRING,
        age                         INT,
        age_group                   STRING,
        gender                      STRING,
        primary_branch_code         STRING,
        customer_segment            STRING,
        kyc_status                  STRING,
        register_date               DATE,
        total_accounts              INT,
        total_cards                 INT,
        total_loans                 INT,
        has_credit_card             INT,
        has_savings                 INT,
        has_loan                    INT,
        total_deposit_balance       DECIMAL(18,2),
        total_loan_outstanding      DECIMAL(18,2),
        aum_total                   DECIMAL(18,2),
        aum_bucket                  STRING,
        txn_count_30d               BIGINT,
        txn_amount_30d              DECIMAL(18,2),
        last_txn_date               TIMESTAMP,
        days_since_last_txn         INT,
        primary_channel             STRING,
        interaction_count_90d       BIGINT,
        last_interaction_date       TIMESTAMP,
        rfm_recency_score           INT,
        rfm_frequency_score         INT,
        rfm_monetary_score          INT,
        rfm_segment                 STRING,
        churn_flag                  INT,
        churn_risk                  STRING,
        is_churn_candidate          INT,
        cross_sell_credit_card_flag INT,
        no_credit_card              INT,
        no_deposit                  INT,
        no_loan                     INT,
        cross_sell_score            INT,
        recommended_product         STRING,
        recommendation_reason       STRING,
        campaign_priority           STRING,
        contact_eligible_flag       INT,
        suppression_reason          STRING,
        campaign_type               STRING,
        cob_dt                      DATE
    )
    USING iceberg
    PARTITIONED BY (days(cob_dt))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.target-file-size-bytes' = '134217728'
    )
"""


def _mask_name_udf() -> str:
    """Trả về biểu thức SQL mask tên tiếng Việt."""
    return """
        CASE
            WHEN full_name IS NULL THEN NULL
            WHEN size(split(full_name, ' ')) >= 3 THEN
                -- Họ + ký tự đầu đệm + '** ' + ký tự đầu tên + '**'
                concat(
                    split(full_name, ' ')[0], ' ',
                    substr(split(full_name, ' ')[1], 1, 1), '** ',
                    substr(split(full_name, ' ')[size(split(full_name, ' ')) - 1], 1, 1), '**'
                )
            WHEN size(split(full_name, ' ')) = 2 THEN
                concat(split(full_name, ' ')[0], ' ', substr(split(full_name, ' ')[1], 1, 1), '**')
            ELSE
                concat(substr(full_name, 1, 1), '**')
        END AS full_name_masked
    """


def create_masked_dim_customer(spark, cob_dt: str) -> None:
    logger.info("Creating sandbox.dim_customer_masked for cob_dt=%s", cob_dt)
    spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox")
    spark.sql(f"""
        CREATE OR REPLACE TABLE lakehouse.sandbox.dim_customer_masked
        USING iceberg
        PARTITIONED BY (is_current)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
            customer_sk,
            customer_id,
            -- PII masked
            {_mask_name_udf()},
            gender,
            -- age bucket thay vì ngày sinh cụ thể
            FLOOR(DATEDIFF(DATE '{cob_dt}', date_of_birth) / 365 / 10) * 10 AS age_group_decade,
            -- phone: giữ 3 đầu + 3 cuối
            CASE WHEN phone IS NULL THEN NULL
                 ELSE concat(substr(phone, 1, 3), '****', substr(phone, 8)) END AS phone_masked,
            -- email: che phần local trước @
            CASE WHEN email IS NULL THEN NULL
                 ELSE concat(
                     substr(split(email, '@')[0], 1, 1),
                     '*****',
                     substr(split(email, '@')[0], length(split(email, '@')[0])),
                     '@',
                     split(email, '@')[1]
                 ) END AS email_masked,
            -- cccd: SHA256 với salt (deterministic → vẫn join được)
            sha2(concat(cccd, '{PII_SALT_SQL}'), 256)  AS cccd_hash,
            -- địa chỉ: chỉ giữ city/district
            city,
            district,
            branch_code,
            customer_segment,
            kyc_status,
            register_date,
            is_active,
            effective_from,
            effective_to,
            is_current,
            last_updated
        FROM lakehouse.silver.dim_customer
    """)

def create_masked_mart_customer_360(spark, cob_dt: str) -> None:
    logger.info("Creating sandbox.mart_customer_360_masked for cob_dt=%s", cob_dt)
    spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox")
    spark.sql(f"""
        CREATE OR REPLACE TABLE lakehouse.sandbox.mart_customer_360_masked
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
            customer_id,
            customer_sk,
            -- full_name_masked đã được mask từ mart_customer_360
            full_name_masked,
            age,
            gender,
            primary_branch_code,
            customer_segment,
            kyc_status,
            register_date,
            -- Product Holding
            total_accounts,
            total_cards,
            total_loans,
            has_credit_card,
            has_savings,
            has_loan,
            -- Balance (giữ nguyên — team marketing cần AUM để segment)
            total_deposit_balance,
            total_loan_outstanding,
            aum_total,
            aum_bucket,
            -- Transaction Behavior
            txn_count_30d,
            txn_amount_30d,
            last_txn_date,
            days_since_last_txn,
            primary_channel,
            -- CRM
            interaction_count_90d,
            last_interaction_date,
            -- RFM
            rfm_recency_score,
            rfm_frequency_score,
            rfm_monetary_score,
            rfm_segment,
            -- Risk
            churn_flag,
            cross_sell_credit_card_flag,
            cob_dt
        FROM lakehouse.gold.mart_customer_360
    """)


def create_dashboard_mart(spark, cob_dt: str) -> None:
    """Publish the masked historical Customer 360 + NBO serving contract."""
    logger.info("Publishing %s for cob_dt=%s", DASHBOARD_TABLE, cob_dt)
    spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox")
    spark.sql(DASHBOARD_DDL)

    dashboard = spark.sql(f"""
        SELECT
            m.customer_id,
            m.customer_sk,
            m.full_name_masked,
            m.age,
            CASE
                WHEN m.age IS NULL THEN 'UNKNOWN'
                WHEN m.age < 25 THEN '18-24'
                WHEN m.age < 35 THEN '25-34'
                WHEN m.age < 45 THEN '35-44'
                WHEN m.age < 55 THEN '45-54'
                WHEN m.age < 65 THEN '55-64'
                ELSE '65+'
            END AS age_group,
            m.gender,
            m.primary_branch_code,
            m.customer_segment,
            m.kyc_status,
            m.register_date,
            m.total_accounts,
            m.total_cards,
            m.total_loans,
            m.has_credit_card,
            m.has_savings,
            m.has_loan,
            m.total_deposit_balance,
            m.total_loan_outstanding,
            m.aum_total,
            m.aum_bucket,
            m.txn_count_30d,
            m.txn_amount_30d,
            m.last_txn_date,
            m.days_since_last_txn,
            m.primary_channel,
            m.interaction_count_90d,
            m.last_interaction_date,
            m.rfm_recency_score,
            m.rfm_frequency_score,
            m.rfm_monetary_score,
            m.rfm_segment,
            m.churn_flag,
            c.churn_risk,
            c.is_churn_candidate,
            m.cross_sell_credit_card_flag,
            c.no_credit_card,
            c.no_deposit,
            c.no_loan,
            c.cross_sell_score,
            c.recommended_product,
            c.recommendation_reason,
            c.campaign_priority,
            c.contact_eligible_flag,
            c.suppression_reason,
            c.campaign_type,
            m.cob_dt
        FROM lakehouse.gold.mart_customer_360_history m
        LEFT JOIN lakehouse.gold.campaign_target c
          ON m.customer_id = c.customer_id
         AND m.cob_dt = c.cob_dt
        WHERE m.cob_dt = DATE '{cob_dt}'
    """)

    stats = dashboard.selectExpr(
        "COUNT(*) AS row_count",
        "COUNT(DISTINCT customer_id) AS unique_customers",
        "SUM(CASE WHEN cross_sell_score IS NULL OR cross_sell_score NOT BETWEEN 0 AND 100 "
        "OR recommended_product IS NULL OR TRIM(recommended_product) = '' "
        "OR recommendation_reason IS NULL OR TRIM(recommendation_reason) = '' "
        "OR campaign_priority IS NULL OR campaign_priority NOT IN ('HIGH', 'MEDIUM', 'LOW') "
        "OR contact_eligible_flag IS NULL OR contact_eligible_flag NOT IN (0, 1) "
        "OR (contact_eligible_flag = 1 AND suppression_reason IS NOT NULL) "
        "OR (contact_eligible_flag = 0 AND suppression_reason IS NULL) "
        "THEN 1 ELSE 0 END) AS invalid_nbo",
    ).first()
    if not stats or stats.row_count == 0:
        raise RuntimeError(f"Dashboard source is empty for cob_dt={cob_dt}")
    if stats.row_count != stats.unique_customers:
        raise RuntimeError(
            f"Dashboard grain violation for cob_dt={cob_dt}: "
            f"rows={stats.row_count}, unique_customers={stats.unique_customers}"
        )
    if stats.invalid_nbo:
        raise RuntimeError(
            f"Dashboard source has {stats.invalid_nbo} invalid NBO rows for cob_dt={cob_dt}"
        )

    dashboard.writeTo(DASHBOARD_TABLE).overwritePartitions()
    published_rows = spark.table(DASHBOARD_TABLE).where(f"cob_dt = DATE '{cob_dt}'").count()
    if published_rows != stats.row_count:
        raise RuntimeError(
            f"Dashboard publish validation failed for cob_dt={cob_dt}: "
            f"expected={stats.row_count}, actual={published_rows}"
        )
    logger.info("Published %d dashboard rows for cob_dt=%s", published_rows, cob_dt)

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PII Masking Job")
    parser.add_argument("--cob_dt", required=True, help="Ngày xử lý YYYY-MM-DD")
    parser.add_argument(
        "--target",
        choices=["dim_customer", "mart_360", "dashboard", "all"],
        default="all",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = get_spark_session("pii_masking")

    if args.target in ("dim_customer", "all"):
        create_masked_dim_customer(spark, args.cob_dt)

    if args.target in ("mart_360", "all"):
        create_masked_mart_customer_360(spark, args.cob_dt)

    if args.target in ("dashboard", "all"):
        create_dashboard_mart(spark, args.cob_dt)

    logger.info("PII masking completed for cob_dt=%s", args.cob_dt)


if __name__ == "__main__":
    main()
