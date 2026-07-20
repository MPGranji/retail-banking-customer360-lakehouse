"""
reset_iceberg_tables.py — DROP PURGE + CREATE cho từng tầng Iceberg.

DROP thứ tự: gold → silver → bronze   (tránh dependency conflict)
CREATE thứ tự: bronze → silver → gold

Tham số CLI:
    --layer  bronze | silver | gold | all
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from spark.spark_session import get_spark_session

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("reset_iceberg_tables")

# ─── DROP statements — thứ tự gold → silver → bronze ────────────────────────
# PURGE: xóa cả metadata trên catalog REST + đánh dấu data files để dọn trên MinIO
_DROP_GOLD = [
    "DROP TABLE IF EXISTS lakehouse.gold.campaign_target PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.cross_sell_segment PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.churn_prediction PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.rfm_segment PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.customer_card_summary PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.customer_product_summary PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.customer_transaction_summary PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.customer_balance_summary PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.mart_customer_360 PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.mart_customer_360_history PURGE",
    "DROP TABLE IF EXISTS lakehouse.gold.mart_branch_monthly_summary PURGE",
]

_DROP_SILVER = [
    "DROP TABLE IF EXISTS lakehouse.silver.fact_crm_interaction PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.fact_card_txn PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.fact_txn_account PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.dim_card PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.dim_loan PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.dim_deposit PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.dim_account PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.dim_customer PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.dim_product PURGE",
    "DROP TABLE IF EXISTS lakehouse.silver.dim_branch PURGE",
]

_DROP_BRONZE = [
    "DROP TABLE IF EXISTS lakehouse.bronze.crm_interaction PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.card_txn PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.card PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.core_txn_account PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.core_loan PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.core_deposit PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.core_account PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.core_customer PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.core_product PURGE",
    "DROP TABLE IF EXISTS lakehouse.bronze.core_branch PURGE",
]

# ─── CREATE statements — thứ tự bronze → silver → gold ───────────────────────

_CREATE_BRONZE = [
    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_branch (
    branch_code   STRING,
    branch_name   STRING,
    region        STRING,
    city          STRING,
    district      STRING,
    address       STRING,
    manager_name  STRING,
    open_date     DATE,
    status        STRING,
    last_updated  TIMESTAMP,
    cob_dt        DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_product (
    product_code   STRING,
    product_name   STRING,
    product_group  STRING,
    product_type   STRING,
    currency       STRING,
    is_active      INT,
    launch_date    DATE,
    last_updated   TIMESTAMP,
    cob_dt         DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_customer (
    customer_id       BIGINT,
    cccd              STRING,
    full_name         STRING,
    gender            STRING,
    date_of_birth     DATE,
    phone             STRING,
    email             STRING,
    address           STRING,
    city              STRING,
    district          STRING,
    branch_code       STRING,
    customer_segment  STRING,
    kyc_status        STRING,
    register_date     DATE,
    is_active         INT,
    last_updated      TIMESTAMP,
    cob_dt            DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_account (
    account_id    BIGINT,
    account_no    STRING,
    customer_id   BIGINT,
    product_code  STRING,
    branch_code   STRING,
    account_type  STRING,
    currency      STRING,
    balance       DECIMAL(18,2),
    open_date     DATE,
    close_date    DATE,
    status        STRING,
    last_updated  TIMESTAMP,
    cob_dt        DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_deposit (
    deposit_id       BIGINT,
    account_id       BIGINT,
    customer_id      BIGINT,
    product_code     STRING,
    principal_amount DECIMAL(18,2),
    interest_rate    DECIMAL(5,2),
    term_months      INT,
    open_date        DATE,
    maturity_date    DATE,
    status           STRING,
    last_updated     TIMESTAMP,
    cob_dt           DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_loan (
    loan_id              BIGINT,
    customer_id          BIGINT,
    product_code         STRING,
    branch_code          STRING,
    loan_amount          DECIMAL(18,2),
    outstanding_balance  DECIMAL(18,2),
    interest_rate        DECIMAL(5,2),
    term_months          INT,
    disbursement_date    DATE,
    maturity_date        DATE,
    loan_status          STRING,
    last_updated         TIMESTAMP,
    cob_dt               DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_txn_account (
    txn_id           BIGINT,
    account_id       BIGINT,
    customer_id      BIGINT,
    txn_date         TIMESTAMP,
    txn_amount       DECIMAL(18,2),
    txn_type         STRING,
    debit_credit     STRING,
    balance_after    DECIMAL(18,2),
    channel          STRING,
    description      STRING,
    counter_account  STRING,
    created_ts       TIMESTAMP,
    last_updated     TIMESTAMP,
    cob_dt           DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '268435456'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.card (
    card_id         BIGINT,
    card_no_masked  STRING,
    customer_id     BIGINT,
    account_id      BIGINT,
    product_code    STRING,
    card_type       STRING,
    card_brand      STRING,
    credit_limit    DECIMAL(18,2),
    issue_date      DATE,
    expiry_date     DATE,
    status          STRING,
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_txn (
    txn_id             BIGINT,
    card_id            BIGINT,
    customer_id        BIGINT,
    txn_date           TIMESTAMP,
    txn_amount         DECIMAL(18,2),
    txn_type           STRING,
    currency           STRING,
    merchant_name      STRING,
    merchant_category  STRING,
    channel            STRING,
    status             STRING,
    created_ts         TIMESTAMP,
    last_updated       TIMESTAMP,
    cob_dt             DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '268435456'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.bronze.crm_interaction (
    interaction_id    BIGINT,
    customer_id       BIGINT,
    interaction_date  TIMESTAMP,
    channel           STRING,
    direction         STRING,
    subject           STRING,
    category          STRING,
    status            STRING,
    assigned_to       STRING,
    created_ts        TIMESTAMP,
    last_updated      TIMESTAMP,
    cob_dt            DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",
]

_CREATE_SILVER = [
    """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_branch (
    branch_code   STRING,
    branch_name   STRING,
    region        STRING,
    city          STRING,
    district      STRING,
    address       STRING,
    manager_name  STRING,
    open_date     DATE,
    status        STRING,
    last_updated  TIMESTAMP,
    effective_from DATE,
    effective_to   DATE,
    is_current     INT,
    branch_sk      STRING
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_product (
    product_code   STRING,
    product_name   STRING,
    product_group  STRING,
    product_type   STRING,
    currency       STRING,
    is_active      INT,
    launch_date    DATE,
    last_updated   TIMESTAMP,
    effective_from DATE,
    effective_to   DATE,
    is_current     INT,
    product_sk     STRING
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_customer (
    customer_id       BIGINT,
    cccd              STRING,
    full_name         STRING,
    gender            STRING,
    date_of_birth     DATE,
    phone             STRING,
    email             STRING,
    address           STRING,
    city              STRING,
    district          STRING,
    branch_code       STRING,
    customer_segment  STRING,
    kyc_status        STRING,
    register_date     DATE,
    is_active         INT,
    last_updated      TIMESTAMP,
    effective_from    DATE,
    effective_to      DATE,
    is_current        INT,
    customer_sk       STRING
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_account (
    account_id     BIGINT,
    account_no     STRING,
    customer_id    BIGINT,
    product_code   STRING,
    branch_code    STRING,
    account_type   STRING,
    currency       STRING,
    balance        DECIMAL(18,2),
    open_date      DATE,
    close_date     DATE,
    status         STRING,
    last_updated   TIMESTAMP,
    effective_from DATE,
    effective_to   DATE,
    is_current     INT,
    account_sk     STRING
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_deposit (
    deposit_id       BIGINT,
    account_id       BIGINT,
    customer_id      BIGINT,
    product_code     STRING,
    principal_amount DECIMAL(18,2),
    interest_rate    DECIMAL(5,2),
    term_months      INT,
    open_date        DATE,
    maturity_date    DATE,
    status           STRING,
    last_updated     TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_loan (
    loan_id              BIGINT,
    customer_id          BIGINT,
    product_code         STRING,
    branch_code          STRING,
    loan_amount          DECIMAL(18,2),
    outstanding_balance  DECIMAL(18,2),
    interest_rate        DECIMAL(5,2),
    term_months          INT,
    disbursement_date    DATE,
    maturity_date        DATE,
    loan_status          STRING,
    last_updated         TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_card (
    card_id        BIGINT,
    card_no_masked STRING,
    customer_id    BIGINT,
    account_id     BIGINT,
    product_code   STRING,
    card_type      STRING,
    card_brand     STRING,
    credit_limit   DECIMAL(18,2),
    issue_date     DATE,
    expiry_date    DATE,
    status         STRING,
    last_updated   TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_txn_account (
    txn_id           BIGINT,
    account_id       BIGINT,
    account_sk       STRING,
    customer_id      BIGINT,
    customer_sk      STRING,
    txn_date         TIMESTAMP,
    txn_amount       DECIMAL(18,2),
    txn_type         STRING,
    debit_credit     STRING,
    balance_after    DECIMAL(18,2),
    channel          STRING,
    description      STRING,
    counter_account  STRING,
    created_ts       TIMESTAMP,
    cob_dt           DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '268435456'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_card_txn (
    txn_id             BIGINT,
    card_id            BIGINT,
    customer_id        BIGINT,
    customer_sk        STRING,
    txn_date           TIMESTAMP,
    txn_amount         DECIMAL(18,2),
    txn_type           STRING,
    currency           STRING,
    merchant_name      STRING,
    merchant_category  STRING,
    channel            STRING,
    status             STRING,
    created_ts         TIMESTAMP,
    cob_dt             DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '268435456'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_crm_interaction (
    interaction_id    BIGINT,
    customer_id       BIGINT,
    customer_sk       STRING,
    interaction_date  TIMESTAMP,
    channel           STRING,
    direction         STRING,
    subject           STRING,
    category          STRING,
    status            STRING,
    assigned_to       STRING,
    created_ts        TIMESTAMP,
    cob_dt            DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",
]

_CREATE_GOLD = [
    """CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_customer_360 (
    customer_id                 BIGINT,
    customer_sk                 STRING,
    full_name_masked            STRING,
    age                         INT,
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
    cross_sell_credit_card_flag INT,
    cob_dt                      DATE
)
USING iceberg
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_customer_360_history (
    customer_id                 BIGINT,
    customer_sk                 STRING,
    full_name_masked            STRING,
    age                         INT,
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
    cross_sell_credit_card_flag INT,
    cob_dt                      DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_balance_summary (
    customer_id              BIGINT,
    customer_sk              STRING,
    total_account_balance    DECIMAL(18,2),
    avg_account_balance      DECIMAL(18,2),
    total_deposit_principal  DECIMAL(18,2),
    total_loan_outstanding   DECIMAL(18,2),
    aum_total                DECIMAL(18,2),
    net_worth                DECIMAL(18,2),
    cob_dt                   DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_transaction_summary (
    customer_id              BIGINT,
    customer_sk              STRING,
    acct_txn_count_30d       BIGINT,
    acct_txn_amount_30d      DECIMAL(18,2),
    acct_credit_count_30d    BIGINT,
    acct_debit_count_30d     BIGINT,
    acct_credit_amount_30d   DECIMAL(18,2),
    acct_debit_amount_30d    DECIMAL(18,2),
    card_txn_count_30d       BIGINT,
    card_txn_amount_30d      DECIMAL(18,2),
    total_txn_count_30d      BIGINT,
    total_txn_amount_30d     DECIMAL(18,2),
    last_txn_date            TIMESTAMP,
    cob_dt                   DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_product_summary (
    customer_id          BIGINT,
    customer_sk          STRING,
    total_accounts       INT,
    cnt_casa_active      INT,
    cnt_td_active        INT,
    cnt_deposits         INT,
    cnt_deposits_active  INT,
    cnt_loans            INT,
    cnt_loans_active     INT,
    cnt_cards            INT,
    cnt_credit_cards     INT,
    cnt_debit_cards      INT,
    has_credit_card      INT,
    has_savings          INT,
    has_loan             INT,
    cob_dt               DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_card_summary (
    customer_id                   BIGINT,
    customer_sk                   STRING,
    total_cards                   INT,
    cnt_credit_active             INT,
    cnt_debit_active              INT,
    max_credit_limit              DECIMAL(18,2),
    total_card_txn_count_30d      BIGINT,
    total_card_txn_amount_30d     DECIMAL(18,2),
    avg_card_txn_amount_30d       DECIMAL(18,2),
    distinct_merchant_categories  INT,
    last_card_txn_date            TIMESTAMP,
    cob_dt                        DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.rfm_segment (
    customer_id   BIGINT,
    customer_sk   STRING,
    recency_days  INT,
    frequency     BIGINT,
    monetary      DECIMAL(18,2),
    r_score       INT,
    f_score       INT,
    m_score       INT,
    rfm_score     INT,
    rfm_segment   STRING,
    cob_dt        DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.churn_prediction (
    customer_id           BIGINT,
    customer_sk           STRING,
    txn_cnt_30d           BIGINT,
    txn_cnt_90d           BIGINT,
    txn_amt_30d           DECIMAL(18,2),
    txn_amt_90d           DECIMAL(18,2),
    days_since_last_txn   INT,
    churn_risk            STRING,
    is_churn_candidate    INT,
    cob_dt                DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.cross_sell_segment (
    customer_id          BIGINT,
    customer_sk          STRING,
    customer_segment     STRING,
    no_credit_card       INT,
    no_deposit           INT,
    no_loan              INT,
    primary_opportunity  STRING,
    cob_dt               DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_branch_monthly_summary (
    branch_code           STRING,
    branch_name           STRING,
    region                STRING,
    city                  STRING,
    txn_year              INT,
    txn_month             INT,
    txn_quarter           INT,
    active_customers      BIGINT,
    txn_count             BIGINT,
    total_txn_amount      DECIMAL(18,2),
    avg_txn_amount        DECIMAL(18,2),
    total_credit_amount   DECIMAL(18,2),
    total_debit_amount    DECIMAL(18,2),
    top_channel           STRING,
    cob_dt                DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",

    """CREATE TABLE IF NOT EXISTS lakehouse.gold.campaign_target (
    customer_id           BIGINT,
    customer_sk           STRING,
    rfm_segment           STRING,
    rfm_score             INT,
    recency_days          INT,
    frequency             BIGINT,
    monetary              DECIMAL(18,2),
    churn_risk            STRING,
    is_churn_candidate    INT,
    days_since_last_txn   INT,
    customer_segment      STRING,
    aum_total             DECIMAL(18,2),
    aum_bucket            STRING,
    primary_branch_code   STRING,
    primary_opportunity   STRING,
    no_credit_card        INT,
    no_deposit            INT,
    no_loan               INT,
    campaign_type         STRING,
    cob_dt                DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728'
)""",
]

# ─── Namespace ensure — chạy trước khi CREATE ────────────────────────────────
_ENSURE_NAMESPACES = [
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze",
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver",
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold",
]


def _run_statements(spark, statements: list[str], label: str) -> None:
    """Chạy tuần tự danh sách SQL, log từng bước. Fail-fast nếu có lỗi."""
    for i, sql in enumerate(statements, 1):
        preview = sql.strip().splitlines()[0]
        logger.info("[%s] %d/%d — %s", label, i, len(statements), preview)
        spark.sql(sql)
    logger.info("[%s] Hoàn thành %d statements.", label, len(statements))


def _build_drop_sequence(layer: str) -> list[str]:
    """Trả về danh sách DROP.
    Layer đơn lẻ: chỉ drop đúng layer đó (Iceberg không có FK cascade).
    all: gold → silver → bronze để tránh orphan references trong catalog.
    """
    if layer == "bronze":
        return _DROP_BRONZE
    if layer == "silver":
        return _DROP_SILVER
    if layer == "gold":
        return _DROP_GOLD
    # all — thứ tự an toàn: gold → silver → bronze
    return _DROP_GOLD + _DROP_SILVER + _DROP_BRONZE


def _build_create_sequence(layer: str) -> list[str]:
    """Trả về danh sách CREATE theo thứ tự phụ thuộc (bronze → silver → gold)."""
    if layer == "bronze":
        return _CREATE_BRONZE
    if layer == "silver":
        return _CREATE_SILVER
    if layer == "gold":
        return _CREATE_GOLD
    # all
    return _CREATE_BRONZE + _CREATE_SILVER + _CREATE_GOLD


def reset_layer(spark, layer: str) -> None:
    """Thực hiện DROP PURGE rồi CREATE cho layer (hoặc all layers)."""
    logger.info("=== RESET bắt đầu — layer=%s ===", layer)

    drop_stmts = _build_drop_sequence(layer)
    logger.info("DROP PURGE %d bảng ...", len(drop_stmts))
    _run_statements(spark, drop_stmts, "DROP")

    logger.info("Đảm bảo namespace tồn tại ...")
    _run_statements(spark, _ENSURE_NAMESPACES, "NAMESPACE")

    create_stmts = _build_create_sequence(layer)
    logger.info("CREATE %d bảng ...", len(create_stmts))
    _run_statements(spark, create_stmts, "CREATE")

    logger.info("=== RESET hoàn thành — layer=%s ===", layer)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DROP PURGE + CREATE Iceberg tables theo layer"
    )
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        required=True,
        help="Tầng cần reset: bronze | silver | gold | all",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = get_spark_session("reset_iceberg_tables")

    reset_layer(spark, args.layer)

    logger.info("Job hoàn thành thành công.")
    spark.stop()


if __name__ == "__main__":
    main()
