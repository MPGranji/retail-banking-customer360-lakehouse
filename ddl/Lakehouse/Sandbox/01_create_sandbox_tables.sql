-- =====================================================
-- DDL Sandbox serving layer — Apache Iceberg
-- Dashboard grain: 1 row/customer/cob_dt
-- No raw phone/email/CCCD/address fields are allowed.
-- =====================================================

CREATE TABLE IF NOT EXISTS lakehouse.sandbox.mart_customer_360_dashboard (
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
);
