-- Additive Trino migration for the historical masked dashboard serving mart.
-- The daily masking job also creates this table when it does not yet exist.

CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox;

CREATE TABLE IF NOT EXISTS lakehouse.sandbox.mart_customer_360_dashboard (
    customer_id                 BIGINT,
    customer_sk                 VARCHAR,
    full_name_masked            VARCHAR,
    age                         INTEGER,
    age_group                   VARCHAR,
    gender                      VARCHAR,
    primary_branch_code         VARCHAR,
    customer_segment            VARCHAR,
    kyc_status                  VARCHAR,
    register_date               DATE,
    total_accounts              INTEGER,
    total_cards                 INTEGER,
    total_loans                 INTEGER,
    has_credit_card             INTEGER,
    has_savings                 INTEGER,
    has_loan                    INTEGER,
    total_deposit_balance       DECIMAL(18,2),
    total_loan_outstanding      DECIMAL(18,2),
    aum_total                   DECIMAL(18,2),
    aum_bucket                  VARCHAR,
    txn_count_30d               BIGINT,
    txn_amount_30d              DECIMAL(18,2),
    last_txn_date               TIMESTAMP(6),
    days_since_last_txn         INTEGER,
    primary_channel             VARCHAR,
    interaction_count_90d       BIGINT,
    last_interaction_date       TIMESTAMP(6),
    rfm_recency_score           INTEGER,
    rfm_frequency_score         INTEGER,
    rfm_monetary_score          INTEGER,
    rfm_segment                 VARCHAR,
    churn_flag                  INTEGER,
    churn_risk                  VARCHAR,
    is_churn_candidate          INTEGER,
    cross_sell_credit_card_flag INTEGER,
    no_credit_card              INTEGER,
    no_deposit                  INTEGER,
    no_loan                     INTEGER,
    cross_sell_score            INTEGER,
    recommended_product         VARCHAR,
    recommendation_reason       VARCHAR,
    campaign_priority           VARCHAR,
    contact_eligible_flag       INTEGER,
    suppression_reason          VARCHAR,
    campaign_type               VARCHAR,
    cob_dt                      DATE
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['day(cob_dt)']
);
