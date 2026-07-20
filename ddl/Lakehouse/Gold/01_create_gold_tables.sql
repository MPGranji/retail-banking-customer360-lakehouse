-- =====================================================
-- DDL Gold Layer — Apache Iceberg Tables
-- Catalog: lakehouse  |  Schema: gold
-- Mart360 current: 1 row/customer toàn bảng
-- Mart360 history: 1 row/customer/cob_dt
-- Segmentation: 1 row/customer/cob_dt
-- Partitioned by days(cob_dt)
-- =====================================================

-- ─── MART 360 ─────────────────────────────────────────

-- mart_customer_360: serving table hiện hành với 28+ KPIs/khách hàng
CREATE TABLE lakehouse.gold.mart_customer_360 (
    -- Identity & Profile (5 KPIs)
    customer_id               BIGINT,
    customer_sk               STRING,
    full_name_masked          STRING,
    age                       INT,
    gender                    STRING,
    primary_branch_code       STRING,
    customer_segment          STRING,
    kyc_status                STRING,
    register_date             DATE,
    -- Product Holding (6 KPIs)
    total_accounts            INT,
    total_cards               INT,
    total_loans               INT,
    has_credit_card           INT,           -- 0/1
    has_savings               INT,           -- 0/1
    has_loan                  INT,           -- 0/1
    -- Balance & AUM (4 KPIs)
    total_deposit_balance     DECIMAL(18,2),
    total_loan_outstanding    DECIMAL(18,2),
    aum_total                 DECIMAL(18,2),
    aum_bucket                STRING,        -- MASS / AFFLUENT / PRIORITY / VIP
    -- Transaction Behavior (5 KPIs)
    txn_count_30d             BIGINT,
    txn_amount_30d            DECIMAL(18,2),
    last_txn_date             TIMESTAMP,
    days_since_last_txn       INT,
    primary_channel           STRING,
    -- CRM Engagement (2 KPIs)
    interaction_count_90d     BIGINT,
    last_interaction_date     TIMESTAMP,
    -- RFM (4 KPIs)
    rfm_recency_score         INT,
    rfm_frequency_score       INT,
    rfm_monetary_score        INT,
    rfm_segment               STRING,        -- Champions / Loyal Customers / ...
    -- Risk Flags (2 KPIs)
    churn_flag                INT,           -- 0/1
    cross_sell_credit_card_flag INT,         -- 0/1
    -- Partition
    cob_dt                    DATE
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- mart_customer_360_history: lịch sử snapshot, cùng schema với current mart
CREATE TABLE lakehouse.gold.mart_customer_360_history (
    customer_id               BIGINT,
    customer_sk               STRING,
    full_name_masked          STRING,
    age                       INT,
    gender                    STRING,
    primary_branch_code       STRING,
    customer_segment          STRING,
    kyc_status                STRING,
    register_date             DATE,
    total_accounts            INT,
    total_cards               INT,
    total_loans               INT,
    has_credit_card           INT,
    has_savings               INT,
    has_loan                  INT,
    total_deposit_balance     DECIMAL(18,2),
    total_loan_outstanding    DECIMAL(18,2),
    aum_total                 DECIMAL(18,2),
    aum_bucket                STRING,
    txn_count_30d             BIGINT,
    txn_amount_30d            DECIMAL(18,2),
    last_txn_date             TIMESTAMP,
    days_since_last_txn       INT,
    primary_channel           STRING,
    interaction_count_90d     BIGINT,
    last_interaction_date     TIMESTAMP,
    rfm_recency_score         INT,
    rfm_frequency_score       INT,
    rfm_monetary_score        INT,
    rfm_segment               STRING,
    churn_flag                INT,
    cross_sell_credit_card_flag INT,
    cob_dt                    DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- customer_balance_summary: AUM và balance chi tiết
CREATE TABLE lakehouse.gold.customer_balance_summary (
    customer_id             BIGINT,
    customer_sk             STRING,
    total_account_balance   DECIMAL(18,2),
    avg_account_balance     DECIMAL(18,2),
    total_deposit_principal DECIMAL(18,2),
    total_loan_outstanding  DECIMAL(18,2),
    aum_total               DECIMAL(18,2),
    net_worth               DECIMAL(18,2),
    cob_dt                  DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- customer_transaction_summary: hành vi giao dịch 30 ngày
CREATE TABLE lakehouse.gold.customer_transaction_summary (
    customer_id               BIGINT,
    customer_sk               STRING,
    acct_txn_count_30d        BIGINT,
    acct_txn_amount_30d       DECIMAL(18,2),
    acct_credit_count_30d     BIGINT,
    acct_debit_count_30d      BIGINT,
    acct_credit_amount_30d    DECIMAL(18,2),
    acct_debit_amount_30d     DECIMAL(18,2),
    card_txn_count_30d        BIGINT,
    card_txn_amount_30d       DECIMAL(18,2),
    total_txn_count_30d       BIGINT,
    total_txn_amount_30d      DECIMAL(18,2),
    last_txn_date             TIMESTAMP,
    cob_dt                    DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- customer_product_summary: sở hữu sản phẩm chi tiết
CREATE TABLE lakehouse.gold.customer_product_summary (
    customer_id         BIGINT,
    customer_sk         STRING,
    total_accounts      INT,
    cnt_casa_active     INT,
    cnt_td_active       INT,
    cnt_deposits        INT,
    cnt_deposits_active INT,
    cnt_loans           INT,
    cnt_loans_active    INT,
    cnt_cards           INT,
    cnt_credit_cards    INT,
    cnt_debit_cards     INT,
    has_credit_card     INT,
    has_savings         INT,
    has_loan            INT,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- customer_card_summary: chi tiết thẻ và giao dịch thẻ
CREATE TABLE lakehouse.gold.customer_card_summary (
    customer_id                 BIGINT,
    customer_sk                 STRING,
    total_cards                 INT,
    cnt_credit_active           INT,
    cnt_debit_active            INT,
    max_credit_limit            DECIMAL(18,2),
    total_card_txn_count_30d    BIGINT,
    total_card_txn_amount_30d   DECIMAL(18,2),
    avg_card_txn_amount_30d     DECIMAL(18,2),
    distinct_merchant_categories INT,
    last_card_txn_date          TIMESTAMP,
    cob_dt                      DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- ─── SEGMENTATION ─────────────────────────────────────

-- rfm_segment: điểm RFM và phân khúc 7 nhóm
CREATE TABLE lakehouse.gold.rfm_segment (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- churn_prediction: dự đoán nguy cơ churn
CREATE TABLE lakehouse.gold.churn_prediction (
    customer_id           BIGINT,
    customer_sk           STRING,
    txn_cnt_30d           BIGINT,
    txn_cnt_90d           BIGINT,
    txn_amt_30d           DECIMAL(18,2),
    txn_amt_90d           DECIMAL(18,2),
    days_since_last_txn   INT,
    churn_risk            STRING,     -- Active / Low / Medium / High
    is_churn_candidate    INT,        -- 0/1
    cob_dt                DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- cross_sell_segment: cơ hội cross-sell theo sản phẩm
CREATE TABLE lakehouse.gold.cross_sell_segment (
    customer_id           BIGINT,
    customer_sk           STRING,
    customer_segment      STRING,
    no_credit_card        INT,
    no_deposit            INT,
    no_loan               INT,
    primary_opportunity   STRING,
    cob_dt                DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- campaign_target: danh sách target cuối cùng cho CRM
CREATE TABLE lakehouse.gold.campaign_target (
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
    cross_sell_score      INT,        -- 0..100
    recommended_product   STRING,
    recommendation_reason STRING,
    campaign_priority     STRING,     -- HIGH / MEDIUM / LOW
    contact_eligible_flag INT,        -- 0/1, suppression applied after scoring
    suppression_reason    STRING,
    campaign_type         STRING,     -- Retention / Cross_Sell_CC / Cross_Sell / Upsell / Awareness
    cob_dt                DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
