-- =====================================================
-- DDL Bronze Layer — Apache Iceberg Tables
-- Catalog: lakehouse  |  Schema: bronze
-- Partitioned by days(cob_dt) — idempotent overwrites
-- Format-version 2 (row-level deletes, merge-on-read)
--
-- Cột khớp chính xác với SELECT trong YAML config.
-- Bronze job (ingestion_jdbc.py) không thêm metadata —
-- chỉ ghi đúng những gì JDBC query trả về.
-- =====================================================

-- ─── CORE BANKING (nguồn Oracle) ─────────────────────

-- core_branch: full_snapshot, 100 chi nhánh
CREATE TABLE lakehouse.bronze.core_branch (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- core_product: full_snapshot, 30 sản phẩm
CREATE TABLE lakehouse.bronze.core_product (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- core_customer: full_snapshot, 10k khách hàng
CREATE TABLE lakehouse.bronze.core_customer (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- core_account: full_snapshot, 30k tài khoản
CREATE TABLE lakehouse.bronze.core_account (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- core_deposit: full_snapshot, 5k sổ tiết kiệm
CREATE TABLE lakehouse.bronze.core_deposit (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- core_loan: full_snapshot, 3k khoản vay
CREATE TABLE lakehouse.bronze.core_loan (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- core_txn_account: incremental, ~1M giao dịch/tháng
CREATE TABLE lakehouse.bronze.core_txn_account (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

-- ─── CARD & CRM (nguồn PostgreSQL) ───────────────────

-- card: full_snapshot, 8k thẻ
CREATE TABLE lakehouse.bronze.card (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- card_txn: incremental, ~500k giao dịch thẻ/tháng
CREATE TABLE lakehouse.bronze.card_txn (
    txn_id            BIGINT,
    card_id           BIGINT,
    customer_id       BIGINT,
    txn_date          TIMESTAMP,
    txn_amount        DECIMAL(18,2),
    txn_type          STRING,
    currency          STRING,
    merchant_name     STRING,
    merchant_category STRING,
    channel           STRING,
    status            STRING,
    created_ts        TIMESTAMP,
    last_updated      TIMESTAMP,
    cob_dt            DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

-- crm_interaction: incremental, ~50k tương tác/quý
CREATE TABLE lakehouse.bronze.crm_interaction (
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
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
