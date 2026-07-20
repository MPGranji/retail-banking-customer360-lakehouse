-- =====================================================
-- DDL Silver Layer — Apache Iceberg Tables
-- Catalog: lakehouse  |  Schema: silver
--
-- Cột khớp chính xác với output của từng base job:
--   SCD Type 1 (scd_type1.py)  : ghi đúng src_cols từ YAML SQL (MERGE INTO)
--                                 → không có cob_dt (dim là current-state, không partition)
--   SCD Type 2 (scd_type2.py)  : ghi src_cols + effective_from + effective_to
--                                 + is_current + {entity}_sk (append)
--                                 → không có cob_dt (YAML SQL không SELECT nó)
--   Fact (fact_txn.py)         : ghi src_cols từ YAML SQL (createOrReplace)
--                                 → có cob_dt vì YAML SQL SELECT t.cob_dt / i.cob_dt
-- =====================================================

-- ─── DIMENSION TABLES ────────────────────────────────

-- dim_branch: SCD Type 1 — MERGE IN-PLACE, không có lịch sử
-- Nguồn: bronze.core_branch | Cột: đúng với SELECT trong dim_branch.yml
CREATE TABLE lakehouse.silver.dim_branch (
    branch_code   STRING,
    branch_name   STRING,
    region        STRING,
    city          STRING,
    district      STRING,
    address       STRING,
    manager_name  STRING,
    open_date     DATE,
    status        STRING,
    last_updated  TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- dim_product: SCD Type 1 — MERGE IN-PLACE, không có lịch sử
-- Nguồn: bronze.core_product | Cột: đúng với SELECT trong dim_product.yml
CREATE TABLE lakehouse.silver.dim_product (
    product_code   STRING,
    product_name   STRING,
    product_group  STRING,
    product_type   STRING,
    currency       STRING,
    is_active      INT,
    launch_date    DATE,
    last_updated   TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- dim_customer: SCD Type 2 — lưu lịch sử thay đổi thông tin cá nhân
-- Nguồn: bronze.core_customer
-- scd_type2.py append: src_cols + effective_from + effective_to + is_current + customer_sk
-- Partition by is_current tối ưu cho query WHERE is_current = 1
CREATE TABLE lakehouse.silver.dim_customer (
    customer_id        BIGINT,
    cccd               STRING,
    full_name          STRING,
    gender             STRING,
    date_of_birth      DATE,
    phone              STRING,
    email              STRING,
    address            STRING,
    city               STRING,
    district           STRING,
    branch_code        STRING,
    customer_segment   STRING,
    kyc_status         STRING,
    register_date      DATE,
    is_active          INT,
    last_updated       TIMESTAMP,
    -- SCD2 metadata (thêm bởi scd_type2.py)
    effective_from     DATE            COMMENT 'Ngày dòng này bắt đầu có hiệu lực (= cob_dt detect thay đổi)',
    effective_to       DATE            COMMENT '9999-12-31 nếu is_current=1; ngày expire nếu is_current=0',
    is_current         INT,
    customer_sk        STRING          -- SHA256(customer_id || cob_dt)
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- dim_account: SCD Type 2 — theo dõi thay đổi balance, status, close_date
-- Nguồn: bronze.core_account
-- scd_type2.py append: src_cols + effective_from + effective_to + is_current + account_sk
CREATE TABLE lakehouse.silver.dim_account (
    account_id     BIGINT,
    account_no     STRING,
    customer_id    BIGINT,
    product_code   STRING,
    branch_code    STRING,
    account_type   STRING,            -- CASA / TIME_DEPOSIT
    currency       STRING,
    balance        DECIMAL(18,2),
    open_date      DATE,
    close_date     DATE,
    status         STRING,            -- ACTIVE / CLOSED / FROZEN
    last_updated   TIMESTAMP,
    -- SCD2 metadata (thêm bởi scd_type2.py)
    effective_from DATE               COMMENT 'Ngày dòng này bắt đầu có hiệu lực (= cob_dt detect thay đổi)',
    effective_to   DATE               COMMENT '9999-12-31 nếu is_current=1; ngày expire nếu is_current=0',
    is_current     INT,
    account_sk     STRING             -- SHA256(account_id || cob_dt)
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- dim_deposit: SCD Type 1 — ít thay đổi sau khi mở
-- Nguồn: bronze.core_deposit | Cột: đúng với SELECT trong dim_deposit.yml
CREATE TABLE lakehouse.silver.dim_deposit (
    deposit_id       BIGINT,
    account_id       BIGINT,
    customer_id      BIGINT,
    product_code     STRING,
    principal_amount DECIMAL(18,2),
    interest_rate    DECIMAL(5,2),
    term_months      INT,
    open_date        DATE,
    maturity_date    DATE,
    status           STRING,          -- ACTIVE / MATURED / EARLY_WITHDRAWN
    last_updated     TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- dim_loan: SCD Type 1 — outstanding_balance cập nhật liên tục qua MERGE
-- Nguồn: bronze.core_loan | Cột: đúng với SELECT trong dim_loan.yml
CREATE TABLE lakehouse.silver.dim_loan (
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
    loan_status          STRING,      -- ACTIVE / CLOSED / OVERDUE / WRITTEN_OFF
    last_updated         TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- dim_card: SCD Type 1 — status, credit_limit cập nhật qua MERGE
-- Nguồn: bronze.card | Cột: đúng với SELECT trong dim_card.yml
-- Lưu ý: expiry_date ở đây là ngày hết hạn thẻ (business column, KHÔNG phải SCD metadata)
CREATE TABLE lakehouse.silver.dim_card (
    card_id        BIGINT,
    card_no_masked STRING,
    customer_id    BIGINT,
    account_id     BIGINT,
    product_code   STRING,
    card_type      STRING,            -- DEBIT / CREDIT / PREPAID
    card_brand     STRING,            -- VISA / MASTER / JCB / NAPAS
    credit_limit   DECIMAL(18,2),
    issue_date     DATE,
    expiry_date    DATE,              -- ngày hết hạn thẻ (business field)
    status         STRING,            -- ACTIVE / BLOCKED / EXPIRED / CLOSED
    last_updated   TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- ─── FACT TABLES ──────────────────────────────────────

-- fact_txn_account: giao dịch tài khoản CASA/TGCKH
-- Nguồn: bronze.core_txn_account JOIN silver.dim_account JOIN silver.dim_customer
-- fact_txn.py ghi đúng src_cols từ YAML SQL (createOrReplace theo cob_dt)
-- cob_dt có mặt vì YAML SQL SELECT t.cob_dt và fact_txn.py partition theo nó
CREATE TABLE lakehouse.silver.fact_txn_account (
    txn_id           BIGINT,
    account_id       BIGINT,
    account_sk       STRING,          -- từ dim_account (LEFT JOIN)
    customer_id      BIGINT,
    customer_sk      STRING,          -- từ dim_customer (LEFT JOIN)
    txn_date         TIMESTAMP,
    txn_amount       DECIMAL(18,2),
    txn_type         STRING,          -- DEPOSIT / WITHDRAWAL / TRANSFER_IN / TRANSFER_OUT / FEE / INTEREST
    debit_credit     STRING,          -- D (Debit) / C (Credit)
    balance_after    DECIMAL(18,2),
    channel          STRING,          -- BRANCH / ATM / INTERNET_BANKING / MOBILE_BANKING / POS
    description      STRING,
    counter_account  STRING,
    created_ts       TIMESTAMP,
    cob_dt           DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

-- fact_card_txn: giao dịch thẻ
-- Nguồn: bronze.card_txn JOIN silver.dim_customer
-- fact_txn.py ghi đúng src_cols từ YAML SQL (createOrReplace theo cob_dt)
CREATE TABLE lakehouse.silver.fact_card_txn (
    txn_id            BIGINT,
    card_id           BIGINT,
    customer_id       BIGINT,
    customer_sk       STRING,         -- từ dim_customer (LEFT JOIN)
    txn_date          TIMESTAMP,
    txn_amount        DECIMAL(18,2),
    txn_type          STRING,         -- PURCHASE / CASH_ADVANCE / REFUND / REVERSAL
    currency          STRING,
    merchant_name     STRING,
    merchant_category STRING,         -- GROCERY / RESTAURANT / TRAVEL / ECOM / FUEL / EDUCATION
    channel           STRING,         -- POS / ECOM / ATM
    status            STRING,         -- SUCCESS / FAILED / PENDING
    created_ts        TIMESTAMP,
    cob_dt            DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

-- fact_crm_interaction: tương tác CRM
-- Nguồn: bronze.crm_interaction JOIN silver.dim_customer
-- fact_txn.py ghi đúng src_cols từ YAML SQL (createOrReplace theo cob_dt)
CREATE TABLE lakehouse.silver.fact_crm_interaction (
    interaction_id    BIGINT,
    customer_id       BIGINT,
    customer_sk       STRING,         -- từ dim_customer (LEFT JOIN)
    interaction_date  TIMESTAMP,
    channel           STRING,         -- CALL / EMAIL / CHAT / BRANCH / SMS
    direction         STRING,         -- INBOUND / OUTBOUND
    subject           STRING,
    category          STRING,         -- COMPLAINT / INQUIRY / CAMPAIGN / CROSS_SELL / RETENTION
    status            STRING,         -- OPEN / RESOLVED / PENDING
    assigned_to       STRING,
    created_ts        TIMESTAMP,
    cob_dt            DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
