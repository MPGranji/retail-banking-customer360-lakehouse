-- =============================================================================
-- DDL: Core Banking Schema (Oracle XE 21c)
-- Schema: core_banking (7 bảng)
-- Mục đích: Nguồn dữ liệu cho Bronze ingestion pipeline
-- Quy tắc: Mọi bảng có last_updated TIMESTAMP + BEFORE UPDATE trigger
-- =============================================================================

-- Chuyển sang PDB (Pluggable Database)
ALTER SESSION SET CONTAINER = XEPDB1;

-- =============================================================================
-- 1. Cấp quyền cho schema/user CORE_BANKING
-- User được Oracle container tạo từ APP_USER/APP_USER_PASSWORD trong docker/.env.
-- Khi chạy DDL độc lập, tạo user bên ngoài script này để không lưu password trong Git.
-- =============================================================================

GRANT CREATE SESSION   TO core_banking;
GRANT CREATE TABLE     TO core_banking;
GRANT CREATE SEQUENCE  TO core_banking;
GRANT CREATE TRIGGER   TO core_banking;
GRANT CREATE VIEW      TO core_banking;

-- =============================================================================
-- 2. Bảng BRANCH (chi nhánh ngân hàng)
-- =============================================================================
CREATE TABLE core_banking.branch (
    branch_code     VARCHAR2(10)    NOT NULL,
    branch_name     VARCHAR2(200)   NOT NULL,
    region          VARCHAR2(20)    NOT NULL,   -- NORTH / CENTRAL / SOUTH
    city            VARCHAR2(100)   NOT NULL,
    district        VARCHAR2(100),
    address         VARCHAR2(500),
    manager_name    VARCHAR2(200),
    open_date       DATE,
    status          VARCHAR2(20)    NOT NULL,   -- ACTIVE / CLOSED
    last_updated    TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    --
    CONSTRAINT pk_branch PRIMARY KEY (branch_code),
    CONSTRAINT chk_branch_region  CHECK (region IN ('NORTH', 'CENTRAL', 'SOUTH')),
    CONSTRAINT chk_branch_status  CHECK (status IN ('ACTIVE', 'CLOSED'))
);

CREATE OR REPLACE TRIGGER core_banking.trg_branch_last_upd
    BEFORE UPDATE ON core_banking.branch
    FOR EACH ROW
BEGIN
    :NEW.last_updated := SYSTIMESTAMP;
END;
/

-- =============================================================================
-- 3. Bảng PRODUCT (sản phẩm ngân hàng)
-- =============================================================================
CREATE TABLE core_banking.product (
    product_code    VARCHAR2(20)    NOT NULL,
    product_name    VARCHAR2(200)   NOT NULL,
    product_group   VARCHAR2(20)    NOT NULL,   -- DEPOSIT / LOAN / CARD
    product_type    VARCHAR2(30)    NOT NULL,   -- CASA / SAVINGS / PERSONAL_LOAN / MORTGAGE / CREDIT_CARD / DEBIT_CARD
    currency        CHAR(3)         NOT NULL,   -- VND / USD
    is_active       NUMBER(1)       NOT NULL,   -- 0 / 1
    launch_date     DATE,
    last_updated    TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    --
    CONSTRAINT pk_product PRIMARY KEY (product_code),
    CONSTRAINT chk_product_group  CHECK (product_group  IN ('DEPOSIT', 'LOAN', 'CARD')),
    CONSTRAINT chk_product_type   CHECK (product_type   IN ('CASA', 'SAVINGS', 'PERSONAL_LOAN', 'MORTGAGE', 'CREDIT_CARD', 'DEBIT_CARD')),
    CONSTRAINT chk_product_active CHECK (is_active      IN (0, 1))
);

CREATE OR REPLACE TRIGGER core_banking.trg_product_last_upd
    BEFORE UPDATE ON core_banking.product
    FOR EACH ROW
BEGIN
    :NEW.last_updated := SYSTIMESTAMP;
END;
/

-- =============================================================================
-- 4. Bảng CUSTOMER (khách hàng)
-- =============================================================================
CREATE TABLE core_banking.customer (
    customer_id         NUMBER(12)      NOT NULL,
    cccd                VARCHAR2(12),               -- Căn cước công dân 12 số
    full_name           VARCHAR2(200)   NOT NULL,
    gender              CHAR(1)         NOT NULL,   -- M / F / O
    date_of_birth       DATE            NOT NULL,
    phone               VARCHAR2(15),               -- 0xxxxxxxxx
    email               VARCHAR2(200),
    address             VARCHAR2(500),
    city                VARCHAR2(100),
    district            VARCHAR2(100),
    branch_code         VARCHAR2(10),               -- FK → branch (chi nhánh mở đầu tiên)
    customer_segment    VARCHAR2(20)    NOT NULL,   -- RETAIL / PRIORITY / VIP
    kyc_status          VARCHAR2(20)    NOT NULL,   -- PENDING / VERIFIED / REJECTED
    register_date       DATE            NOT NULL,
    is_active           NUMBER(1)       NOT NULL,   -- 0 / 1
    last_updated        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    --
    CONSTRAINT pk_customer          PRIMARY KEY (customer_id),
    CONSTRAINT uq_customer_cccd     UNIQUE      (cccd),
    CONSTRAINT fk_customer_branch   FOREIGN KEY (branch_code) REFERENCES core_banking.branch(branch_code),
    CONSTRAINT chk_customer_gender  CHECK (gender             IN ('M', 'F', 'O')),
    CONSTRAINT chk_customer_segment CHECK (customer_segment   IN ('RETAIL', 'PRIORITY', 'VIP')),
    CONSTRAINT chk_customer_kyc     CHECK (kyc_status         IN ('PENDING', 'VERIFIED', 'REJECTED')),
    CONSTRAINT chk_customer_active  CHECK (is_active          IN (0, 1))
);

CREATE INDEX core_banking.idx_customer_branch   ON core_banking.customer(branch_code);
CREATE INDEX core_banking.idx_customer_segment  ON core_banking.customer(customer_segment);
CREATE INDEX core_banking.idx_customer_upd      ON core_banking.customer(last_updated);

CREATE OR REPLACE TRIGGER core_banking.trg_customer_last_upd
    BEFORE UPDATE ON core_banking.customer
    FOR EACH ROW
BEGIN
    :NEW.last_updated := SYSTIMESTAMP;
END;
/

-- =============================================================================
-- 5. Bảng ACCOUNT (tài khoản TGTT / tiết kiệm)
-- =============================================================================
CREATE TABLE core_banking.account (
    account_id      NUMBER(14)      NOT NULL,
    account_no      VARCHAR2(20)    NOT NULL,
    customer_id     NUMBER(12)      NOT NULL,
    product_code    VARCHAR2(20)    NOT NULL,
    branch_code     VARCHAR2(10)    NOT NULL,
    account_type    VARCHAR2(20)    NOT NULL,   -- CASA / TIME_DEPOSIT
    currency        CHAR(3)         DEFAULT 'VND' NOT NULL,
    balance         NUMBER(18,2)    DEFAULT 0 NOT NULL,
    open_date       DATE            NOT NULL,
    close_date      DATE,
    status          VARCHAR2(20)    NOT NULL,   -- ACTIVE / CLOSED / FROZEN
    last_updated    TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    --
    CONSTRAINT pk_account           PRIMARY KEY (account_id),
    CONSTRAINT uq_account_no        UNIQUE      (account_no),
    CONSTRAINT fk_account_customer  FOREIGN KEY (customer_id)  REFERENCES core_banking.customer(customer_id),
    CONSTRAINT fk_account_product   FOREIGN KEY (product_code) REFERENCES core_banking.product(product_code),
    CONSTRAINT fk_account_branch    FOREIGN KEY (branch_code)  REFERENCES core_banking.branch(branch_code),
    CONSTRAINT chk_account_type     CHECK (account_type IN ('CASA', 'TIME_DEPOSIT')),
    CONSTRAINT chk_account_status   CHECK (status       IN ('ACTIVE', 'CLOSED', 'FROZEN'))
);

CREATE INDEX core_banking.idx_account_customer ON core_banking.account(customer_id);
CREATE INDEX core_banking.idx_account_upd      ON core_banking.account(last_updated);

CREATE OR REPLACE TRIGGER core_banking.trg_account_last_upd
    BEFORE UPDATE ON core_banking.account
    FOR EACH ROW
BEGIN
    :NEW.last_updated := SYSTIMESTAMP;
END;
/

-- =============================================================================
-- 6. Bảng DEPOSIT (sổ tiết kiệm)
-- =============================================================================
CREATE TABLE core_banking.deposit (
    deposit_id          NUMBER(14)      NOT NULL,
    account_id          NUMBER(14),                 -- FK → account (có thể NULL với TK tiết kiệm độc lập)
    customer_id         NUMBER(12)      NOT NULL,
    product_code        VARCHAR2(20)    NOT NULL,
    principal_amount    NUMBER(18,2)    NOT NULL,
    interest_rate       NUMBER(5,2)     NOT NULL,   -- vd 5.50 = 5.5%/năm
    term_months         NUMBER(3)       NOT NULL,   -- 1/3/6/12/24/36
    open_date           DATE            NOT NULL,
    maturity_date       DATE            NOT NULL,
    status              VARCHAR2(20)    NOT NULL,   -- ACTIVE / MATURED / EARLY_WITHDRAWN
    last_updated        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    --
    CONSTRAINT pk_deposit           PRIMARY KEY (deposit_id),
    CONSTRAINT fk_deposit_account   FOREIGN KEY (account_id)   REFERENCES core_banking.account(account_id),
    CONSTRAINT fk_deposit_customer  FOREIGN KEY (customer_id)  REFERENCES core_banking.customer(customer_id),
    CONSTRAINT fk_deposit_product   FOREIGN KEY (product_code) REFERENCES core_banking.product(product_code),
    CONSTRAINT chk_deposit_status   CHECK (status IN ('ACTIVE', 'MATURED', 'EARLY_WITHDRAWN')),
    CONSTRAINT chk_deposit_rate     CHECK (interest_rate > 0),
    CONSTRAINT chk_deposit_term     CHECK (term_months IN (1, 3, 6, 12, 24, 36))
);

CREATE INDEX core_banking.idx_deposit_customer ON core_banking.deposit(customer_id);
CREATE INDEX core_banking.idx_deposit_upd      ON core_banking.deposit(last_updated);

CREATE OR REPLACE TRIGGER core_banking.trg_deposit_last_upd
    BEFORE UPDATE ON core_banking.deposit
    FOR EACH ROW
BEGIN
    :NEW.last_updated := SYSTIMESTAMP;
END;
/

-- =============================================================================
-- 7. Bảng LOAN (khoản vay)
-- =============================================================================
CREATE TABLE core_banking.loan (
    loan_id             NUMBER(14)      NOT NULL,
    customer_id         NUMBER(12)      NOT NULL,
    product_code        VARCHAR2(20)    NOT NULL,
    branch_code         VARCHAR2(10)    NOT NULL,
    loan_amount         NUMBER(18,2)    NOT NULL,   -- số tiền vay ban đầu
    outstanding_balance NUMBER(18,2)    NOT NULL,   -- dư nợ hiện tại
    interest_rate       NUMBER(5,2)     NOT NULL,
    term_months         NUMBER(3)       NOT NULL,
    disbursement_date   DATE            NOT NULL,
    maturity_date       DATE            NOT NULL,
    loan_status         VARCHAR2(20)    NOT NULL,   -- ACTIVE / CLOSED / OVERDUE / WRITTEN_OFF
    last_updated        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    --
    CONSTRAINT pk_loan              PRIMARY KEY (loan_id),
    CONSTRAINT fk_loan_customer     FOREIGN KEY (customer_id)  REFERENCES core_banking.customer(customer_id),
    CONSTRAINT fk_loan_product      FOREIGN KEY (product_code) REFERENCES core_banking.product(product_code),
    CONSTRAINT fk_loan_branch       FOREIGN KEY (branch_code)  REFERENCES core_banking.branch(branch_code),
    CONSTRAINT chk_loan_status      CHECK (loan_status IN ('ACTIVE', 'CLOSED', 'OVERDUE', 'WRITTEN_OFF')),
    CONSTRAINT chk_loan_amount      CHECK (loan_amount > 0)
);

CREATE INDEX core_banking.idx_loan_customer ON core_banking.loan(customer_id);
CREATE INDEX core_banking.idx_loan_status   ON core_banking.loan(loan_status);
CREATE INDEX core_banking.idx_loan_upd      ON core_banking.loan(last_updated);

CREATE OR REPLACE TRIGGER core_banking.trg_loan_last_upd
    BEFORE UPDATE ON core_banking.loan
    FOR EACH ROW
BEGIN
    :NEW.last_updated := SYSTIMESTAMP;
END;
/

-- =============================================================================
-- 8. Bảng TXN_ACCOUNT (giao dịch tài khoản) — bảng lớn nhất (~1M rows/tháng)
-- =============================================================================
CREATE TABLE core_banking.txn_account (
    txn_id          NUMBER(18)      NOT NULL,
    account_id      NUMBER(14)      NOT NULL,
    customer_id     NUMBER(12)      NOT NULL,   -- denormalized để tăng tốc query
    txn_date        TIMESTAMP       NOT NULL,
    txn_amount      NUMBER(18,2)    NOT NULL,
    txn_type        VARCHAR2(30)    NOT NULL,   -- DEPOSIT / WITHDRAWAL / TRANSFER_IN / TRANSFER_OUT / FEE / INTEREST
    debit_credit    CHAR(1)         NOT NULL,   -- D (Debit) / C (Credit)
    balance_after   NUMBER(18,2)    NOT NULL,
    channel         VARCHAR2(20)    NOT NULL,   -- BRANCH / ATM / INTERNET_BANKING / MOBILE_BANKING / POS
    description     VARCHAR2(500),
    counter_account VARCHAR2(20),              -- tài khoản đối ứng (transfer)
    created_ts      TIMESTAMP       NOT NULL,
    last_updated    TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    --
    CONSTRAINT pk_txn_account       PRIMARY KEY (txn_id),
    CONSTRAINT fk_txn_account_acct  FOREIGN KEY (account_id) REFERENCES core_banking.account(account_id),
    CONSTRAINT chk_txn_type         CHECK (txn_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER_IN', 'TRANSFER_OUT', 'FEE', 'INTEREST')),
    CONSTRAINT chk_txn_dc           CHECK (debit_credit IN ('D', 'C')),
    CONSTRAINT chk_txn_channel      CHECK (channel IN ('BRANCH', 'ATM', 'INTERNET_BANKING', 'MOBILE_BANKING', 'POS'))
);

-- Index hỗ trợ incremental ingest và query theo khách hàng
CREATE INDEX core_banking.idx_txn_acct_date ON core_banking.txn_account(account_id,  txn_date);
CREATE INDEX core_banking.idx_txn_cust_date ON core_banking.txn_account(customer_id, txn_date);
CREATE INDEX core_banking.idx_txn_upd       ON core_banking.txn_account(last_updated);

CREATE OR REPLACE TRIGGER core_banking.trg_txn_account_last_upd
    BEFORE UPDATE ON core_banking.txn_account
    FOR EACH ROW
BEGIN
    :NEW.last_updated := SYSTIMESTAMP;
END;
/

-- =============================================================================
-- Kết thúc — commit
-- =============================================================================
COMMIT;
