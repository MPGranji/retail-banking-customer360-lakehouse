-- Controlled day-2 changes for the SCD2 history demonstration.
-- Run only after the 2025-12-31 Bronze/Silver baseline has been accepted.
-- The statements are idempotent for the seeded demo dataset.

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
SET SERVEROUTPUT ON
SET VERIFY OFF
SET LINESIZE 240
SET PAGESIZE 100

ALTER SESSION SET CONTAINER = XEPDB1;

PROMPT === SCD2 DEMO PRE-CHECK ===

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM core_banking.customer WHERE customer_id IN (10001, 10002);
    IF v_count <> 2 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Expected customer_id 10001 and 10002');
    END IF;

    SELECT COUNT(*) INTO v_count FROM core_banking.account WHERE account_id = 100001;
    IF v_count <> 1 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Expected account_id 100001');
    END IF;

    SELECT COUNT(*) INTO v_count FROM core_banking.product WHERE product_code = 'CASA002';
    IF v_count <> 1 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Expected product_code CASA002');
    END IF;

    SELECT COUNT(*) INTO v_count FROM core_banking.branch WHERE branch_code = 'BDG001';
    IF v_count <> 1 THEN
        RAISE_APPLICATION_ERROR(-20004, 'Expected branch_code BDG001');
    END IF;
END;
/

SELECT customer_id, phone, address, branch_code, customer_segment, last_updated
FROM core_banking.customer
WHERE customer_id IN (10001, 10002)
ORDER BY customer_id;

SELECT account_id, product_code, branch_code, status, balance, last_updated
FROM core_banking.account
WHERE account_id = 100001;

SELECT product_code, product_name, is_active, last_updated
FROM core_banking.product
WHERE product_code = 'CASA002';

SELECT branch_code, manager_name, address, status, last_updated
FROM core_banking.branch
WHERE branch_code = 'BDG001';

PROMPT === APPLY BUSINESS CHANGES FOR 2026-01-01 ===

UPDATE core_banking.customer
SET phone = '0988880001',
    address = 'SCD2 Demo Customer Address',
    customer_segment = 'PRIORITY',
    last_updated = TIMESTAMP '2026-01-01 08:00:00'
WHERE customer_id = 10001;

-- The branch-code guard prevents balance from increasing again on script rerun.
UPDATE core_banking.account
SET balance = CASE WHEN branch_code = 'BDG001' THEN balance + 1000000 ELSE balance END,
    branch_code = 'BDG002',
    status = 'FROZEN',
    last_updated = TIMESTAMP '2026-01-01 08:00:00'
WHERE account_id = 100001
  AND branch_code IN ('BDG001', 'BDG002');

UPDATE core_banking.product
SET product_name = 'Tai khoan thanh toan USD - SCD2 DEMO',
    is_active = 0,
    last_updated = TIMESTAMP '2026-01-01 08:00:00'
WHERE product_code = 'CASA002';

UPDATE core_banking.branch
SET manager_name = 'SCD2 Demo Manager',
    address = 'SCD2 Demo Branch Address',
    status = 'CLOSED',
    last_updated = TIMESTAMP '2026-01-01 08:00:00'
WHERE branch_code = 'BDG001';

-- Technical-only change: this customer must not receive a new SCD2 version.
UPDATE core_banking.customer
SET last_updated = TIMESTAMP '2026-01-01 08:00:00'
WHERE customer_id = 10002;

COMMIT;

PROMPT === SCD2 DEMO POST-CHECK ===

SELECT customer_id, phone, address, branch_code, customer_segment, last_updated
FROM core_banking.customer
WHERE customer_id IN (10001, 10002)
ORDER BY customer_id;

SELECT account_id, product_code, branch_code, status, balance, last_updated
FROM core_banking.account
WHERE account_id = 100001;

SELECT product_code, product_name, is_active, last_updated
FROM core_banking.product
WHERE product_code = 'CASA002';

SELECT branch_code, manager_name, address, status, last_updated
FROM core_banking.branch
WHERE branch_code = 'BDG001';

PROMPT Changes committed. Ingest Bronze with cob_dt=2026-01-01 next.
EXIT SUCCESS
