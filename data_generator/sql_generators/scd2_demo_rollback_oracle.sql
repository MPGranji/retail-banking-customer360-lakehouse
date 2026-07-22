-- Restore the deterministic source values after the SCD2 demonstration.
-- Run after acceptance if a clean source state is required.

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
SET SERVEROUTPUT ON
SET LINESIZE 240
SET PAGESIZE 100

ALTER SESSION SET CONTAINER = XEPDB1;

PROMPT === ROLLBACK SCD2 DEMO SOURCE CHANGES ===

UPDATE core_banking.customer
SET phone = '0900009973',
    address = 'So 18 Le Loi',
    customer_segment = 'VIP',
    last_updated = TIMESTAMP '2026-01-02 08:00:00'
WHERE customer_id = 10001;

UPDATE core_banking.account
SET balance = CASE
        WHEN branch_code = 'BDG002' AND status = 'FROZEN' THEN balance - 1000000
        ELSE balance
    END,
    branch_code = 'BDG001',
    status = 'ACTIVE',
    last_updated = TIMESTAMP '2026-01-02 08:00:00'
WHERE account_id = 100001;

UPDATE core_banking.product
SET product_name = 'Tai khoan thanh toan USD',
    is_active = 1,
    last_updated = TIMESTAMP '2026-01-02 08:00:00'
WHERE product_code = 'CASA002';

UPDATE core_banking.branch
SET manager_name = 'Nguyen Van A',
    address = 'So 18 Duong 8, Thu Dau Mot',
    status = 'ACTIVE',
    last_updated = TIMESTAMP '2026-01-02 08:00:00'
WHERE branch_code = 'BDG001';

COMMIT;

SELECT customer_id, phone, address, customer_segment, last_updated
FROM core_banking.customer
WHERE customer_id = 10001;

SELECT account_id, branch_code, status, balance, last_updated
FROM core_banking.account
WHERE account_id = 100001;

SELECT product_code, product_name, is_active, last_updated
FROM core_banking.product
WHERE product_code = 'CASA002';

SELECT branch_code, manager_name, address, status, last_updated
FROM core_banking.branch
WHERE branch_code = 'BDG001';

PROMPT Source values restored. A later Bronze snapshot will represent the reversal as new history.
EXIT SUCCESS
