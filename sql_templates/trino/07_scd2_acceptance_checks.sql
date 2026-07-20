-- SCD2 acceptance checks for branch/product/customer/account.
-- Run through Trino after Silver day-2 and once more after the same-day rerun.

-- 1. Row counts, current/history counts and rerun fingerprint.
SELECT 'dim_branch' AS table_name,
       count(*) AS total_rows,
       count_if(is_current = 1) AS current_rows,
       count_if(is_current = 0) AS history_rows,
       count(DISTINCT branch_code) AS business_keys,
       to_hex(checksum(branch_sk)) AS sk_checksum
FROM lakehouse.silver.dim_branch
UNION ALL
SELECT 'dim_product', count(*), count_if(is_current = 1), count_if(is_current = 0),
       count(DISTINCT product_code), to_hex(checksum(product_sk))
FROM lakehouse.silver.dim_product
UNION ALL
SELECT 'dim_customer', count(*), count_if(is_current = 1), count_if(is_current = 0),
       count(DISTINCT customer_id), to_hex(checksum(customer_sk))
FROM lakehouse.silver.dim_customer
UNION ALL
SELECT 'dim_account', count(*), count_if(is_current = 1), count_if(is_current = 0),
       count(DISTINCT account_id), to_hex(checksum(account_sk))
FROM lakehouse.silver.dim_account
ORDER BY table_name;

-- 2. Every result must be zero.
WITH history AS (
    SELECT 'branch' AS entity, branch_code AS business_key, branch_sk AS sk,
           effective_from, effective_to, is_current
    FROM lakehouse.silver.dim_branch
    UNION ALL
    SELECT 'product', product_code, product_sk, effective_from, effective_to, is_current
    FROM lakehouse.silver.dim_product
    UNION ALL
    SELECT 'customer', CAST(customer_id AS varchar), customer_sk,
           effective_from, effective_to, is_current
    FROM lakehouse.silver.dim_customer
    UNION ALL
    SELECT 'account', CAST(account_id AS varchar), account_sk,
           effective_from, effective_to, is_current
    FROM lakehouse.silver.dim_account
), ordered AS (
    SELECT *,
           lag(effective_to) OVER (
               PARTITION BY entity, business_key
               ORDER BY effective_from, sk
           ) AS previous_effective_to
    FROM history
)
SELECT 'duplicate_surrogate_key' AS check_name, count(*) AS violations
FROM (
    SELECT entity, sk FROM history GROUP BY entity, sk HAVING count(*) > 1
)
UNION ALL
SELECT 'multiple_current_rows', count(*)
FROM (
    SELECT entity, business_key
    FROM history
    GROUP BY entity, business_key
    HAVING count_if(is_current = 1) > 1
)
UNION ALL
SELECT 'invalid_date_or_sentinel', count(*)
FROM history
WHERE effective_from > effective_to
   OR (is_current = 1 AND effective_to <> DATE '9999-12-31')
   OR (is_current = 0 AND effective_to = DATE '9999-12-31')
UNION ALL
SELECT 'overlapping_effective_ranges', count(*)
FROM ordered
WHERE previous_effective_to IS NOT NULL
  AND effective_from <= previous_effective_to
ORDER BY check_name;

-- 3. Full-snapshot keys must match the current target keys for day-2.
SELECT 'branch_source_current_mismatch' AS check_name, count(*) AS violations
FROM (
    (SELECT branch_code FROM lakehouse.bronze.core_branch WHERE cob_dt = DATE '2026-01-01'
     EXCEPT
     SELECT branch_code FROM lakehouse.silver.dim_branch WHERE is_current = 1)
    UNION ALL
    (SELECT branch_code FROM lakehouse.silver.dim_branch WHERE is_current = 1
     EXCEPT
     SELECT branch_code FROM lakehouse.bronze.core_branch WHERE cob_dt = DATE '2026-01-01')
)
UNION ALL
SELECT 'product_source_current_mismatch', count(*)
FROM (
    (SELECT product_code FROM lakehouse.bronze.core_product WHERE cob_dt = DATE '2026-01-01'
     EXCEPT
     SELECT product_code FROM lakehouse.silver.dim_product WHERE is_current = 1)
    UNION ALL
    (SELECT product_code FROM lakehouse.silver.dim_product WHERE is_current = 1
     EXCEPT
     SELECT product_code FROM lakehouse.bronze.core_product WHERE cob_dt = DATE '2026-01-01')
)
UNION ALL
SELECT 'customer_source_current_mismatch', count(*)
FROM (
    (SELECT customer_id FROM lakehouse.bronze.core_customer WHERE cob_dt = DATE '2026-01-01'
     EXCEPT
     SELECT customer_id FROM lakehouse.silver.dim_customer WHERE is_current = 1)
    UNION ALL
    (SELECT customer_id FROM lakehouse.silver.dim_customer WHERE is_current = 1
     EXCEPT
     SELECT customer_id FROM lakehouse.bronze.core_customer WHERE cob_dt = DATE '2026-01-01')
)
UNION ALL
SELECT 'account_source_current_mismatch', count(*)
FROM (
    (SELECT account_id FROM lakehouse.bronze.core_account WHERE cob_dt = DATE '2026-01-01'
     EXCEPT
     SELECT account_id FROM lakehouse.silver.dim_account WHERE is_current = 1)
    UNION ALL
    (SELECT account_id FROM lakehouse.silver.dim_account WHERE is_current = 1
     EXCEPT
     SELECT account_id FROM lakehouse.bronze.core_account WHERE cob_dt = DATE '2026-01-01')
)
ORDER BY check_name;

-- 4. Controlled demo keys: each entity must show the day-1 and day-2 values.
SELECT 'branch' AS entity, branch_code AS business_key,
       manager_name AS value_1, address AS value_2, status AS value_3,
       effective_from, effective_to, is_current, branch_sk AS sk
FROM lakehouse.silver.dim_branch WHERE branch_code = 'BDG001'
UNION ALL
SELECT 'product', product_code, product_name, CAST(is_active AS varchar), currency,
       effective_from, effective_to, is_current, product_sk
FROM lakehouse.silver.dim_product WHERE product_code = 'CASA002'
UNION ALL
SELECT 'customer', CAST(customer_id AS varchar), phone, address, customer_segment,
       effective_from, effective_to, is_current, customer_sk
FROM lakehouse.silver.dim_customer WHERE customer_id = 10001
UNION ALL
SELECT 'account', CAST(account_id AS varchar), branch_code, status, CAST(balance AS varchar),
       effective_from, effective_to, is_current, account_sk
FROM lakehouse.silver.dim_account WHERE account_id = 100001
ORDER BY entity, effective_from;

-- 5. Technical-only source change must not create a new version.
SELECT customer_id,
       count(*) AS versions,
       count_if(is_current = 1) AS current_versions
FROM lakehouse.silver.dim_customer
WHERE customer_id = 10002
GROUP BY customer_id;
