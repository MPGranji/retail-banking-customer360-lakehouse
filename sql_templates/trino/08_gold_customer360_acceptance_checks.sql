-- Customer 360 Gold acceptance suite (steps 9-11)
-- Run with Trino after Gold, segmentation and PII masking complete for both demo dates.

-- 1. Current/history/masked population and serving-date contract.
SELECT 'mart_customer_360' AS table_name,
       COUNT(*) AS row_count,
       COUNT(DISTINCT customer_id) AS customer_count,
       MIN(cob_dt) AS min_cob_dt,
       MAX(cob_dt) AS max_cob_dt
FROM lakehouse.gold.mart_customer_360
UNION ALL
SELECT 'mart_customer_360_history', COUNT(*), COUNT(DISTINCT customer_id), MIN(cob_dt), MAX(cob_dt)
FROM lakehouse.gold.mart_customer_360_history
UNION ALL
SELECT 'mart_customer_360_masked', COUNT(*), COUNT(DISTINCT customer_id), MIN(cob_dt), MAX(cob_dt)
FROM lakehouse.sandbox.mart_customer_360_masked;

-- 2. Uniqueness violations must all be zero.
SELECT check_name,
       violations,
       CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'current_duplicate_customer' AS check_name, COUNT(*) AS violations
    FROM (
        SELECT customer_id
        FROM lakehouse.gold.mart_customer_360
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )
    UNION ALL
    SELECT 'history_duplicate_customer_date', COUNT(*)
    FROM (
        SELECT customer_id, cob_dt
        FROM lakehouse.gold.mart_customer_360_history
        GROUP BY customer_id, cob_dt
        HAVING COUNT(*) > 1
    )
    UNION ALL
    SELECT 'campaign_duplicate_customer_date', COUNT(*)
    FROM (
        SELECT customer_id, cob_dt
        FROM lakehouse.gold.campaign_target
        GROUP BY customer_id, cob_dt
        HAVING COUNT(*) > 1
    )
) checks
ORDER BY check_name;

-- 3. History must retain both dates while current contains only the newest date.
SELECT cob_dt, COUNT(*) AS row_count, COUNT(DISTINCT customer_id) AS customer_count
FROM lakehouse.gold.mart_customer_360_history
GROUP BY cob_dt
ORDER BY cob_dt;

-- 4. At least 25 business columns in the current mart.
SELECT COUNT(*) AS total_columns,
       COUNT_IF(column_name NOT IN ('customer_id', 'customer_sk', 'cob_dt')) AS business_columns,
       CASE
           WHEN COUNT_IF(column_name NOT IN ('customer_id', 'customer_sk', 'cob_dt')) >= 25
           THEN 'PASS' ELSE 'FAIL'
       END AS status
FROM lakehouse.information_schema.columns
WHERE table_schema = 'gold'
  AND table_name = 'mart_customer_360';

-- 5. Balance/AUM reconciliation against independent source aggregates for both dates.
WITH dates(cob_dt) AS (
    VALUES DATE '2025-12-31', DATE '2026-01-01'
),
customers AS (
    SELECT d.cob_dt, c.customer_id
    FROM dates d
    JOIN lakehouse.silver.dim_customer c
      ON d.cob_dt BETWEEN c.effective_from AND c.effective_to
),
account_agg AS (
    SELECT d.cob_dt,
           a.customer_id,
           SUM(CASE WHEN a.status = 'ACTIVE' THEN a.balance ELSE 0 END) AS account_balance
    FROM dates d
    JOIN lakehouse.silver.dim_account a
      ON d.cob_dt BETWEEN a.effective_from AND a.effective_to
    GROUP BY d.cob_dt, a.customer_id
),
deposit_agg AS (
    SELECT customer_id,
           SUM(CASE WHEN status = 'ACTIVE' THEN principal_amount ELSE 0 END) AS deposit_principal
    FROM lakehouse.silver.dim_deposit
    GROUP BY customer_id
),
loan_agg AS (
    SELECT customer_id,
           SUM(CASE WHEN loan_status = 'ACTIVE' THEN outstanding_balance ELSE 0 END) AS loan_outstanding
    FROM lakehouse.silver.dim_loan
    GROUP BY customer_id
),
expected AS (
    SELECT c.cob_dt,
           c.customer_id,
           COALESCE(a.account_balance, DECIMAL '0') AS account_balance,
           COALESCE(d.deposit_principal, DECIMAL '0') AS deposit_principal,
           COALESCE(l.loan_outstanding, DECIMAL '0') AS loan_outstanding
    FROM customers c
    LEFT JOIN account_agg a
      ON c.cob_dt = a.cob_dt AND c.customer_id = a.customer_id
    LEFT JOIN deposit_agg d ON c.customer_id = d.customer_id
    LEFT JOIN loan_agg l ON c.customer_id = l.customer_id
)
SELECT COUNT(*) AS balance_mismatches,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM expected e
JOIN lakehouse.gold.customer_balance_summary g
  ON e.customer_id = g.customer_id AND e.cob_dt = g.cob_dt
WHERE g.total_account_balance <> CAST(e.account_balance AS DECIMAL(18,2))
   OR g.total_deposit_principal <> CAST(e.deposit_principal AS DECIMAL(18,2))
   OR g.total_loan_outstanding <> CAST(e.loan_outstanding AS DECIMAL(18,2))
   OR g.aum_total <> CAST(e.account_balance + e.deposit_principal AS DECIMAL(18,2));

-- 6. Card reconciliation. Convert timestamps to the Spark session business timezone before DATE comparison.
WITH dates(cob_dt) AS (
    VALUES DATE '2025-12-31', DATE '2026-01-01'
),
expected AS (
    SELECT d.cob_dt,
           t.customer_id,
           COUNT(t.txn_id) AS txn_count,
           CAST(ROUND(SUM(t.txn_amount), 2) AS DECIMAL(18,2)) AS txn_amount,
           COUNT(DISTINCT t.merchant_category) AS merchant_categories,
           MAX(t.txn_date) AS last_txn_date
    FROM dates d
    JOIN lakehouse.silver.fact_card_txn t
      ON CAST(AT_TIMEZONE(t.txn_date, 'Asia/Ho_Chi_Minh') AS DATE)
         BETWEEN DATE_ADD('day', -30, d.cob_dt) AND d.cob_dt
    WHERE t.status = 'SUCCESS'
      AND t.txn_type NOT IN ('REFUND', 'REVERSAL')
    GROUP BY d.cob_dt, t.customer_id
)
SELECT COUNT(*) AS card_mismatches,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM lakehouse.gold.customer_card_summary g
LEFT JOIN expected e
  ON g.cob_dt = e.cob_dt AND g.customer_id = e.customer_id
WHERE g.total_card_txn_count_30d <> COALESCE(e.txn_count, 0)
   OR g.total_card_txn_amount_30d <> COALESCE(e.txn_amount, DECIMAL '0')
   OR g.distinct_merchant_categories <> COALESCE(e.merchant_categories, 0)
   OR g.last_card_txn_date IS DISTINCT FROM e.last_txn_date;

-- 7. Current mart must agree with the independently built Gold summaries.
SELECT check_name,
       violations,
       CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'mart_balance_summary_mismatch' AS check_name, COUNT(*) AS violations
    FROM lakehouse.gold.mart_customer_360 m
    JOIN lakehouse.gold.customer_balance_summary b
      ON m.customer_id = b.customer_id AND m.cob_dt = b.cob_dt
    WHERE m.aum_total <> b.aum_total
       OR m.total_loan_outstanding <> b.total_loan_outstanding
    UNION ALL
    SELECT 'mart_transaction_summary_mismatch', COUNT(*)
    FROM lakehouse.gold.mart_customer_360 m
    JOIN lakehouse.gold.customer_transaction_summary t
      ON m.customer_id = t.customer_id AND m.cob_dt = t.cob_dt
    WHERE m.txn_count_30d <> t.total_txn_count_30d
       OR m.txn_amount_30d <> t.total_txn_amount_30d
) checks
ORDER BY check_name;

-- 8. RFM ranges, population and segment reachability.
SELECT cob_dt,
       COUNT(*) AS scored_customers,
       MIN(r_score) AS min_r,
       MAX(r_score) AS max_r,
       MIN(f_score) AS min_f,
       MAX(f_score) AS max_f,
       MIN(m_score) AS min_m,
       MAX(m_score) AS max_m,
       MIN(rfm_score) AS min_rfm,
       MAX(rfm_score) AS max_rfm
FROM lakehouse.gold.rfm_segment
GROUP BY cob_dt
ORDER BY cob_dt;

SELECT cob_dt, rfm_segment, COUNT(*) AS customer_count
FROM lakehouse.gold.rfm_segment
GROUP BY cob_dt, rfm_segment
ORDER BY cob_dt, customer_count DESC;

-- 9. Segmentation flags must be binary and every target table populated.
SELECT check_name,
       violations,
       CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'invalid_cross_sell_flags' AS check_name, COUNT(*) AS violations
    FROM lakehouse.gold.cross_sell_segment
    WHERE no_credit_card NOT IN (0, 1)
       OR no_deposit NOT IN (0, 1)
       OR no_loan NOT IN (0, 1)
    UNION ALL
    SELECT 'invalid_churn_flags', COUNT(*)
    FROM lakehouse.gold.churn_prediction
    WHERE is_churn_candidate NOT IN (0, 1)
    UNION ALL
    SELECT 'empty_rfm_segment', CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM lakehouse.gold.rfm_segment
    UNION ALL
    SELECT 'empty_churn_prediction', CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM lakehouse.gold.churn_prediction
    UNION ALL
    SELECT 'empty_cross_sell_segment', CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM lakehouse.gold.cross_sell_segment
    UNION ALL
    SELECT 'empty_campaign_target', CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
    FROM lakehouse.gold.campaign_target
) checks
ORDER BY check_name;

-- 10. Marketing mart must not expose raw PII fields.
SELECT COUNT(*) AS raw_pii_columns,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM lakehouse.information_schema.columns
WHERE table_schema = 'sandbox'
  AND table_name = 'mart_customer_360_masked'
  AND column_name IN ('full_name', 'phone', 'email', 'cccd', 'address');

-- 11. Controlled customer demonstrates two distinct historical snapshots.
SELECT customer_id,
       cob_dt,
       primary_branch_code,
       customer_segment,
       aum_total,
       rfm_segment
FROM lakehouse.gold.mart_customer_360_history
WHERE customer_id = 10001
ORDER BY cob_dt;

-- 12. Consumer isolation: demo Trino should expose only lakehouse and system catalogs.
SHOW CATALOGS;
