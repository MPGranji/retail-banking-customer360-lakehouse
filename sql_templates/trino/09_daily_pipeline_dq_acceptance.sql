-- Daily pipeline and automated DQ acceptance suite (steps 12-13).
-- Run after lakehouse_daily_pipeline_dag succeeds for cob_dt = 2026-01-01.

-- 1. Daily fact rerun must preserve the historical population and business keys.
SELECT 'fact_txn_account' AS table_name,
       COUNT(*) AS row_count,
       COUNT(DISTINCT txn_id) AS business_key_count,
       MIN(CAST(AT_TIMEZONE(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)) AS min_event_date,
       MAX(CAST(AT_TIMEZONE(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)) AS max_event_date
FROM lakehouse.silver.fact_txn_account
UNION ALL
SELECT 'fact_card_txn', COUNT(*), COUNT(DISTINCT txn_id),
       MIN(CAST(AT_TIMEZONE(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)),
       MAX(CAST(AT_TIMEZONE(txn_date, 'Asia/Ho_Chi_Minh') AS DATE))
FROM lakehouse.silver.fact_card_txn
UNION ALL
SELECT 'fact_crm_interaction', COUNT(*), COUNT(DISTINCT interaction_id),
       MIN(CAST(AT_TIMEZONE(interaction_date, 'Asia/Ho_Chi_Minh') AS DATE)),
       MAX(CAST(AT_TIMEZONE(interaction_date, 'Asia/Ho_Chi_Minh') AS DATE))
FROM lakehouse.silver.fact_crm_interaction
ORDER BY table_name;

-- 2. No fact row may lose the surrogate key resolved from the SCD2 event date.
SELECT check_name,
       violations,
       CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'fact_txn_account_orphan_sk' AS check_name, COUNT(*) AS violations
    FROM lakehouse.silver.fact_txn_account
    WHERE account_sk IS NULL OR customer_sk IS NULL
    UNION ALL
    SELECT 'fact_card_txn_orphan_sk', COUNT(*)
    FROM lakehouse.silver.fact_card_txn
    WHERE customer_sk IS NULL
    UNION ALL
    SELECT 'fact_crm_interaction_orphan_sk', COUNT(*)
    FROM lakehouse.silver.fact_crm_interaction
    WHERE customer_sk IS NULL
) checks
ORDER BY check_name;

-- 3. The final serving layers must represent exactly the requested business date.
SELECT 'mart_customer_360' AS table_name,
       COUNT(*) AS row_count,
       COUNT(DISTINCT customer_id) AS customer_count,
       MIN(cob_dt) AS min_cob_dt,
       MAX(cob_dt) AS max_cob_dt
FROM lakehouse.gold.mart_customer_360
UNION ALL
SELECT 'mart_customer_360_masked', COUNT(*), COUNT(DISTINCT customer_id),
       MIN(cob_dt), MAX(cob_dt)
FROM lakehouse.sandbox.mart_customer_360_masked
ORDER BY table_name;

-- 4. All same-day marketing outputs must have the same population as current mart.
WITH expected AS (
    SELECT COUNT(*) AS n
    FROM lakehouse.gold.mart_customer_360
    WHERE cob_dt = DATE '2026-01-01'
), populations AS (
    SELECT 'rfm_segment' AS table_name, COUNT(*) AS n
    FROM lakehouse.gold.rfm_segment WHERE cob_dt = DATE '2026-01-01'
    UNION ALL
    SELECT 'churn_prediction', COUNT(*)
    FROM lakehouse.gold.churn_prediction WHERE cob_dt = DATE '2026-01-01'
    UNION ALL
    SELECT 'cross_sell_segment', COUNT(*)
    FROM lakehouse.gold.cross_sell_segment WHERE cob_dt = DATE '2026-01-01'
    UNION ALL
    SELECT 'campaign_target', COUNT(*)
    FROM lakehouse.gold.campaign_target WHERE cob_dt = DATE '2026-01-01'
)
SELECT p.table_name,
       p.n AS row_count,
       e.n AS expected_count,
       CASE WHEN p.n = e.n THEN 'PASS' ELSE 'FAIL' END AS status
FROM populations p
CROSS JOIN expected e
ORDER BY p.table_name;

-- 5. Marketing consumers must not see raw customer PII.
SELECT COUNT(*) AS raw_pii_columns,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM lakehouse.information_schema.columns
WHERE table_schema = 'sandbox'
  AND table_name = 'mart_customer_360_masked'
  AND column_name IN ('full_name', 'phone', 'email', 'cccd', 'address');
