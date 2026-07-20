-- Dashboard serving acceptance through Trino HTTPS.
-- Demo acceptance date: 2026-01-01.

-- 1. Grain and population.
WITH stats AS (
    SELECT COUNT(*) AS row_count,
           COUNT(DISTINCT customer_id) AS unique_customers
    FROM lakehouse.sandbox.mart_customer_360_dashboard
    WHERE cob_dt = DATE '2026-01-01'
), duplicates AS (
    SELECT COUNT(*) AS duplicate_keys
    FROM (
        SELECT customer_id, cob_dt
        FROM lakehouse.sandbox.mart_customer_360_dashboard
        WHERE cob_dt = DATE '2026-01-01'
        GROUP BY customer_id, cob_dt
        HAVING COUNT(*) > 1
    )
)
SELECT row_count, unique_customers, duplicate_keys,
       CASE WHEN row_count = 10000
                  AND unique_customers = 10000
                  AND duplicate_keys = 0
            THEN 'PASS' ELSE 'FAIL' END AS status
FROM stats CROSS JOIN duplicates;

-- 2. Population reconciliation against Gold history.
WITH gold AS (
    SELECT customer_id
    FROM lakehouse.gold.mart_customer_360_history
    WHERE cob_dt = DATE '2026-01-01'
), dashboard AS (
    SELECT customer_id
    FROM lakehouse.sandbox.mart_customer_360_dashboard
    WHERE cob_dt = DATE '2026-01-01'
), mismatch AS (
    SELECT COALESCE(g.customer_id, d.customer_id) AS customer_id
    FROM gold g FULL OUTER JOIN dashboard d ON g.customer_id = d.customer_id
    WHERE g.customer_id IS NULL OR d.customer_id IS NULL
)
SELECT COUNT(*) AS mismatches,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM mismatch;

-- 3. NBO and suppression contract.
SELECT COUNT(*) AS checked_rows,
       COUNT_IF(cross_sell_score IS NULL OR cross_sell_score NOT BETWEEN 0 AND 100) AS invalid_score,
       COUNT_IF(recommended_product IS NULL OR TRIM(recommended_product) = '') AS missing_product,
       COUNT_IF(recommendation_reason IS NULL OR TRIM(recommendation_reason) = '') AS missing_reason,
       COUNT_IF(campaign_priority IS NULL OR campaign_priority NOT IN ('HIGH', 'MEDIUM', 'LOW')) AS invalid_priority,
       COUNT_IF(contact_eligible_flag IS NULL OR contact_eligible_flag NOT IN (0, 1)
                OR (contact_eligible_flag = 1 AND suppression_reason IS NOT NULL)
                OR (contact_eligible_flag = 0 AND suppression_reason IS NULL)) AS invalid_suppression,
       CASE WHEN COUNT(*) = 10000
                  AND COUNT_IF(cross_sell_score IS NULL OR cross_sell_score NOT BETWEEN 0 AND 100) = 0
                  AND COUNT_IF(recommended_product IS NULL OR TRIM(recommended_product) = '') = 0
                  AND COUNT_IF(recommendation_reason IS NULL OR TRIM(recommendation_reason) = '') = 0
                  AND COUNT_IF(campaign_priority IS NULL OR campaign_priority NOT IN ('HIGH', 'MEDIUM', 'LOW')) = 0
                  AND COUNT_IF(contact_eligible_flag IS NULL OR contact_eligible_flag NOT IN (0, 1)
                         OR (contact_eligible_flag = 1 AND suppression_reason IS NOT NULL)
                         OR (contact_eligible_flag = 0 AND suppression_reason IS NULL)) = 0
            THEN 'PASS' ELSE 'FAIL' END AS status
FROM lakehouse.sandbox.mart_customer_360_dashboard
WHERE cob_dt = DATE '2026-01-01';

-- 4. Raw PII must not be exposed in the serving schema.
SELECT COUNT(*) AS exposed_raw_pii_columns,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM lakehouse.information_schema.columns
WHERE table_schema = 'sandbox'
  AND table_name = 'mart_customer_360_dashboard'
  AND column_name IN ('full_name', 'phone', 'email', 'cccd', 'address');

-- 5. DA-facing aggregate used to reconcile the future Superset dashboard.
SELECT recommended_product,
       campaign_priority,
       contact_eligible_flag,
       COUNT(*) AS customers,
       CAST(SUM(aum_total) AS DECIMAL(24,2)) AS total_aum,
       ROUND(AVG(cross_sell_score), 2) AS avg_cross_sell_score
FROM lakehouse.sandbox.mart_customer_360_dashboard
WHERE cob_dt = DATE '2026-01-01'
GROUP BY recommended_product, campaign_priority, contact_eligible_flag
ORDER BY customers DESC;

-- Security matrix to run separately:
-- marketing: SELECT COUNT(*) FROM lakehouse.sandbox.mart_customer_360_dashboard; -- ALLOW
-- marketing: SELECT COUNT(*) FROM lakehouse.gold.campaign_target;                -- DENY
