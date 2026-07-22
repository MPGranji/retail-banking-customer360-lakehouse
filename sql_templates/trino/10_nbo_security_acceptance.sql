-- Run as data_engineer over HTTPS for the processing date under test.
SELECT current_user AS authenticated_user;

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(cross_sell_score) AS min_score,
    MAX(cross_sell_score) AS max_score,
    COUNT_IF(recommendation_reason IS NULL OR TRIM(recommendation_reason) = '') AS missing_reason,
    COUNT_IF(contact_eligible_flag = 0 AND suppression_reason IS NULL) AS invalid_suppression
FROM lakehouse.gold.campaign_target
WHERE cob_dt = DATE '2026-01-01';

SELECT campaign_priority, contact_eligible_flag, recommended_product, COUNT(*) AS customers
FROM lakehouse.gold.campaign_target
WHERE cob_dt = DATE '2026-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, customers DESC;

-- RBAC acceptance commands (execute separately and assert the expected result):
-- marketing:     SELECT current_user;                                                           -- ALLOW
-- marketing:     SELECT COUNT(*) FROM lakehouse.sandbox.mart_customer_360_masked;               -- ALLOW
-- marketing:     SELECT COUNT(*) FROM lakehouse.bronze.core_customer;                           -- DENY
-- data_engineer: SELECT current_user;                                                           -- ALLOW
-- data_engineer: SELECT COUNT(*) FROM lakehouse.bronze.core_customer;                           -- ALLOW
