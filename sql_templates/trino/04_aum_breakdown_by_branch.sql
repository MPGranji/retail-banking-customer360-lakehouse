-- =====================================================
-- Query 04: AUM Breakdown by Branch & Region
-- Use case : Regional Manager xem hiệu quả AUM theo vùng/chi nhánh
-- Owner    : Retail Banking / Regional Managers
-- Source   : sandbox.mart_customer_360_masked
-- Expected : < 10 seconds
--
-- Business context:
--   Quản lý vùng muốn biết chi nhánh nào có AUM cao nhất,
--   và tiềm năng upsell từ khách AFFLUENT chưa có credit card.
-- =====================================================

SELECT
    -- Lấy prefix vùng từ branch_code (vd HCM, HAN, DAN)
    SUBSTR(primary_branch_code, 1, 3)                  AS region_prefix,
    primary_branch_code,
    COUNT(*)                                           AS total_customers,
    COUNT(CASE WHEN customer_segment = 'VIP'      THEN 1 END)  AS vip_count,
    COUNT(CASE WHEN customer_segment = 'PRIORITY' THEN 1 END)  AS priority_count,
    COUNT(CASE WHEN customer_segment = 'RETAIL'   THEN 1 END)  AS retail_count,
    ROUND(SUM(aum_total) / 1000000000, 2)             AS total_aum_billion,
    ROUND(AVG(aum_total) / 1000000, 1)                AS avg_aum_million,
    -- Cross-sell opportunity
    COUNT(CASE WHEN has_credit_card = 0
               AND aum_total >= 100000000 THEN 1 END) AS cross_sell_cc_opportunity,
    -- Churn risk
    COUNT(CASE WHEN churn_flag = 1 THEN 1 END)        AS churn_risk_count,
    -- Active customers (giao dịch trong 30 ngày)
    COUNT(CASE WHEN days_since_last_txn <= 30 THEN 1 END) AS active_30d
FROM lakehouse.sandbox.mart_customer_360_masked
GROUP BY
    SUBSTR(primary_branch_code, 1, 3),
    primary_branch_code
ORDER BY total_aum_billion DESC
LIMIT 50;
