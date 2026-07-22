-- =====================================================
-- Query 03: RFM Segment Distribution Report
-- Use case : Marketing báo cáo phân khúc khách hàng hàng tháng
-- Owner    : Marketing Analytics / BI Team
-- Source   : gold.rfm_segment + sandbox.mart_customer_360_masked
-- Expected : < 10 seconds
--
-- Business context:
--   Báo cáo tháng cho leadership: % khách hàng Champions tăng/giảm?
--   Có bao nhiêu khách "At Risk" cần hành động ngay?
--   AUM trung bình theo từng segment là bao nhiêu?
-- =====================================================

-- Phần 1: Phân bố segment
SELECT
    rfm_segment,
    COUNT(*)                                           AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total,
    ROUND(AVG(aum_total) / 1000000, 1)                AS avg_aum_million,
    ROUND(SUM(aum_total) / 1000000000, 1)             AS total_aum_billion,
    ROUND(AVG(txn_count_30d), 1)                      AS avg_txn_30d,
    ROUND(AVG(txn_amount_30d) / 1000000, 1)           AS avg_txn_amt_30d_million,
    COUNT(CASE WHEN has_credit_card = 0 THEN 1 END)   AS no_credit_card_cnt,
    COUNT(CASE WHEN churn_flag = 1 THEN 1 END)        AS churn_flag_cnt
FROM lakehouse.sandbox.mart_customer_360_masked
GROUP BY rfm_segment
ORDER BY
    CASE rfm_segment
        WHEN 'Champions'          THEN 1
        WHEN 'Loyal Customers'    THEN 2
        WHEN 'Potential Loyalists' THEN 3
        WHEN 'New Customers'      THEN 4
        WHEN 'At Risk'            THEN 5
        WHEN 'Hibernating'        THEN 6
        WHEN 'Lost'               THEN 7
        ELSE 8
    END;
