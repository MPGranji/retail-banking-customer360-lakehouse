-- =====================================================
-- Query 06: Product Penetration & Cross-sell Opportunity Analysis
-- Use case : Product Manager phân tích tỷ lệ sở hữu sản phẩm
--            và ước tính doanh thu tiềm năng từ cross-sell
-- Owner    : Product Management / Strategy Team
-- Source   : sandbox.mart_customer_360_masked
-- Expected : < 10 seconds
--
-- Business context:
--   Product Manager muốn biết: trong 10.000 khách hàng,
--   bao nhiêu % có thể upsell thêm credit card?
--   Revenue tiềm năng nếu chuyển đổi được 20% target?
--   Segment nào có tỷ lệ penetration thấp nhất (opportunity lớn nhất)?
-- =====================================================

WITH penetration AS (
    SELECT
        customer_segment,
        aum_bucket,
        COUNT(*)                                          AS total,
        -- Tỷ lệ sở hữu từng sản phẩm
        ROUND(AVG(has_credit_card) * 100, 1)             AS pct_has_credit_card,
        ROUND(AVG(has_savings) * 100, 1)                 AS pct_has_savings,
        ROUND(AVG(has_loan) * 100, 1)                    AS pct_has_loan,
        -- Cross-sell opportunity size
        SUM(CASE WHEN has_credit_card = 0 THEN 1 END)   AS opportunity_cc,
        SUM(CASE WHEN has_savings = 0 THEN 1 END)        AS opportunity_deposit,
        SUM(CASE WHEN has_loan = 0 THEN 1 END)           AS opportunity_loan,
        -- AUM của nhóm chưa có credit card (revenue potential)
        ROUND(SUM(CASE WHEN has_credit_card = 0
                       THEN aum_total ELSE 0 END) / 1000000000, 2)
                                                         AS cc_opportunity_aum_billion,
        -- Active customers trong nhóm chưa có CC
        SUM(CASE WHEN has_credit_card = 0
                      AND days_since_last_txn <= 30 THEN 1 END)
                                                         AS cc_opportunity_active_30d
    FROM lakehouse.sandbox.mart_customer_360_masked
    WHERE cob_dt = DATE '2025-12-31'
    GROUP BY customer_segment, aum_bucket
)
SELECT
    customer_segment,
    aum_bucket,
    total,
    pct_has_credit_card,
    pct_has_savings,
    pct_has_loan,
    opportunity_cc,
    opportunity_deposit,
    opportunity_loan,
    cc_opportunity_aum_billion,
    cc_opportunity_active_30d,
    -- Ước tính revenue nếu convert 20% opportunity (giả sử fee 300k/năm/thẻ)
    ROUND(opportunity_cc * 0.2 * 300000 / 1000000, 1)   AS estimated_fee_revenue_million_vnd
FROM penetration
ORDER BY
    CASE customer_segment
        WHEN 'VIP'      THEN 1
        WHEN 'PRIORITY' THEN 2
        ELSE 3
    END,
    CASE aum_bucket
        WHEN 'VIP'      THEN 1
        WHEN 'PRIORITY' THEN 2
        WHEN 'AFFLUENT' THEN 3
        ELSE 4
    END;
