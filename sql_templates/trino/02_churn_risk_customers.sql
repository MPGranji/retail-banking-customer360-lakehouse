-- =====================================================
-- Query 02: Churn Risk Customers — Retention Campaign
-- Use case : Risk/CRM phát hiện khách hàng "ngủ đông" để retention
-- Owner    : CRM / Relationship Manager Team
-- Source   : sandbox.mart_customer_360_masked + gold.churn_prediction
-- Expected : < 15 seconds
--
-- Business context:
--   10% khách hàng không giao dịch trong 90 ngày — nguy cơ churn cao.
--   Đặc biệt quan trọng với PRIORITY/VIP vì mất 1 khách VIP = mất
--   hàng tỷ AUM. Lọc những khách có AUM đáng kể để ưu tiên gọi lại.
-- =====================================================

SELECT
    m.customer_id,
    m.full_name_masked,
    m.primary_branch_code,
    m.customer_segment,
    ROUND(m.aum_total / 1000000, 1)    AS aum_million_vnd,
    m.aum_bucket,
    m.rfm_segment,
    ch.days_since_last_txn,
    ch.churn_risk,
    ch.txn_cnt_30d,
    ch.txn_cnt_90d,
    m.interaction_count_90d,
    m.primary_channel                   AS last_active_channel
FROM lakehouse.sandbox.mart_customer_360_masked m
JOIN lakehouse.gold.churn_prediction ch
    ON m.customer_id = ch.customer_id
    AND ch.cob_dt = m.cob_dt
WHERE m.churn_flag = 1                                        -- không giao dịch > 90 ngày
  AND m.aum_total >= 50000000                                 -- AUM > 50 triệu (đáng giữ lại)
  AND m.customer_segment IN ('PRIORITY', 'VIP')
  AND m.cob_dt = DATE '2025-12-31'
ORDER BY m.aum_total DESC, ch.days_since_last_txn DESC
LIMIT 500;
