-- =====================================================
-- Query 01: Cross-sell Credit Card Candidates
-- Use case : Marketing chạy chiến dịch "Cross-sell thẻ tín dụng Q4"
-- Owner    : Marketing / CRM Team
-- Source   : sandbox.mart_customer_360_masked
-- Expected : < 10 seconds
--
-- Business context:
--   Team Marketing cần 1.000 khách hàng để gọi điện tư vấn mở thẻ tín dụng.
--   Điều kiện: tổng tiền gửi > 100 triệu VND, chưa có thẻ tín dụng,
--   RFM segment tốt, có giao dịch gần đây, ưu tiên chi nhánh TP.HCM.
--   Trước Lakehouse: chờ DBA 2-3 ngày.
--   Sau Lakehouse: Marketing tự query Trino < 10 giây.
-- =====================================================

SELECT
    customer_id,
    full_name_masked,
    primary_branch_code,
    customer_segment,
    ROUND(aum_total / 1000000, 1)      AS aum_million_vnd,
    aum_bucket,
    rfm_segment,
    txn_count_30d,
    ROUND(txn_amount_30d / 1000000, 1) AS txn_amount_30d_million,
    days_since_last_txn,
    last_txn_date,
    primary_channel,
    interaction_count_90d
FROM lakehouse.sandbox.mart_customer_360_masked
WHERE has_credit_card = 0
  AND aum_total > 100000000                                    -- > 100 triệu VND
  AND rfm_segment IN ('Champions', 'Loyal Customers')
  AND days_since_last_txn <= 30
  AND primary_branch_code LIKE 'HCM%'
  AND cob_dt = DATE '2025-12-31'                              -- thay bằng ngày cần query
ORDER BY aum_total DESC
LIMIT 1000;
