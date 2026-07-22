-- =====================================================
-- Query 05: Single Customer 360° Profile
-- Use case : CSR/RM xem toàn bộ hồ sơ khách hàng trước cuộc gọi
-- Owner    : Customer Service / Relationship Managers
-- Source   : sandbox.mart_customer_360_masked + gold.campaign_target
-- Expected : < 3 seconds (single customer lookup)
--
-- Business context:
--   Nhân viên CSR đang trên điện thoại với khách hàng.
--   Họ cần tóm tắt nhanh: sản phẩm đang dùng, AUM, giao dịch gần đây,
--   RFM segment, và đề xuất cross-sell phù hợp — tất cả trong < 3 giây.
-- =====================================================

SELECT
    -- Profile
    m.customer_id,
    m.full_name_masked,
    m.age,
    m.gender,
    m.customer_segment,
    m.primary_branch_code,
    m.kyc_status,
    -- Product holding
    m.total_accounts,
    m.total_cards,
    m.total_loans,
    m.has_credit_card,
    m.has_savings,
    m.has_loan,
    -- AUM
    ROUND(m.aum_total / 1000000, 1)    AS aum_million_vnd,
    m.aum_bucket,
    ROUND(m.total_deposit_balance / 1000000, 1)  AS deposit_balance_million,
    ROUND(m.total_loan_outstanding / 1000000, 1) AS loan_outstanding_million,
    -- Transaction behavior
    m.txn_count_30d,
    ROUND(m.txn_amount_30d / 1000000, 1) AS txn_amount_30d_million,
    m.days_since_last_txn,
    m.primary_channel,
    -- CRM
    m.interaction_count_90d,
    m.last_interaction_date,
    -- RFM & Segmentation
    m.rfm_recency_score,
    m.rfm_frequency_score,
    m.rfm_monetary_score,
    m.rfm_segment,
    -- Risk & Opportunity
    m.churn_flag,
    m.cross_sell_credit_card_flag,
    -- Campaign recommendation
    ct.campaign_type                    AS recommended_campaign
FROM lakehouse.sandbox.mart_customer_360_masked m
LEFT JOIN lakehouse.gold.campaign_target ct
    ON m.customer_id = ct.customer_id
    AND ct.cob_dt = m.cob_dt
WHERE m.customer_id = 10001              -- thay bằng customer_id cần tra cứu
  AND m.cob_dt = DATE '2025-12-31';
