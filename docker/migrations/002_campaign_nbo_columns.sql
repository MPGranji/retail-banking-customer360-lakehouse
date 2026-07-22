-- Additive migration for an existing campaign_target Iceberg table.
-- Fresh environments already receive these columns from the Gold DDL/reset helper.
ALTER TABLE lakehouse.gold.campaign_target ADD COLUMN IF NOT EXISTS cross_sell_score INTEGER;
ALTER TABLE lakehouse.gold.campaign_target ADD COLUMN IF NOT EXISTS recommended_product VARCHAR;
ALTER TABLE lakehouse.gold.campaign_target ADD COLUMN IF NOT EXISTS recommendation_reason VARCHAR;
ALTER TABLE lakehouse.gold.campaign_target ADD COLUMN IF NOT EXISTS campaign_priority VARCHAR;
ALTER TABLE lakehouse.gold.campaign_target ADD COLUMN IF NOT EXISTS contact_eligible_flag INTEGER;
ALTER TABLE lakehouse.gold.campaign_target ADD COLUMN IF NOT EXISTS suppression_reason VARCHAR;
