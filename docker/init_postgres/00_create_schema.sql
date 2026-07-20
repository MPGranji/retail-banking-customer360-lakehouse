-- =============================================================================
-- DDL: Khởi tạo schemas cho PostgreSQL 15
-- File này chạy đầu tiên (thứ tự alphabetical: 00_)
-- Schemas:
--   card_crm     — nguồn dữ liệu Card & CRM (3 bảng nguồn)
--   opslakehouse — bảng vận hành: flag_job_etl, audit log
-- =============================================================================

-- Schema cho Card + CRM
CREATE SCHEMA IF NOT EXISTS card_crm;

-- Schema cho bảng vận hành Lakehouse
CREATE SCHEMA IF NOT EXISTS opslakehouse;

-- Comment schema
COMMENT ON SCHEMA card_crm     IS 'Card system và CRM interaction data';
COMMENT ON SCHEMA opslakehouse IS 'Operational tables: ETL flags, audit logs, DQ results';
