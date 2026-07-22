-- =============================================================================
-- DDL: Operational Tables (PostgreSQL 15)
-- Schema: opslakehouse
-- Mục đích: Tracking ETL pipeline — flags, audit, data quality
-- =============================================================================

-- =============================================================================
-- 1. Bảng FLAG_JOB_ETL — theo dõi trạng thái từng job ETL
--    Quy tắc: INSERT only, không UPDATE (immutable audit trail)
--    - INSERT R row khi job bắt đầu
--    - INSERT S row khi job hoàn thành
--    - Silver/Gold dùng bảng này để check upstream dependency (SqlSensor)
-- =============================================================================
CREATE TABLE opslakehouse.flag_job_etl (
    id          BIGSERIAL       NOT NULL,
    job_name    VARCHAR(200)    NOT NULL,   -- DAG id, vd 'bronze_core_banking_dag'
    schema_name VARCHAR(20)     NOT NULL,   -- 'bronze' / 'silver' / 'gold'
    table_name  VARCHAR(200)    NOT NULL,   -- tên bảng target, vd 'core_customer'
    status      CHAR(1)         NOT NULL,   -- 'R' Running / 'S' Success
    start_time  TIMESTAMP,                  -- điền khi status = R
    end_time    TIMESTAMP,                  -- điền khi status = S
    cob_dt      DATE            NOT NULL,   -- ngày xử lý
    created_at  TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_flag_job_etl  PRIMARY KEY (id),
    CONSTRAINT chk_flag_status  CHECK (status IN ('R', 'S'))
);

-- Index cho SqlSensor query: WHERE schema_name=? AND table_name=? AND status='S' AND cob_dt=?
CREATE INDEX idx_flag_lookup ON opslakehouse.flag_job_etl(schema_name, table_name, status, cob_dt);
CREATE INDEX idx_flag_cobdt  ON opslakehouse.flag_job_etl(cob_dt);

COMMENT ON TABLE  opslakehouse.flag_job_etl           IS 'ETL job execution flags — INSERT only, no UPDATE';
COMMENT ON COLUMN opslakehouse.flag_job_etl.status    IS 'R=Running, S=Success';
COMMENT ON COLUMN opslakehouse.flag_job_etl.schema_name IS 'bronze / silver / gold';
