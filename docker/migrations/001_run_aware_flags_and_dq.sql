-- Idempotent migration for an existing PostgreSQL demo volume.

ALTER TABLE opslakehouse.flag_job_etl
    ADD COLUMN IF NOT EXISTS dag_run_id VARCHAR(300),
    ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR(300),
    ADD COLUMN IF NOT EXISTS try_number INTEGER DEFAULT 1,
    ADD COLUMN IF NOT EXISTS error_detail TEXT;

UPDATE opslakehouse.flag_job_etl
SET dag_run_id = COALESCE(dag_run_id, 'legacy:' || id::text),
    pipeline_run_id = COALESCE(pipeline_run_id, 'legacy:' || cob_dt::text),
    try_number = COALESCE(try_number, 1)
WHERE dag_run_id IS NULL
   OR pipeline_run_id IS NULL
   OR try_number IS NULL;

ALTER TABLE opslakehouse.flag_job_etl
    ALTER COLUMN dag_run_id SET NOT NULL,
    ALTER COLUMN pipeline_run_id SET NOT NULL,
    ALTER COLUMN try_number SET NOT NULL;

ALTER TABLE opslakehouse.flag_job_etl DROP CONSTRAINT IF EXISTS chk_flag_status;
ALTER TABLE opslakehouse.flag_job_etl
    ADD CONSTRAINT chk_flag_status CHECK (status IN ('R', 'S', 'F'));

DROP INDEX IF EXISTS opslakehouse.idx_flag_lookup;
CREATE INDEX IF NOT EXISTS idx_flag_latest_run
    ON opslakehouse.flag_job_etl(job_name, cob_dt, pipeline_run_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_flag_dag_run
    ON opslakehouse.flag_job_etl(dag_run_id);

CREATE TABLE IF NOT EXISTS opslakehouse.dq_check_result (
    id               BIGSERIAL       PRIMARY KEY,
    check_name       VARCHAR(200)    NOT NULL,
    table_name       VARCHAR(200)    NOT NULL,
    schema_name      VARCHAR(20)     NOT NULL,
    cob_dt           DATE            NOT NULL,
    pipeline_run_id  VARCHAR(300)    NOT NULL,
    dq_run_id        VARCHAR(400)    NOT NULL,
    severity         VARCHAR(20)     NOT NULL,
    check_type       VARCHAR(50)     NOT NULL,
    passed           BOOLEAN         NOT NULL,
    failed_count     BIGINT          NOT NULL DEFAULT 0,
    total_count      BIGINT          NOT NULL DEFAULT 0,
    error_detail     TEXT,
    executed_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_dq_severity CHECK (severity IN ('critical', 'warning'))
);

ALTER TABLE opslakehouse.dq_check_result
    ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR(300),
    ADD COLUMN IF NOT EXISTS dq_run_id VARCHAR(400),
    ADD COLUMN IF NOT EXISTS severity VARCHAR(20);

UPDATE opslakehouse.dq_check_result
SET pipeline_run_id = COALESCE(pipeline_run_id, 'legacy:' || cob_dt::text),
    dq_run_id = COALESCE(dq_run_id, 'legacy:' || id::text),
    severity = COALESCE(severity, 'critical')
WHERE pipeline_run_id IS NULL OR dq_run_id IS NULL OR severity IS NULL;

ALTER TABLE opslakehouse.dq_check_result
    ALTER COLUMN pipeline_run_id SET NOT NULL,
    ALTER COLUMN dq_run_id SET NOT NULL,
    ALTER COLUMN severity SET NOT NULL;

ALTER TABLE opslakehouse.dq_check_result DROP CONSTRAINT IF EXISTS chk_dq_severity;
ALTER TABLE opslakehouse.dq_check_result
    ADD CONSTRAINT chk_dq_severity CHECK (severity IN ('critical', 'warning'));

CREATE INDEX IF NOT EXISTS idx_dq_pipeline
    ON opslakehouse.dq_check_result(pipeline_run_id, cob_dt, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_failed
    ON opslakehouse.dq_check_result(passed, severity, cob_dt)
    WHERE passed = FALSE;
