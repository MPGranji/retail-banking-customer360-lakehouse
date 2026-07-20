-- Canonical mirror of docker/init_postgres/02_ddl_ops.sql.

CREATE TABLE opslakehouse.flag_job_etl (
    id               BIGSERIAL       PRIMARY KEY,
    job_name         VARCHAR(200)    NOT NULL,
    schema_name      VARCHAR(20)     NOT NULL,
    table_name       VARCHAR(200)    NOT NULL,
    status           CHAR(1)         NOT NULL,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    cob_dt           DATE            NOT NULL,
    dag_run_id       VARCHAR(300)    NOT NULL,
    pipeline_run_id  VARCHAR(300)    NOT NULL,
    try_number       INTEGER         NOT NULL DEFAULT 1,
    error_detail     TEXT,
    created_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_flag_status CHECK (status IN ('R', 'S', 'F'))
);

CREATE INDEX idx_flag_latest_run
    ON opslakehouse.flag_job_etl(job_name, cob_dt, pipeline_run_id, id DESC);
CREATE INDEX idx_flag_dag_run
    ON opslakehouse.flag_job_etl(dag_run_id);

COMMENT ON TABLE opslakehouse.flag_job_etl IS
    'Immutable ETL run events; R=Running, S=Success, F=Failed';

CREATE TABLE opslakehouse.dq_check_result (
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

CREATE INDEX idx_dq_pipeline
    ON opslakehouse.dq_check_result(pipeline_run_id, cob_dt, executed_at DESC);
CREATE INDEX idx_dq_failed
    ON opslakehouse.dq_check_result(passed, severity, cob_dt)
    WHERE passed = FALSE;
