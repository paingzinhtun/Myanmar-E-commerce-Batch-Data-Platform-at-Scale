CREATE TABLE IF NOT EXISTS monitoring.pipeline_run_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    pipeline_stage VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL,
    row_count BIGINT NULL,
    message TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
