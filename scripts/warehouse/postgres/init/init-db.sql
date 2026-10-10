-- Initialize Bronze, Silver, and Gold schemas for AdventureWorks Analytics Platform

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS bronze_staging;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS silver_staging;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create schema metadata tables in bronze for tracking
CREATE TABLE IF NOT EXISTS bronze.schema_version (
    schema_name VARCHAR(50) NOT NULL,
    version VARCHAR(20) NOT NULL,
    migration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (schema_name, version)
);

CREATE TABLE IF NOT EXISTS bronze.platform_schema_version (
    component VARCHAR(128) PRIMARY KEY,
    version VARCHAR(64) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO bronze.platform_schema_version (component, version)
VALUES ('platform', '1')
ON CONFLICT (component) DO NOTHING;

-- Create lineage tracking table in bronze
CREATE TABLE IF NOT EXISTS bronze.load_audit (
    load_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_count BIGINT,
    status VARCHAR(50),
    error_message TEXT
);

-- Create a metadata table to track schema changes
CREATE TABLE IF NOT EXISTS bronze.column_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100),
    is_nullable BOOLEAN,
    primary_key BOOLEAN,
    load_date DATE,
    PRIMARY KEY (table_name, column_name)
);

CREATE TABLE IF NOT EXISTS bronze.ingestion_checkpoint (
    batch_id VARCHAR(128) PRIMARY KEY,
    upper_bound TEXT NOT NULL,
    committed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.pipeline_run_audit (
    run_id VARCHAR(128) PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    mode VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error_count INTEGER NOT NULL DEFAULT 0,
    error_type VARCHAR(255),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS bronze.table_load_audit (
    load_id VARCHAR(128) PRIMARY KEY,
    run_id VARCHAR(128) NOT NULL REFERENCES bronze.pipeline_run_audit(run_id),
    stage VARCHAR(50) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_written BIGINT NOT NULL DEFAULT 0,
    rows_rejected BIGINT NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error_type VARCHAR(255),
    error_message TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.batch_load_audit (
    batch_id VARCHAR(128) PRIMARY KEY,
    load_id VARCHAR(128) NOT NULL REFERENCES bronze.table_load_audit(load_id),
    batch_number INTEGER NOT NULL,
    lower_bound TEXT,
    upper_bound TEXT,
    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_written BIGINT NOT NULL DEFAULT 0,
    rows_rejected BIGINT NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    committed_at TIMESTAMPTZ,
    content_hash VARCHAR(128),
    duration_ms BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.ingestion_batch_registry (
    batch_id VARCHAR(128) PRIMARY KEY,
    content_hash VARCHAR(128) NOT NULL,
    upper_bound TEXT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.rejected_records (
    rejected_id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(128) NOT NULL,
    load_id VARCHAR(128) NOT NULL,
    batch_id VARCHAR(128) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    record_key VARCHAR(255),
    source_hash VARCHAR(128),
    reason TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL,
    transform_version VARCHAR(128) NOT NULL DEFAULT 'unknown',
    error_type VARCHAR(128) NOT NULL DEFAULT 'TransformationError',
    UNIQUE (run_id, load_id, batch_id, record_key, source_hash)
);

-- Grant permissions (if needed, adjust as per your setup)
GRANT ALL PRIVILEGES ON SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA bronze_staging TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA gold TO postgres;

-- Insert schema version records
INSERT INTO bronze.schema_version (schema_name, version) VALUES ('bronze', '1.0.0') ON CONFLICT DO NOTHING;
INSERT INTO bronze.schema_version (schema_name, version) VALUES ('silver', '1.0.0') ON CONFLICT DO NOTHING;
INSERT INTO bronze.schema_version (schema_name, version) VALUES ('gold', '1.0.0') ON CONFLICT DO NOTHING;
