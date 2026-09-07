from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector


PLATFORM_SCHEMA_VERSION = "1"


def ensure_ingestion_schema(settings: Settings | None = None) -> dict[str, object]:
    """Apply non-destructive platform metadata DDL and record its version.

    This helper owns only schemas and ingestion metadata. Published Bronze,
    Silver, and Gold data are never dropped, truncated, or replaced here.
    """
    with PostgreSQLConnector(settings=settings or get_settings()) as connection:
        connection.execute_query("CREATE SCHEMA IF NOT EXISTS bronze")
        connection.execute_query("CREATE SCHEMA IF NOT EXISTS silver")
        connection.execute_query("CREATE SCHEMA IF NOT EXISTS gold")
        connection.execute_query("CREATE SCHEMA IF NOT EXISTS bronze_staging")
        connection.execute_query("CREATE SCHEMA IF NOT EXISTS silver_staging")
        connection.execute_query(
            """
            CREATE TABLE IF NOT EXISTS bronze.platform_schema_version (
                component VARCHAR(128) PRIMARY KEY,
                version VARCHAR(64) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute_query(
            """
            INSERT INTO bronze.platform_schema_version (component, version)
            VALUES ('platform', %s)
            ON CONFLICT (component) DO NOTHING
            """,
            (PLATFORM_SCHEMA_VERSION,),
        )
        connection.execute_query(
            """
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
            )
            """
        )
        connection.execute_query(
            """
            CREATE TABLE IF NOT EXISTS bronze.table_load_audit (
                load_id VARCHAR(128) PRIMARY KEY,
                run_id VARCHAR(128) NOT NULL,
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
            )
            """
        )
        connection.execute_query(
            """
            CREATE TABLE IF NOT EXISTS bronze.batch_load_audit (
                batch_id VARCHAR(128) PRIMARY KEY,
                load_id VARCHAR(128) NOT NULL,
                batch_number INTEGER NOT NULL,
                lower_bound TEXT,
                upper_bound TEXT,
                rows_read BIGINT NOT NULL DEFAULT 0,
                rows_written BIGINT NOT NULL DEFAULT 0,
                rows_rejected BIGINT NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                status VARCHAR(50) NOT NULL,
                committed_at TIMESTAMPTZ
                ,duration_ms BIGINT
            )
            """
        )
        connection.execute_query(
            "ALTER TABLE bronze.pipeline_run_audit "
            "ADD COLUMN IF NOT EXISTS error_type VARCHAR(255)"
        )
        connection.execute_query(
            "ALTER TABLE bronze.pipeline_run_audit "
            "ADD COLUMN IF NOT EXISTS error_message TEXT"
        )
        connection.execute_query(
            "ALTER TABLE bronze.table_load_audit "
            "ADD COLUMN IF NOT EXISTS duration_ms BIGINT"
        )
        connection.execute_query(
            "ALTER TABLE bronze.batch_load_audit "
            "ADD COLUMN IF NOT EXISTS duration_ms BIGINT"
        )
        connection.execute_query(
            """
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
            )
            """
        )
        connection.execute_query(
            "ALTER TABLE bronze.rejected_records "
            "ADD COLUMN IF NOT EXISTS transform_version VARCHAR(128) NOT NULL DEFAULT 'unknown'"
        )
        connection.execute_query(
            "ALTER TABLE bronze.rejected_records "
            "ADD COLUMN IF NOT EXISTS error_type VARCHAR(128) NOT NULL DEFAULT 'TransformationError'"
        )
        connection.execute_query(
            "ALTER TABLE bronze.batch_load_audit "
            "ADD COLUMN IF NOT EXISTS content_hash VARCHAR(128)"
        )
        connection.execute_query(
            """
            CREATE TABLE IF NOT EXISTS bronze.ingestion_batch_registry (
                batch_id VARCHAR(128) PRIMARY KEY,
                content_hash VARCHAR(128) NOT NULL,
                upper_bound TEXT NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    return {
        "schema_version": PLATFORM_SCHEMA_VERSION,
        "migration": "platform_metadata_v1",
        "objects": [
            "bronze",
            "silver",
            "gold",
            "bronze_staging",
            "silver_staging",
            "bronze.platform_schema_version",
            "bronze.pipeline_run_audit",
            "bronze.table_load_audit",
            "bronze.batch_load_audit",
            "bronze.rejected_records",
            "bronze.ingestion_batch_registry",
        ],
    }