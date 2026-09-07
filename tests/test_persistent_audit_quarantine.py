from datetime import datetime, timezone

import pytest

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.audit_service import PostgresAuditService
from src.shared.ingestion.ingestion_models import (
    BatchLoadAudit,
    IngestionStatus,
    RejectedRecord,
    RunAudit,
    TableLoadAudit,
)
from src.shared.ingestion.quarantine_service import PostgresQuarantineService


pytestmark = pytest.mark.integration


RUN_ID = "persistent-test-run"
LOAD_ID = "persistent-test-load"
BATCH_ID = "persistent-test-batch"


@pytest.fixture(autouse=True)
def persistent_tables():
    with PostgreSQLConnector() as connection:
        connection.execute_query(
            "CREATE TABLE IF NOT EXISTS bronze.pipeline_run_audit ("
            "run_id VARCHAR(128) PRIMARY KEY, pipeline_name VARCHAR(255) NOT NULL, "
            "mode VARCHAR(50) NOT NULL, status VARCHAR(50) NOT NULL, "
            "started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ, "
            "error_count INTEGER NOT NULL DEFAULT 0)"
        )
        connection.execute_query(
            "CREATE TABLE IF NOT EXISTS bronze.table_load_audit ("
            "load_id VARCHAR(128) PRIMARY KEY, run_id VARCHAR(128) NOT NULL, "
            "stage VARCHAR(50) NOT NULL, source_table VARCHAR(255) NOT NULL, "
            "target_table VARCHAR(255) NOT NULL, status VARCHAR(50) NOT NULL, "
            "rows_read BIGINT NOT NULL DEFAULT 0, rows_written BIGINT NOT NULL DEFAULT 0, "
            "rows_rejected BIGINT NOT NULL DEFAULT 0, attempt_count INTEGER NOT NULL DEFAULT 0, "
            "error_type VARCHAR(255), error_message TEXT, started_at TIMESTAMPTZ, "
            "finished_at TIMESTAMPTZ)"
        )
        connection.execute_query(
            "CREATE TABLE IF NOT EXISTS bronze.batch_load_audit ("
            "batch_id VARCHAR(128) PRIMARY KEY, load_id VARCHAR(128) NOT NULL, "
            "batch_number INTEGER NOT NULL, lower_bound TEXT, upper_bound TEXT, "
            "rows_read BIGINT NOT NULL DEFAULT 0, rows_written BIGINT NOT NULL DEFAULT 0, "
            "rows_rejected BIGINT NOT NULL DEFAULT 0, attempt_count INTEGER NOT NULL DEFAULT 0, "
            "status VARCHAR(50) NOT NULL, committed_at TIMESTAMPTZ)"
        )
        connection.execute_query(
            "CREATE TABLE IF NOT EXISTS bronze.rejected_records ("
            "rejected_id BIGSERIAL PRIMARY KEY, run_id VARCHAR(128) NOT NULL, "
            "load_id VARCHAR(128) NOT NULL, batch_id VARCHAR(128) NOT NULL, "
            "source_table VARCHAR(255) NOT NULL, record_key VARCHAR(255), "
            "source_hash VARCHAR(128), reason TEXT NOT NULL, "
            "rejected_at TIMESTAMPTZ NOT NULL, "
            "UNIQUE (run_id, load_id, batch_id, record_key, source_hash))"
        )
        for table in ("rejected_records", "batch_load_audit", "table_load_audit", "pipeline_run_audit"):
            connection.execute_query(
                f"DELETE FROM bronze.{table} WHERE "
                + ("run_id = %s" if table != "batch_load_audit" else "batch_id = %s"),
                (RUN_ID if table != "batch_load_audit" else BATCH_ID,),
            )
    yield
    with PostgreSQLConnector() as connection:
        connection.execute_query(
            "DELETE FROM bronze.rejected_records WHERE run_id = %s", (RUN_ID,)
        )
        connection.execute_query(
            "DELETE FROM bronze.batch_load_audit WHERE batch_id = %s", (BATCH_ID,)
        )
        connection.execute_query(
            "DELETE FROM bronze.table_load_audit WHERE load_id = %s", (LOAD_ID,)
        )
        connection.execute_query(
            "DELETE FROM bronze.pipeline_run_audit WHERE run_id = %s", (RUN_ID,)
        )


def test_persistent_audit_and_quarantine_survive_service_recreation():
    now = datetime.now(timezone.utc)
    audit = PostgresAuditService()
    audit.record_run(RunAudit(RUN_ID, "bronze", "full", IngestionStatus.SUCCESS, now))
    audit.record_table_load(
        TableLoadAudit(LOAD_ID, RUN_ID, "bronze", "Sales.Customer", "bronze.customer",
                       IngestionStatus.SUCCESS, rows_read=2, rows_written=1, rows_rejected=1)
    )
    audit.record_batch(
        BatchLoadAudit(BATCH_ID, LOAD_ID, 1, "1", "2", 2, 1, 1, 1,
                       IngestionStatus.SUCCESS, now)
    )
    rejected = RejectedRecord(RUN_ID, LOAD_ID, BATCH_ID, "Sales.Customer", "2", "hash-2",
                              "NULL primary key: ID", now)
    quarantine = PostgresQuarantineService()
    quarantine.record(rejected)
    quarantine.record(rejected)

    restarted_audit = PostgresAuditService()
    restarted_quarantine = PostgresQuarantineService()
    assert restarted_audit.get_run(RUN_ID).run_id == RUN_ID
    assert restarted_audit.get_table_load(LOAD_ID).rows_rejected == 1
    assert len(restarted_audit.batches_for_load(LOAD_ID)) == 1
    assert restarted_quarantine.count_for_load(LOAD_ID) == 1


def test_persistent_batch_audit_rejects_duplicate_identity():
    audit = PostgresAuditService()
    now = datetime.now(timezone.utc)
    record = BatchLoadAudit(BATCH_ID, LOAD_ID, 1, status=IngestionStatus.SUCCESS, committed_at=now)
    audit.record_batch(record)
    with pytest.raises(ValueError, match="already exists"):
        audit.record_batch(record)