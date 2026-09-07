import pandas as pd
import pytest
from psycopg2 import sql

from src.features.Sales_Performance.domain.bronze.bronze_loader import BronzeLoader
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_publish_service import PostgresPublishService
from src.shared.ingestion.postgres_reconciliation_service import (
    PostgresReconciliationService,
)
from src.shared.ingestion.postgres_ingestion_schema import ensure_ingestion_schema


pytestmark = pytest.mark.integration


TARGET = "w4_atomic_publish"
STAGING = "w4_atomic_publish__run_1__load_1"
BATCH = "w4-unknown-commit"


@pytest.fixture(autouse=True)
def database_objects():
    ensure_ingestion_schema()
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS bronze.{TARGET}")
        connection.execute_query(f"DROP TABLE IF EXISTS bronze_staging.{STAGING}")
        connection.execute_query(
            f"CREATE TABLE bronze.{TARGET} (id INTEGER NOT NULL, value TEXT)"
        )
        connection.execute_query(
            f"INSERT INTO bronze.{TARGET} VALUES (1, 'old')"
        )
        connection.execute_query(
            "DELETE FROM bronze.ingestion_batch_registry WHERE batch_id = %s",
            (BATCH,),
        )
    yield
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS bronze.{TARGET}")
        connection.execute_query(f"DROP TABLE IF EXISTS bronze_staging.{STAGING}")
        connection.execute_query(
            "DELETE FROM bronze.ingestion_batch_registry WHERE batch_id = %s",
            (BATCH,),
        )
        cursor = connection.connection.cursor()
        cursor.execute(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'bronze' AND tablename LIKE %s",
            (f"{TARGET}__previous__%",),
        )
        for (table_name,) in cursor.fetchall():
            cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier("bronze"), sql.Identifier(table_name)
                )
            )
        connection.connection.commit()
        cursor.close()


def test_postgres_publish_swaps_staging_into_bronze_atomically():
    BronzeLoader().load(
        pd.DataFrame({"id": [2], "value": ["new"]}),
        "bronze_staging",
        STAGING,
    )
    PostgresPublishService().publish(
        TARGET, STAGING, {"validation_passed": True}
    )

    with PostgreSQLConnector() as connection:
        published = connection.fetch_results(
            f"SELECT id, value FROM bronze.{TARGET}"
        )
        staging = connection.fetch_results(
            "SELECT to_regclass(%s)", (f"bronze_staging.{STAGING}",)
        )
    assert published == [(2, "new")]
    assert staging == [(None,)]


def test_postgres_publish_rejects_failed_validation_without_changing_bronze():
    with pytest.raises(ValueError, match="validation must pass"):
        PostgresPublishService().publish(TARGET, STAGING, {"validation_passed": False})

    with PostgreSQLConnector() as connection:
        assert connection.fetch_results(f"SELECT id, value FROM bronze.{TARGET}") == [
            (1, "old")
        ]


def test_postgres_reconciliation_reads_registry_across_service_instances():
    with PostgreSQLConnector() as connection:
        connection.execute_query(
            "INSERT INTO bronze.ingestion_batch_registry "
            "(batch_id, content_hash, upper_bound) VALUES (%s, %s, %s)",
            (BATCH, "hash-1", "10"),
        )

    restarted = PostgresReconciliationService()
    assert restarted.resolve("ignored", BATCH, "hash-1") == "SKIP"
    with pytest.raises(ValueError, match="different content hash"):
        restarted.resolve("ignored", BATCH, "hash-2")
    assert restarted.resolve("ignored", "missing-batch", "hash-1") == "RETRY"