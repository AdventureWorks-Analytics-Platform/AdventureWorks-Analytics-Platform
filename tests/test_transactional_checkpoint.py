import pandas as pd
import pytest

from src.features.Sales_Performance.domain.bronze.bronze_loader import BronzeLoader
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.checkpoint_manager import PostgresCheckpointManager


TABLE_NAME = "w45_checkpoint_test"


@pytest.fixture(autouse=True)
def checkpoint_test_table():
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS bronze.{TABLE_NAME}")
        connection.execute_query(
            f"CREATE TABLE bronze.{TABLE_NAME} (id INTEGER NOT NULL, value TEXT)"
        )
        connection.execute_query(
            "CREATE TABLE IF NOT EXISTS bronze.ingestion_checkpoint ("
            "batch_id VARCHAR(128) PRIMARY KEY, upper_bound TEXT NOT NULL, "
            "committed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
    yield
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS bronze.{TABLE_NAME}")
        connection.execute_query(
            "DELETE FROM bronze.ingestion_checkpoint WHERE batch_id LIKE 'w45-%'"
        )


def _count_rows():
    with PostgreSQLConnector() as connection:
        return connection.fetch_results(
            f"SELECT COUNT(*) FROM bronze.{TABLE_NAME}"
        )[0][0]


def test_batch_data_and_checkpoint_commit_together():
    loader = BronzeLoader()
    loader.load_batch_transactionally(
        pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}),
        "bronze",
        TABLE_NAME,
        "w45-commit",
        2,
        PostgresCheckpointManager(),
    )

    with PostgreSQLConnector() as connection:
        checkpoint = connection.fetch_results(
            "SELECT upper_bound FROM bronze.ingestion_checkpoint "
            "WHERE batch_id = 'w45-commit'"
        )
    assert _count_rows() == 2
    assert checkpoint == [("2",)]


def test_checkpoint_failure_rolls_back_batch_data():
    class FailingCheckpointManager(PostgresCheckpointManager):
        def advance_in_transaction(self, connection, batch_id, upper_bound):
            raise RuntimeError("checkpoint write failed")

    with pytest.raises(RuntimeError, match="checkpoint write failed"):
        BronzeLoader().load_batch_transactionally(
            pd.DataFrame({"id": [1], "value": ["a"]}),
            "bronze",
            TABLE_NAME,
            "w45-rollback",
            1,
            FailingCheckpointManager(),
        )

    assert _count_rows() == 0
