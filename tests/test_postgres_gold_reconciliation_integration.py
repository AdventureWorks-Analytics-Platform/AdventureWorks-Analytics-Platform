import pytest

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_gold_reconciliation_service import (
    REGISTRY_TABLE,
    PostgresGoldReconciliationError,
    PostgresGoldReconciliationService,
)
from src.shared.ingestion.retry_policy import RetryPolicy


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres_integration,
]


@pytest.fixture
def registry_cleanup():
    service = PostgresGoldReconciliationService()
    service.ensure_registry()
    with PostgreSQLConnector() as connection:
        connection.execute_query(
            f"DELETE FROM gold.{REGISTRY_TABLE} WHERE batch_id LIKE 'phase665-%'"
        )
    yield service
    with PostgreSQLConnector() as connection:
        connection.execute_query(
            f"DELETE FROM gold.{REGISTRY_TABLE} WHERE batch_id LIKE 'phase665-%'"
        )


def test_reconciliation_skips_matching_committed_batch(registry_cleanup):
    registry_cleanup.record_committed_batch(
        batch_id="phase665-committed",
        candidate_schema="gold_v001",
        target_table="fact_sales",
        ordering_key="sales_order_detail_id",
        content_hash="hash-1",
        lower_bound=0,
        upper_bound=10,
    )

    assert (
        registry_cleanup.resolve(batch_id="phase665-committed", content_hash="hash-1")
        == "SKIP"
    )


def test_reconciliation_retries_missing_batch_and_rejects_hash_conflict(
    registry_cleanup,
):
    assert (
        registry_cleanup.resolve(batch_id="phase665-missing", content_hash="hash-1")
        == "RETRY"
    )
    registry_cleanup.record_committed_batch(
        batch_id="phase665-conflict",
        candidate_schema="gold_v001",
        target_table="fact_sales",
        ordering_key="sales_order_detail_id",
        content_hash="hash-1",
    )

    with pytest.raises(PostgresGoldReconciliationError, match="different content hash"):
        registry_cleanup.resolve(batch_id="phase665-conflict", content_hash="hash-2")


def test_reconciliation_skips_from_candidate_unique_key_evidence(registry_cleanup):
    with PostgreSQLConnector() as connection:
        connection.execute_query('DROP SCHEMA IF EXISTS "gold_v998" CASCADE')
        connection.execute_query('CREATE SCHEMA "gold_v998"')
        connection.execute_query(
            'CREATE TABLE "gold_v998"."fact_sales" '
            "(sales_order_detail_id INTEGER PRIMARY KEY)"
        )
        connection.execute_query('INSERT INTO "gold_v998"."fact_sales" VALUES (9001)')

    try:
        assert (
            registry_cleanup.resolve(
                batch_id="phase665-unique",
                content_hash="hash-unknown",
                candidate_schema="gold_v998",
                unique_key_values=[9001],
            )
            == "SKIP"
        )
    finally:
        with PostgreSQLConnector() as connection:
            connection.execute_query('DROP SCHEMA IF EXISTS "gold_v998" CASCADE')


def test_reconciliation_rejects_partial_unique_key_evidence(registry_cleanup):
    schema = "gold_phase665_partial"
    with PostgreSQLConnector() as connection:
        connection.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.execute_query(f'CREATE SCHEMA "{schema}"')
        connection.execute_query(
            f'CREATE TABLE "{schema}".fact_sales '
            "(sales_order_detail_id INTEGER PRIMARY KEY)"
        )

    calls = []

    def commit_only_part_of_batch():
        calls.append("write")
        with PostgreSQLConnector() as connection:
            connection.execute_query(f'INSERT INTO "{schema}".fact_sales VALUES (9101)')
        raise TimeoutError("only part of batch committed")

    try:
        with pytest.raises(
            PostgresGoldReconciliationError, match="partial batch evidence"
        ):
            registry_cleanup.execute_batch_with_retry(
                commit_only_part_of_batch,
                batch_id="phase665-partial",
                content_hash="hash-partial",
                candidate_schema=schema,
                target_table="fact_sales",
                ordering_key="sales_order_detail_id",
                unique_key_values=[9101, 9102],
                gold_load_id="gold-load-partial",
                policy=RetryPolicy(
                    max_attempts=2,
                    initial_delay_seconds=0.01,
                    max_delay_seconds=0.01,
                ),
                sleeper=lambda _: None,
            )
        assert calls == ["write"]
    finally:
        with PostgreSQLConnector() as connection:
            connection.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_batch_commit_retry_preserves_identity_and_records_after_commit(
    registry_cleanup,
):
    calls = []

    def operation():
        calls.append("write")
        if len(calls) == 1:
            raise TimeoutError("unknown commit")
        return {"rows": 2}

    result = registry_cleanup.execute_batch_with_retry(
        operation,
        batch_id="phase665-retry",
        content_hash="hash-retry",
        candidate_schema="gold_v001",
        lower_bound=10,
        upper_bound=20,
        gold_load_id="gold-load-1",
        policy=RetryPolicy(
            max_attempts=2, initial_delay_seconds=0.01, max_delay_seconds=0.01
        ),
        sleeper=lambda _: None,
    )

    assert result["status"] == "SUCCESS"
    assert result["attempt_count"] == 2
    assert result["batch_id"] == "phase665-retry"
    assert result["gold_load_id"] == "gold-load-1"
    assert result["lower_bound"] == 10
    assert result["upper_bound"] == 20
    assert result["content_hash"] == "hash-retry"
    assert calls == ["write", "write"]
    assert (
        registry_cleanup.resolve(batch_id="phase665-retry", content_hash="hash-retry")
        == "SKIP"
    )


def test_unknown_commit_reconciles_from_real_candidate_unique_key(registry_cleanup):
    schema = "gold_phase665_unknown_commit"
    batch_id = "phase665-unknown-commit"
    key = 9002
    with PostgreSQLConnector() as connection:
        connection.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.execute_query(f'CREATE SCHEMA "{schema}"')
        connection.execute_query(
            f'CREATE TABLE "{schema}".fact_sales '
            "(sales_order_detail_id INTEGER PRIMARY KEY)"
        )

    calls = []

    def commit_then_lose_response():
        calls.append("write")
        with PostgreSQLConnector() as connection:
            connection.execute_query(
                f'INSERT INTO "{schema}".fact_sales VALUES (%s)', (key,)
            )
        raise TimeoutError("server committed; client lost the response")

    try:
        result = registry_cleanup.execute_batch_with_retry(
            commit_then_lose_response,
            batch_id=batch_id,
            content_hash="hash-unknown-commit",
            candidate_schema=schema,
            target_table="fact_sales",
            ordering_key="sales_order_detail_id",
            unique_key_values=[key],
            lower_bound=key - 1,
            upper_bound=key,
            gold_load_id="gold-load-unknown-commit",
            policy=RetryPolicy(
                max_attempts=2,
                initial_delay_seconds=0.01,
                max_delay_seconds=0.01,
            ),
            sleeper=lambda _: None,
        )

        with PostgreSQLConnector() as connection:
            persisted_rows = connection.fetch_results(
                f'SELECT sales_order_detail_id FROM "{schema}".fact_sales'
            )
        assert result["status"] == "SUCCESS"
        assert result["attempt_count"] == 1
        assert result["result"] == {
            "reconciled": True,
            "batch_id": batch_id,
        }
        assert calls == ["write"]
        assert persisted_rows == [(key,)]
        assert (
            registry_cleanup.resolve(
                batch_id=batch_id,
                content_hash="hash-unknown-commit",
            )
            == "SKIP"
        )
    finally:
        with PostgreSQLConnector() as connection:
            connection.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
