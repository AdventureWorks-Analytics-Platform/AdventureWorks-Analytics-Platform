import pytest

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_gold_constraint_service import (
    PostgresGoldCandidateService,
)
from src.shared.ingestion.postgres_gold_publish_service import (
    POINTER_TABLE,
    PostgresGoldPublicationError,
    PostgresGoldPublishService,
)
from src.features.Sales_Performance.jobs.sales_gold_job import (
    GOLD_TABLE_SPECS,
    GoldExecutionIdentity,
)
from tests.test_gold_validation import _frames


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres_integration,
]


@pytest.fixture
def pointer_cleanup():
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS gold.{POINTER_TABLE}")
        connection.execute_query("DROP TABLE IF EXISTS gold.gold_publication_audit")
    yield
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS gold.{POINTER_TABLE}")
        connection.execute_query("DROP TABLE IF EXISTS gold.gold_publication_audit")


def _candidate(run_id):
    return PostgresGoldCandidateService().prepare_candidate(
        _frames(),
        GoldExecutionIdentity.create(run_id, f"snapshot-{run_id}"),
        GOLD_TABLE_SPECS,
    )


def _drop_candidate(candidate):
    with PostgreSQLConnector() as connection:
        connection.execute_query(
            f'DROP SCHEMA IF EXISTS "{candidate["candidate_schema"]}" CASCADE'
        )


def test_postgres_publish_updates_pointer_and_preserves_previous_version(
    pointer_cleanup,
):
    service = PostgresGoldPublishService()
    first = _candidate("run-publish-1")
    first_result = service.publish(
        first, gold_run_id="run-publish-1", source_snapshot_id="snapshot-run-publish-1"
    )
    second = _candidate("run-publish-2")
    second_result = service.publish(
        second, gold_run_id="run-publish-2", source_snapshot_id="snapshot-run-publish-2"
    )

    assert first_result["previous_version"] is None
    assert second_result["previous_version"] == first_result["gold_version"]
    with PostgreSQLConnector() as connection:
        pointer = connection.fetch_results(
            f"SELECT gold_version, candidate_schema, previous_version "
            f"FROM gold.{POINTER_TABLE} WHERE pointer_id = 1"
        )
    assert pointer == [
        (
            second_result["gold_version"],
            second_result["candidate_schema"],
            first_result["gold_version"],
        )
    ]
    _drop_candidate(first)
    _drop_candidate(second)


def test_failed_publish_keeps_previous_pointer(pointer_cleanup):
    service = PostgresGoldPublishService()
    first = _candidate("run-publish-old")
    old = service.publish(
        first,
        gold_run_id="run-publish-old",
        source_snapshot_id="snapshot-run-publish-old",
    )
    failed = _candidate("run-publish-failed")
    failed_schema = failed["candidate_schema"]
    failed["candidate_schema"] = "gold_v999_missing"

    with pytest.raises(PostgresGoldPublicationError, match="incomplete"):
        service.publish(
            failed,
            gold_run_id="run-publish-failed",
            source_snapshot_id="snapshot-run-publish-failed",
        )

    with PostgreSQLConnector() as connection:
        pointer = connection.fetch_results(
            f"SELECT gold_version, candidate_schema FROM gold.{POINTER_TABLE} WHERE pointer_id = 1"
        )
    assert pointer == [(old["gold_version"], old["candidate_schema"])]
    _drop_candidate(first)
    _drop_candidate({"candidate_schema": failed_schema})


def test_publish_writes_complete_publication_audit(pointer_cleanup):
    service = PostgresGoldPublishService()
    candidate = _candidate("run-publish-audit")
    result = service.publish(
        candidate,
        gold_run_id="run-publish-audit",
        source_snapshot_id="snapshot-run-publish-audit",
        validation_report={"validation_passed": True, "issues": []},
        constraint_report={
            "constraints_verified": True,
            "foreign_keys": {"fact_sales": 5},
        },
        kpi_report={
            "kpi_passed": True,
            "comparisons": {"total_revenue": {"within_tolerance": True}},
        },
        counts={
            "rows_read": 7,
            "rows_written": 7,
            "rows_rejected": 0,
            "dimension_rows": 5,
            "fact_rows": 2,
        },
    )

    with PostgreSQLConnector() as connection:
        audit = connection.fetch_results(
            """
            SELECT gold_run_id, gold_version, previous_version, rows_read,
                   rows_written, dimension_rows, fact_rows, validation_passed,
                   constraints_verified, kpi_passed, published
            FROM gold.gold_publication_audit
            WHERE audit_id = %s
            """,
            (result["audit_id"],),
        )
    assert audit == [
        (
            "run-publish-audit",
            result["gold_version"],
            None,
            7,
            7,
            5,
            2,
            True,
            True,
            True,
            True,
        )
    ]
    _drop_candidate(candidate)


def test_publish_rolls_back_pointer_when_audit_insert_fails(pointer_cleanup):
    service = PostgresGoldPublishService()
    current_candidate = _candidate("run-publish-atomic-current")
    current = service.publish(
        current_candidate,
        gold_run_id="run-publish-atomic-current",
        source_snapshot_id="snapshot-run-publish-atomic-current",
    )
    rejected_candidate = _candidate("run-publish-atomic-failed")
    with PostgreSQLConnector() as connection:
        connection.execute_query(
            """
            ALTER TABLE gold.gold_publication_audit
            ADD CONSTRAINT reject_atomic_publish
            CHECK (gold_run_id <> 'run-publish-atomic-failed')
            """
        )

    try:
        with pytest.raises(
            PostgresGoldPublicationError, match="publish transaction failed"
        ):
            service.publish(
                rejected_candidate,
                gold_run_id="run-publish-atomic-failed",
                source_snapshot_id="snapshot-run-publish-atomic-failed",
            )

        with PostgreSQLConnector() as connection:
            pointer = connection.fetch_results(
                f"SELECT gold_version, candidate_schema FROM gold.{POINTER_TABLE} "
                "WHERE pointer_id = 1"
            )
            failed_audits = connection.fetch_results(
                "SELECT audit_id FROM gold.gold_publication_audit "
                "WHERE gold_run_id = %s",
                ("run-publish-atomic-failed",),
            )
        assert pointer == [(current["gold_version"], current["candidate_schema"])]
        assert failed_audits == []
    finally:
        with PostgreSQLConnector() as connection:
            connection.execute_query(
                "ALTER TABLE gold.gold_publication_audit "
                "DROP CONSTRAINT IF EXISTS reject_atomic_publish"
            )
        _drop_candidate(current_candidate)
        _drop_candidate(rejected_candidate)


def test_publish_retry_after_unknown_commit_is_idempotent(pointer_cleanup):
    commit_state = {"lose_response": True}

    class ConnectionProxy:
        def __init__(self, connection):
            self.connection = connection

        def cursor(self):
            return self.connection.cursor()

        def commit(self):
            self.connection.commit()
            if commit_state["lose_response"]:
                commit_state["lose_response"] = False
                raise ConnectionError("connection lost after server commit")

        def rollback(self):
            self.connection.rollback()

    class CommitResponseLossConnector:
        def __init__(self, settings):
            self.inner = PostgreSQLConnector(settings=settings)
            self.connection = None

        def __enter__(self):
            self.inner.__enter__()
            self.connection = ConnectionProxy(self.inner.connection)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.inner.__exit__(exc_type, exc_val, exc_tb)

    candidate = _candidate("run-publish-unknown-commit")
    service = PostgresGoldPublishService(connector_factory=CommitResponseLossConnector)
    publish_args = {
        "gold_run_id": "run-publish-unknown-commit",
        "source_snapshot_id": "snapshot-run-publish-unknown-commit",
    }
    try:
        with pytest.raises(
            PostgresGoldPublicationError, match="publish transaction failed"
        ):
            service.publish(candidate, **publish_args)

        retried = service.publish(candidate, **publish_args)

        with PostgreSQLConnector() as connection:
            pointer = connection.fetch_results(
                f"SELECT gold_version, candidate_schema FROM gold.{POINTER_TABLE} "
                "WHERE pointer_id = 1"
            )
            audit_rows = connection.fetch_results(
                "SELECT audit_id FROM gold.gold_publication_audit "
                "WHERE gold_run_id = %s",
                (publish_args["gold_run_id"],),
            )
        assert retried["published"] is True
        assert retried["audit_id"] == audit_rows[0][0]
        assert len(audit_rows) == 1
        assert pointer == [(candidate["gold_version"], candidate["candidate_schema"])]
    finally:
        _drop_candidate(candidate)
