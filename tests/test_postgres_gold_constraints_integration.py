import pytest

from src.core.settings import get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_gold_constraint_service import (
    PostgresGoldCandidateService,
    PostgresGoldConstraintError,
    PostgresGoldConstraintService,
)
from src.shared.ingestion.postgres_gold_publish_service import (
    POINTER_TABLE,
    PostgresGoldPublishService,
)
from src.features.Sales_Performance.jobs.sales_gold_job import (
    GOLD_TABLE_SPECS,
    GoldConstraintManager,
    GoldExecutionIdentity,
)
from tests.test_gold_validation import _frames


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres_integration,
]


SCHEMA = "gold_v997"


@pytest.fixture
def candidate_schema():
    settings = get_settings()
    with PostgreSQLConnector(settings=settings) as connection:
        connection.execute_query(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    yield
    with PostgreSQLConnector(settings=settings) as connection:
        connection.execute_query(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def test_postgres_gold_constraints_execute_and_inspect_catalog(candidate_schema):
    manager = GoldConstraintManager()
    identity = GoldExecutionIdentity.create("pipeline-live-1", "silver-live-1")
    candidate = manager.create_candidate_schema(
        _frames(), identity, GOLD_TABLE_SPECS, gold_version="v997"
    )

    report = PostgresGoldConstraintService().create_and_verify(
        candidate, GOLD_TABLE_SPECS
    )

    assert report["constraints_verified"] is True
    assert set(report["tables"]) == {spec.target_table for spec in GOLD_TABLE_SPECS}
    assert any(row[2] == "PRIMARY KEY" for row in report["constraint_metadata"])
    assert any(row[2] == "FOREIGN KEY" for row in report["constraint_metadata"])


def test_constraint_failure_rolls_back_candidate_and_preserves_current_pointer():
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS gold.{POINTER_TABLE}")
        baseline_schemas = {
            row[0]
            for row in connection.fetch_results(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name LIKE 'gold_v%'"
            )
        }
    pointer_service = PostgresGoldPublishService()
    identity = GoldExecutionIdentity.create("constraint-old", "snapshot-old")
    old_candidate = PostgresGoldCandidateService().prepare_candidate(
        _frames(), identity, GOLD_TABLE_SPECS
    )
    old_result = pointer_service.publish(
        old_candidate,
        gold_run_id="constraint-old",
        source_snapshot_id="snapshot-old",
    )

    invalid_frames = _frames()
    invalid_frames["dim_customer"] = invalid_frames["dim_customer"].copy()
    invalid_frames["dim_customer"] = (
        invalid_frames["dim_customer"]
        .loc[invalid_frames["dim_customer"].index.repeat(2)]
        .reset_index(drop=True)
    )

    with pytest.raises(PostgresGoldConstraintError):
        PostgresGoldCandidateService().prepare_candidate(
            invalid_frames,
            GoldExecutionIdentity.create("constraint-failed", "snapshot-failed"),
            GOLD_TABLE_SPECS,
        )

    with PostgreSQLConnector() as connection:
        pointer = connection.fetch_results(
            f"SELECT gold_version, candidate_schema FROM gold.{POINTER_TABLE} "
            "WHERE pointer_id = 1"
        )
        failed_schemas = {
            row[0]
            for row in connection.fetch_results(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name LIKE 'gold_v%'"
            )
        }
    assert pointer == [(old_result["gold_version"], old_result["candidate_schema"])]
    assert failed_schemas == baseline_schemas | {old_result["candidate_schema"]}
    assert failed_schemas - baseline_schemas == {old_result["candidate_schema"]}

    with PostgreSQLConnector() as connection:
        connection.execute_query(
            f'DROP SCHEMA IF EXISTS "{old_result["candidate_schema"]}" CASCADE'
        )
        connection.execute_query(f"DROP TABLE IF EXISTS gold.{POINTER_TABLE}")
