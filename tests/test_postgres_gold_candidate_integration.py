import pytest

from src.core.settings import get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_gold_constraint_service import (
    PostgresGoldCandidateService,
    PostgresGoldConstraintError,
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


PUBLISHED_SCHEMA = "gold"
PUBLISHED_TABLE = "phase6_candidate_marker"


@pytest.fixture
def published_marker():
    settings = get_settings()
    with PostgreSQLConnector(settings=settings) as connection:
        connection.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{PUBLISHED_SCHEMA}"')
        connection.execute_query(
            f'DROP TABLE IF EXISTS "{PUBLISHED_SCHEMA}"."{PUBLISHED_TABLE}"'
        )
        connection.execute_query(
            f'CREATE TABLE "{PUBLISHED_SCHEMA}"."{PUBLISHED_TABLE}" (value TEXT)'
        )
        connection.execute_query(
            f'INSERT INTO "{PUBLISHED_SCHEMA}"."{PUBLISHED_TABLE}" VALUES (\'published-old\')'
        )
    yield
    with PostgreSQLConnector(settings=settings) as connection:
        connection.execute_query(
            f'DROP TABLE IF EXISTS "{PUBLISHED_SCHEMA}"."{PUBLISHED_TABLE}"'
        )


def test_candidate_identity_creates_versioned_schema_without_touching_published_gold(
    published_marker,
):
    service = PostgresGoldCandidateService()
    identity = GoldExecutionIdentity.create(
        "pipeline-candidate-1", "silver-candidate-1"
    )

    candidate = service.prepare_candidate(_frames(), identity, GOLD_TABLE_SPECS)

    assert candidate["candidate_schema"].startswith("gold_v")
    assert candidate["published"] is False
    assert candidate["lifecycle"] == "CANDIDATE"
    assert candidate["constraint_report"]["constraints_verified"] is True
    with PostgreSQLConnector() as connection:
        marker = connection.fetch_results(
            f'SELECT value FROM "{PUBLISHED_SCHEMA}"."{PUBLISHED_TABLE}"'
        )
    assert marker == [("published-old",)]

    with PostgreSQLConnector() as connection:
        connection.execute_query(
            f'DROP SCHEMA IF EXISTS "{candidate["candidate_schema"]}" CASCADE'
        )


def test_failed_candidate_rolls_back_candidate_schema_and_preserves_published_gold(
    published_marker,
):
    service = PostgresGoldCandidateService()
    identity = GoldExecutionIdentity.create(
        "pipeline-candidate-2", "silver-candidate-2"
    )
    frames = _frames()
    frames["dim_customer"] = frames["dim_customer"].copy()
    frames["dim_customer"] = (
        frames["dim_customer"]
        .loc[frames["dim_customer"].index.repeat(2)]
        .reset_index(drop=True)
    )

    with pytest.raises(PostgresGoldConstraintError):
        service.prepare_candidate(frames, identity, GOLD_TABLE_SPECS)

    with PostgreSQLConnector() as connection:
        marker = connection.fetch_results(
            f'SELECT value FROM "{PUBLISHED_SCHEMA}"."{PUBLISHED_TABLE}"'
        )
        candidate_schemas = connection.fetch_results(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 'gold_v%'"
        )
    assert marker == [("published-old",)]
    assert all(row[0] != "gold_v999" for row in candidate_schemas)
