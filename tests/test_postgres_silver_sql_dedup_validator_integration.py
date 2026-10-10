"""Live-PostgreSQL evidence for the opt-in SQL-side Silver dedup/validate path.

Covers Phase 4C backlog task 6.5.10: proves `PostgresSilverDedupService` and
`PostgresSilverStagingValidator` behave correctly against a real staged table,
matching the pandas `_global_deduplicate()`/`_default_staging_validator()`
equivalents they replace when `Settings.silver_sql_dedup_enabled=True`.
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.ingestion_models import TableSpec
from src.shared.ingestion.postgres_ingestion_schema import ensure_ingestion_schema
from src.shared.ingestion.postgres_publish_service import (
    PostgresSilverDedupService,
    PostgresSilverStagingValidator,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres_integration,
]


STAGING_TABLE = "w_sql_dedup_integration"
SAMPLE_SPEC = TableSpec(
    source_schema="bronze",
    source_table="sample",
    target_schema="silver",
    target_table="sample_clean",
    primary_key="sample_id",
    required_columns=("sample_id", "value"),
    ordering_key="sample_id",
)
SALES_PERSON_SPEC = TableSpec(
    source_schema="bronze",
    source_table="sales_person",
    target_schema="silver",
    target_table="sales_person_clean",
    primary_key="sample_id",
    required_columns=("sample_id", "salesperson_name"),
    ordering_key="sample_id",
)


@pytest.fixture(autouse=True)
def staging_table():
    ensure_ingestion_schema()
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS silver_staging.{STAGING_TABLE}")
    yield
    with PostgreSQLConnector() as connection:
        connection.execute_query(f"DROP TABLE IF EXISTS silver_staging.{STAGING_TABLE}")


def _seed(rows: list[dict]) -> None:
    frame = pd.DataFrame(rows)
    with PostgreSQLConnector() as pg_conn:
        engine = create_engine(
            "postgresql://", creator=lambda: pg_conn.connection, poolclass=StaticPool
        )
        try:
            frame.to_sql(
                STAGING_TABLE,
                engine,
                schema="silver_staging",
                if_exists="replace",
                index=False,
            )
            pg_conn.connection.commit()
        finally:
            engine.dispose()


def test_dedup_service_removes_cross_batch_duplicates_keeping_latest():
    _seed(
        [
            {
                "sample_id": 1,
                "value": "old",
                "_load_date": "2026-09-04",
                "_record_hash": "a",
            },
            {
                "sample_id": 1,
                "value": "new",
                "_load_date": "2026-09-05",
                "_record_hash": "b",
            },
            {
                "sample_id": 2,
                "value": "solo",
                "_load_date": "2026-09-04",
                "_record_hash": "c",
            },
        ]
    )
    service = PostgresSilverDedupService()

    assert service.count_duplicate_keys(STAGING_TABLE, "sample_id") == 1

    removed = service.deduplicate(STAGING_TABLE, "sample_id")

    assert removed == 1
    assert service.count_duplicate_keys(STAGING_TABLE, "sample_id") == 0
    with PostgreSQLConnector() as connection:
        rows = connection.fetch_results(
            f"SELECT sample_id, value FROM silver_staging.{STAGING_TABLE} ORDER BY sample_id"
        )
    assert rows == [(1, "new"), (2, "solo")]


def test_dedup_service_set_source_snapshot_id_stamps_all_rows():
    _seed([{"sample_id": 1, "value": "a"}, {"sample_id": 2, "value": "b"}])
    service = PostgresSilverDedupService()

    service.set_source_snapshot_id(STAGING_TABLE, "snapshot-123")

    with PostgreSQLConnector() as connection:
        rows = connection.fetch_results(
            f"SELECT source_snapshot_id FROM silver_staging.{STAGING_TABLE}"
        )
    assert all(value == "snapshot-123" for (value,) in rows)


def test_staging_validator_detects_null_and_duplicate_primary_keys():
    _seed(
        [
            {"sample_id": 1, "value": "a"},
            {"sample_id": None, "value": "b"},
            {"sample_id": 3, "value": "c"},
            {"sample_id": 3, "value": "d"},
        ]
    )
    validator = PostgresSilverStagingValidator()

    report = validator.validate(
        STAGING_TABLE,
        SAMPLE_SPEC,
        source_count=4,
        rejected_count=0,
        rejected_threshold=0,
    )

    assert report["validation_passed"] is False
    assert report["schema_ok"] is True
    assert report["primary_key_nulls"] == 1
    assert report["duplicate_primary_keys"] == 1
    assert report["target_count"] == 4
    assert "NULL staging primary keys: 1" in report["issues"]
    assert "Duplicate staging primary keys: 1" in report["issues"]


def test_staging_validator_passes_clean_staging_after_dedup():
    _seed(
        [
            {
                "sample_id": 1,
                "value": "old",
                "_load_date": "2026-09-04",
                "_record_hash": "a",
            },
            {
                "sample_id": 1,
                "value": "new",
                "_load_date": "2026-09-05",
                "_record_hash": "b",
            },
        ]
    )
    PostgresSilverDedupService().deduplicate(STAGING_TABLE, "sample_id")
    validator = PostgresSilverStagingValidator()

    report = validator.validate(
        STAGING_TABLE,
        SAMPLE_SPEC,
        source_count=2,
        rejected_count=0,
        rejected_threshold=0,
    )

    assert report["validation_passed"] is True
    assert report["target_count"] == 1
    assert report["issues"] == []


def test_staging_validator_flags_blank_salesperson_join():
    _seed(
        [
            {"sample_id": 1, "salesperson_name": ""},
            {"sample_id": 2, "salesperson_name": "Jane Doe"},
        ]
    )
    validator = PostgresSilverStagingValidator()

    report = validator.validate(
        STAGING_TABLE,
        SALES_PERSON_SPEC,
        source_count=2,
        rejected_count=0,
        rejected_threshold=0,
    )

    assert report["required_joins_ok"] is False
    assert any("Person join" in issue for issue in report["issues"])


def test_staging_validator_enforces_rejected_threshold():
    _seed([{"sample_id": 1, "value": "a"}])
    validator = PostgresSilverStagingValidator()

    report = validator.validate(
        STAGING_TABLE,
        SAMPLE_SPEC,
        source_count=5,
        rejected_count=3,
        rejected_threshold=1,
    )

    assert report["rejected_threshold_ok"] is False
    assert report["validation_passed"] is False
    assert any("Rejected row threshold exceeded" in issue for issue in report["issues"])
