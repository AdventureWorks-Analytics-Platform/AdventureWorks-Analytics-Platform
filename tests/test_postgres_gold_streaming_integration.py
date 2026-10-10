import pytest

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_gold_constraint_service import (
    PostgresGoldCandidateService,
)
from src.features.Sales_Performance.jobs.sales_gold_job import (
    GOLD_TABLE_SPECS,
    GoldExecutionIdentity,
)
from scripts.warehouse.postgres.gold.sales_gold_load import _PostgresGoldReader
from src.core.settings import get_settings
from tests.test_gold_validation import _frames


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres_integration,
]


def test_streaming_candidate_writes_batches_validates_and_retries_idempotently():
    service = PostgresGoldCandidateService()
    identity = GoldExecutionIdentity.create(
        "pipeline-stream-integration", "silver-stream-integration"
    )
    dimensions = {
        name: frame for name, frame in _frames().items() if name != "fact_sales"
    }
    candidate = service.prepare_streaming_candidate(
        dimensions, identity, GOLD_TABLE_SPECS
    )
    schema = candidate["candidate_schema"]
    silver_schema = "silver_stream_integration"
    with PostgreSQLConnector() as connection:
        connection.execute_query("CREATE SCHEMA IF NOT EXISTS silver")
        connection.execute_query(
            "CREATE TABLE IF NOT EXISTS silver.silver_current_pointer ("
            "pointer_id SMALLINT PRIMARY KEY, candidate_schema VARCHAR(63) NOT NULL, "
            "source_snapshot_id VARCHAR(128) NOT NULL, published_at TIMESTAMPTZ NOT NULL)"
        )
        previous_pointer = connection.fetch_results(
            "SELECT candidate_schema, source_snapshot_id, published_at "
            "FROM silver.silver_current_pointer WHERE pointer_id = 1"
        )
        connection.execute_query(f'DROP SCHEMA IF EXISTS "{silver_schema}" CASCADE')
        connection.execute_query(f'CREATE SCHEMA "{silver_schema}"')
        connection.execute_query(
            f'CREATE TABLE "{silver_schema}".sales_order_detail_clean ('
            "sales_order_id INTEGER, sales_order_detail_id INTEGER, product_id INTEGER, "
            "order_qty INTEGER, unit_price NUMERIC(19,4), unit_price_discount NUMERIC(19,4), "
            "line_total NUMERIC(19,4), source_snapshot_id VARCHAR(128))"
        )
        connection.execute_query(
            f'CREATE TABLE "{silver_schema}".sales_order_header_clean ('
            "sales_order_id INTEGER, order_date DATE, customer_id INTEGER, "
            "territory_id INTEGER, salesperson_id INTEGER, source_snapshot_id VARCHAR(128))"
        )
        connection.execute_query(
            f'INSERT INTO "{silver_schema}".sales_order_detail_clean VALUES '
            "(1, 10, 1, 2, 10, 0.1, 18, %s), "
            "(1, 900001, 1, 1, 10, 0, 10, %s)",
            (identity.source_snapshot_id, identity.source_snapshot_id),
        )
        connection.execute_query(
            f'INSERT INTO "{silver_schema}".sales_order_header_clean VALUES '
            "(1, '2020-01-01', 1, 1, NULL, %s)",
            (identity.source_snapshot_id,),
        )
        connection.execute_query(
            "INSERT INTO silver.silver_current_pointer "
            "(pointer_id, candidate_schema, source_snapshot_id, published_at) "
            "VALUES (1, %s, %s, CURRENT_TIMESTAMP) "
            "ON CONFLICT (pointer_id) DO UPDATE SET "
            "candidate_schema = EXCLUDED.candidate_schema, "
            "source_snapshot_id = EXCLUDED.source_snapshot_id, "
            "published_at = EXCLUDED.published_at",
            (silver_schema, identity.source_snapshot_id),
        )
    try:
        reader = _PostgresGoldReader(get_settings(), batch_size=1)
        batch_count = 0
        repeated_batch = None
        batch_keys = []
        batch_bounds = []
        for batch in reader.fact_batches(
            source_snapshot_id=identity.source_snapshot_id, batch_size=1
        ):
            assert len(batch.dataframe) == 1
            service.write_fact_batch(candidate, batch, identity)
            repeated_batch = batch
            batch_keys.extend(batch.dataframe["sales_order_detail_id"].tolist())
            batch_bounds.append((batch.lower_bound, batch.upper_bound))
            batch_count += 1
        assert repeated_batch is not None
        repeated = service.write_fact_batch(candidate, repeated_batch, identity)
        report = service.constraint_service.validate_candidate(
            candidate, GOLD_TABLE_SPECS, require_silver_baseline=True
        )
        mismatch = service.constraint_service.validate_candidate(
            candidate,
            GOLD_TABLE_SPECS,
            silver_kpi_baseline={"total_revenue": 100.0},
        )
        constraints = service.finalize_streaming_candidate(candidate, GOLD_TABLE_SPECS)

        with PostgreSQLConnector() as connection:
            persisted = connection.fetch_results(
                f'SELECT COUNT(*), SUM(net_sales) FROM "{schema}".fact_sales'
            )
        assert candidate["frames"]["fact_sales"].empty
        assert batch_count == 2
        assert batch_keys == [10, 900001]
        assert batch_bounds == [(None, 10), (10, 900001)]
        assert candidate["tables"]["fact_sales"]["rows"] == 2
        assert repeated["result"]["reconciled"] is True
        assert persisted == [(2, 28.0)]
        assert report["validation_passed"] is True
        assert report["kpi_report"]["actual"]["total_revenue"] == 28.0
        assert report["kpi_report"]["actual"]["total_line_items"] == 2.0
        assert (
            report["kpi_report"]["baseline_source_snapshot_id"]
            == identity.source_snapshot_id
        )
        assert report["kpi_report"]["comparisons"]["total_revenue"]["within_tolerance"]
        assert mismatch["validation_passed"] is False
        assert mismatch["kpi_passed"] is False
        assert constraints["constraints_verified"] is True
    finally:
        with PostgreSQLConnector() as connection:
            connection.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            connection.execute_query(
                "DELETE FROM gold.gold_batch_registry WHERE candidate_schema = %s",
                (schema,),
            )
            connection.execute_query(f'DROP SCHEMA IF EXISTS "{silver_schema}" CASCADE')
            if previous_pointer:
                connection.execute_query(
                    "UPDATE silver.silver_current_pointer SET candidate_schema = %s, "
                    "source_snapshot_id = %s, published_at = %s WHERE pointer_id = 1",
                    previous_pointer[0],
                )
            else:
                connection.execute_query(
                    "DELETE FROM silver.silver_current_pointer WHERE pointer_id = 1"
                )
