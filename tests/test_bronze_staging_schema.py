import pandas as pd

from src.features.Sales_Performance.domain.bronze.bronze_loader import BronzeLoader
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_ingestion_schema import ensure_ingestion_schema


TABLE_NAME = "w4_staging_schema_test"


def test_bronze_loader_uses_dedicated_bronze_staging_schema():
    ensure_ingestion_schema()
    BronzeLoader().load(
        pd.DataFrame({"id": [1], "value": ["staged"]}),
        BronzeLoader.staging_schema,
        TABLE_NAME,
    )

    try:
        with PostgreSQLConnector() as connection:
            rows = connection.fetch_results(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_name = %s AND table_schema IN ('bronze', 'bronze_staging')
                ORDER BY table_schema
                """,
                (TABLE_NAME,),
            )
        assert rows == [("bronze_staging", TABLE_NAME)]
    finally:
        with PostgreSQLConnector() as connection:
            connection.execute_query(
                f"DROP TABLE IF EXISTS bronze_staging.{TABLE_NAME}"
            )
