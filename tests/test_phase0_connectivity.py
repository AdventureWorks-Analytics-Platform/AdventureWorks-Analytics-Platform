# Test module for Phase 0 connectivity validation

"""
Phase 0 connectivity and foundation tests.
"""

import pytest
from src.shared.connectors import SQLServerConnector, PostgreSQLConnector
from scripts.ingestion.bronze.bronze_ingest import bronze_ingest_table


pytestmark = pytest.mark.integration


class TestSQLServerConnectivity:
    """Test SQL Server connectivity."""
    
    def test_sql_server_connection(self):
        """Test SQL Server connection."""
        with SQLServerConnector() as conn:
            assert conn.connect()
    
    def test_sql_server_query_execution(self):
        """Test SQL Server query execution."""
        with SQLServerConnector() as conn:
            result = conn.execute_query("SELECT 1 AS test_value")
            assert result is not None


class TestPostgreSQLConnectivity:
    """Test PostgreSQL connectivity."""
    
    def test_postgresql_connection(self):
        """Test PostgreSQL connection."""
        with PostgreSQLConnector() as conn:
            assert conn.connect()
    
    def test_postgresql_schemas_exist(self):
        """Test that required schemas exist."""
        with PostgreSQLConnector() as conn:
            result = conn.fetch_results(
                "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN ('bronze', 'silver', 'gold')"
            )
            assert result[0][0] == 3, "Not all required schemas exist"


class TestBronzeIngestion:
    """Test raw bronze ingestion for one source table."""

    def test_person_person_raw_load(self):
        """Load a source table into bronze and verify lineage metadata."""
        source_table = 'Person.Person'

        with PostgreSQLConnector() as pg_conn:
            pg_conn.execute_query("DROP TABLE IF EXISTS bronze.person_person")
            pg_conn.execute_query("DELETE FROM bronze.load_audit WHERE table_name = 'person_person'")

        with SQLServerConnector() as sql_conn:
            source_count = sql_conn.execute_query(
                f"SELECT COUNT(*) FROM {source_table}"
            )[0][0]
            assert source_count > 0, "No rows returned from Person.Person"

        inserted_rows = bronze_ingest_table(source_table)
        assert inserted_rows == source_count, "Inserted row count did not match source table row count"

        with PostgreSQLConnector() as pg_conn:
            target_count = pg_conn.fetch_results(
                "SELECT COUNT(*) FROM bronze.person_person"
            )[0][0]
            columns = pg_conn.fetch_results(
                "SELECT lower(column_name) FROM information_schema.columns WHERE table_schema = 'bronze' AND table_name = 'person_person' ORDER BY ordinal_position"
            )
            column_names = {row[0] for row in columns}
            assert target_count == source_count, "bronze.person_person row count mismatch"
            assert '_load_date' in column_names, "Missing lineage column _load_date"
            assert '_source_system' in column_names, "Missing lineage column _source_system"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
