# Quick SQL Server connection test

import pytest

from src.shared.connectors.sql_server_connector import SQLServerConnector


pytestmark = [
    pytest.mark.integration,
    pytest.mark.sqlserver_integration,
]


def test_sql_server_connection():
    """Verify the configured AdventureWorks SQL Server source is reachable."""
    with SQLServerConnector() as connection:
        rows = connection.execute_query("SELECT COUNT(*) FROM Sales.Customer")
    customer_count = rows[0][0]
    assert customer_count >= 0
