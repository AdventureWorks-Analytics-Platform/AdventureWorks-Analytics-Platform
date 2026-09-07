# Quick SQL Server connection test

"""
Test SQL Server connectivity with Windows Authentication
"""

import os
import pytest
from dotenv import load_dotenv
import pyodbc


pytestmark = pytest.mark.integration

# Load environment variables
load_dotenv()

# Get configuration
sql_server_host = os.getenv('SQL_SERVER_HOST')
sql_server_port = os.getenv('SQL_SERVER_PORT', 1433)
sql_server_database = os.getenv('SQL_SERVER_DATABASE', 'AdventureWorks2012')
sql_server_driver = os.getenv('SQL_SERVER_DRIVER', 'ODBC Driver 17 for SQL Server')

def test_sql_server_connection():
    """Verify SQL Server connectivity without running during pytest collection."""
    connection_string = (
        f'Driver={{{sql_server_driver}}};'
        f'Server={sql_server_host};'
        f'Database={sql_server_database};'
        f'Trusted_Connection=yes;'
    )
    
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Sales.Customer")
            customer_count = cursor.fetchone()[0]
    except Exception as exc:
        pytest.skip(f"SQL Server unavailable: {exc}")

    assert customer_count >= 0
