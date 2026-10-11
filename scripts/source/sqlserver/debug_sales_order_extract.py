import pandas as pd

from src.shared.connectors.sql_server_connector import SQLServerConnector

connector = SQLServerConnector()
if not connector.connect():
    raise ConnectionError("Could not connect to SQL Server")

connection = connector.connection
if connection is None:
    raise RuntimeError("SQL Server connector did not provide a connection")

try:
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Sales.SalesOrderHeader")
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    print("rows_type=", type(rows), "len_rows=", len(rows))
    print("first_row_type=", type(rows[0]), "len_first_row=", len(rows[0]))
    print("col_count=", len(columns))
    print("sample=", rows[0])
    print("df_shape=", pd.DataFrame(rows, columns=columns).shape)
finally:
    connector.disconnect()
