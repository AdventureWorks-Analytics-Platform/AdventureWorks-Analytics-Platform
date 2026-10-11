#!/usr/bin/env python
import psycopg2

from src.core.settings import get_settings

settings = get_settings()
conn = psycopg2.connect(
    host=settings.postgres_host,
    port=settings.postgres_port,
    database=settings.postgres_database,
    user=settings.postgres_username,
    password=settings.postgres_password.get_secret_value(),
)

cur = conn.cursor()

# Check Bronze layer data with correct CamelCase column names
print('=== BRONZE LAYER (sales_order_header) - SalesOrderID 43697 ===')
cur.execute('''
SELECT "SalesOrderID", "SalesPersonID", "CustomerID", "TerritoryID"
FROM bronze.sales_order_header
WHERE "SalesOrderID" = 43697
''')
bronze_result = cur.fetchall()
for row in bronze_result:
    print('Bronze:', row)

# Check Silver (which uses lowercase)
print('\n=== SILVER LAYER (sales_order_header_clean) - SalesOrderID 43697 ===')
cur.execute('''
SELECT sales_order_id, salesperson_id, customer_id, territory_id
FROM silver.sales_order_header_clean
WHERE sales_order_id = 43697
''')
silver_result = cur.fetchall()
for row in silver_result:
    print('Silver:', row)

# Check Gold layer
print('\n=== GOLD LAYER (fact_sales) - sales_order_id 43697 ===')
cur.execute('''
SELECT salesperson_id, sales_order_id, customer_id, territory_id, net_sales
FROM gold.fact_sales
WHERE sales_order_id = 43697
''')
gold_result = cur.fetchall()
for row in gold_result:
    print('Gold:', row)

# Check if SalesPersonID is NULL in source SQL Server for comparison
print('\n=== SOURCE CHECK: How many SalesOrderHeader have NULL SalesPersonID in SQL Server? ===')
cur.execute('''
SELECT COUNT(*) FROM bronze.sales_order_header WHERE "SalesPersonID" IS NULL
''')
null_count = cur.fetchone()
print(f'NULL SalesPersonID in Bronze: {null_count[0]} records')

cur.close()
conn.close()
