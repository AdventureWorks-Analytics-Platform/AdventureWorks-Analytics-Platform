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

print("=== DASHBOARD METRICS VALIDATION ===\n")

# Total Revenue
cur.execute('''
SELECT 
    COUNT(*) as total_line_items,
    COUNT(DISTINCT sales_order_id) as total_orders,
    ROUND(SUM(net_sales)::NUMERIC, 2) as total_revenue,
    ROUND(AVG(net_sales)::NUMERIC, 2) as avg_order_value
FROM gold.fact_sales
WHERE salesperson_id IS NOT NULL
''')
result = cur.fetchone()
print(f"With SalesPersonID (excluding NULL):")
print(f"  Line items: {result[0]:,}")
print(f"  Unique Orders: {result[1]:,}")
print(f"  Total Revenue: ${result[2]:,.2f}")
print(f"  Avg Order Value: ${result[3]:,.2f}")

# Including NULL salespeople
cur.execute('''
SELECT 
    COUNT(*) as total_line_items,
    COUNT(DISTINCT sales_order_id) as total_orders,
    ROUND(SUM(net_sales)::NUMERIC, 2) as total_revenue,
    ROUND(AVG(net_sales)::NUMERIC, 2) as avg_order_value
FROM gold.fact_sales
''')
result = cur.fetchone()
print(f"\nIncluding NULL SalesPersonID (Direct/Online):")
print(f"  Line items: {result[0]:,}")
print(f"  Unique Orders: {result[1]:,}")
print(f"  Total Revenue: ${result[2]:,.2f}")
print(f"  Avg Order Value: ${result[3]:,.2f}")

# Revenue by Salesperson (Top 10 non-NULL)
print("\n=== TOP 10 SALESPERSONS BY REVENUE ===")
cur.execute('''
SELECT 
    ds.salesperson_name,
    COUNT(DISTINCT fs.sales_order_id) as orders,
    ROUND(SUM(fs.net_sales)::NUMERIC, 2) as revenue,
    ROUND(AVG(fs.net_sales)::NUMERIC, 2) as avg_order_value
FROM gold.fact_sales fs
JOIN gold.dim_salesperson ds ON fs.salesperson_id = ds.salesperson_id::BIGINT
WHERE fs.salesperson_id IS NOT NULL
GROUP BY ds.salesperson_name
ORDER BY revenue DESC
LIMIT 10
''')
results = cur.fetchall()
for i, row in enumerate(results, 1):
    print(f"{i:2}. {row[0]:<20} | Orders: {row[1]:>5} | Revenue: ${row[2]:>10,.2f} | AOV: ${row[3]:>8,.2f}")

cur.close()
conn.close()
