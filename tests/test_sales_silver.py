import pandas as pd
import pytest

from scripts.transformation.silver.sales_silver_clean import (
    clean_customer,
    clean_product,
    clean_sales_person,
    clean_sales_order_detail,
    clean_sales_order_header,
)


def test_clean_customer_enriches_individuals_from_person():
    customers = pd.DataFrame(
        {
            "CustomerID": [1], "PersonID": [101], "StoreID": [None],
            "TerritoryID": [1], "AccountNumber": ["AW000001"],
            "_source_system": ["AdventureWorks2012"], "_load_date": ["2026-09-08"],
        }
    )
    people = pd.DataFrame(
        {"BusinessEntityID": [101], "FirstName": ["Ada"], "LastName": ["Lovelace"]}
    )

    result = clean_customer(customers, people)

    assert result.iloc[0]["customer_name"] == "Ada Lovelace"
    assert result.iloc[0]["customer_type"] == "INDIVIDUAL"
    assert result.iloc[0]["customer_name_source"] == "PERSON"


def test_clean_customer_uses_account_number_for_store_customers():
    customers = pd.DataFrame(
        {
            "CustomerID": [2], "PersonID": [None], "StoreID": [202],
            "TerritoryID": [1], "AccountNumber": [" AW000202 "],
            "_source_system": ["AdventureWorks2012"], "_load_date": ["2026-09-08"],
        }
    )

    result = clean_customer(customers)

    assert result.iloc[0]["customer_name"] == "AW000202"
    assert result.iloc[0]["customer_type"] == "STORE"
    assert result.iloc[0]["customer_name_source"] == "ACCOUNT"


def test_clean_customer_fails_closed_for_unresolved_individual():
    customers = pd.DataFrame(
        {
            "CustomerID": [3], "PersonID": [303], "StoreID": [None],
            "TerritoryID": [1], "AccountNumber": ["AW000303"],
            "_source_system": ["AdventureWorks2012"], "_load_date": ["2026-09-08"],
        }
    )
    people = pd.DataFrame(
        {"BusinessEntityID": [304], "FirstName": ["Grace"], "LastName": ["Hopper"]}
    )

    with pytest.raises(RuntimeError, match="Unresolved Person references"):
        clean_customer(customers, people)


def test_clean_sales_order_header_renames_casts_and_deduplicates():
    frame = pd.DataFrame(
        {
            "SalesOrderID": [1, 1],
            "OrderDate": ["2024-01-02", "2024-01-03"],
            "DueDate": ["2024-01-05", "2024-01-06"],
            "ShipDate": ["2024-01-04", "2024-01-05"],
            "CustomerID": [10, 10],
            "SalesPersonID": [20, 20],
            "TerritoryID": [1, 1],
            "SubTotal": ["10.00", "12.00"],
            "TaxAmt": ["1.00", "1.20"],
            "Freight": ["2.00", "2.00"],
            "TotalDue": ["13.00", "15.20"],
            "OnlineOrderFlag": [True, True],
            "Status": [5, 5],
            "_source_system": ["AdventureWorks2012", "AdventureWorks2012"],
            "_load_date": ["2026-08-31", "2026-08-31"],
        }
    )

    result = clean_sales_order_header(frame)

    assert len(result) == 1
    assert result.iloc[0]["sales_order_id"] == 1
    assert str(result.iloc[0]["order_date"]) == "2024-01-03"
    assert result.iloc[0]["total_due"] == 15.20


def test_clean_sales_order_detail_converts_numeric_fields():
    frame = pd.DataFrame(
        {
            "SalesOrderID": [1],
            "SalesOrderDetailID": [11],
            "ProductID": [100],
            "OrderQty": ["2"],
            "UnitPrice": ["5.50"],
            "UnitPriceDiscount": ["0.10"],
            "LineTotal": ["11.00"],
            "_source_system": ["AdventureWorks2012"],
            "_load_date": ["2026-08-31"],
        }
    )

    result = clean_sales_order_detail(frame)

    assert result.iloc[0]["order_qty"] == 2
    assert result.iloc[0]["unit_price"] == 5.50
    assert result.iloc[0]["line_total"] == 11.00


def test_clean_product_sets_discontinued_flag_and_trims_name():
    frame = pd.DataFrame(
        {
            "ProductID": [100],
            "Name": ["  Road Bike  "],
            "ProductNumber": ["BK-1"],
            "ProductLine": ["R"],
            "Class": ["H"],
            "Style": ["U"],
            "ListPrice": ["100.00"],
            "StandardCost": ["50.00"],
            "DiscontinuedDate": ["2025-01-01"],
            "_source_system": ["AdventureWorks2012"],
            "_load_date": ["2026-08-31"],
        }
    )

    result = clean_product(frame)

    assert result.iloc[0]["product_name"] == "Road Bike"
    assert bool(result.iloc[0]["is_discontinued"]) is True
    assert "discontinued_date" not in result.columns


def test_clean_sales_person_fails_closed_without_person_dependency():
    salesperson_frame = pd.DataFrame(
        {
            "BusinessEntityID": [20],
            "TerritoryID": [1],
            "SalesQuota": [1000],
            "Bonus": [10],
            "CommissionPct": [0.02],
        }
    )

    with pytest.raises(RuntimeError, match=r"bronze\.person"):
        clean_sales_person(salesperson_frame)


def test_clean_sales_person_fails_closed_when_person_schema_is_incomplete():
    salesperson_frame = pd.DataFrame({"BusinessEntityID": [20]})
    person_frame = pd.DataFrame({"BusinessEntityID": [20], "FirstName": ["Linda"]})

    with pytest.raises(RuntimeError, match="last_name"):
        clean_sales_person(salesperson_frame, person_frame)


def test_clean_sales_order_header_is_deterministic_across_chunk_order():
    rows = [
        {
            "SalesOrderID": 1,
            "OrderDate": "2024-01-02",
            "DueDate": "2024-01-05",
            "ShipDate": "2024-01-04",
            "CustomerID": 10,
            "SalesPersonID": 20,
            "TerritoryID": 1,
            "SubTotal": "10.00",
            "TaxAmt": "1.00",
            "Freight": "2.00",
            "TotalDue": "13.00",
            "OnlineOrderFlag": True,
            "Status": 5,
            "_source_system": "AdventureWorks2012",
            "_load_date": "2026-08-31",
        },
        {
            "SalesOrderID": 1,
            "OrderDate": "2024-01-03",
            "DueDate": "2024-01-06",
            "ShipDate": "2024-01-05",
            "CustomerID": 10,
            "SalesPersonID": 20,
            "TerritoryID": 1,
            "SubTotal": "12.00",
            "TaxAmt": "1.40",
            "Freight": "2.50",
            "TotalDue": "15.90",
            "OnlineOrderFlag": True,
            "Status": 5,
            "_source_system": "AdventureWorks2012",
            "_load_date": "2026-09-01",
        },
    ]

    first = clean_sales_order_header(pd.DataFrame(rows[::-1]))
    second = clean_sales_order_header(pd.DataFrame(rows))

    assert first.equals(second)
    assert first.iloc[0]["total_due"] == 15.90
