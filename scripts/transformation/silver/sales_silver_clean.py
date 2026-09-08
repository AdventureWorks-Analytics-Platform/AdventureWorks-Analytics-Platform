"""Silver transformations for the AdventureWorks sales domain."""

from __future__ import annotations

import hashlib
import json
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.features.Sales_Performance.jobs.sales_silver_job import SalesSilverJob
from src.shared.connectors.postgres_connector import PostgreSQLConnector


SILVER_TABLES = {
    "sales_order_header": "sales_order_header_clean",
    "sales_order_detail": "sales_order_detail_clean",
    "customer": "customer_clean",
    "sales_territory": "sales_territory_clean",
    "sales_person": "sales_person_clean",
    "product": "product_clean",
}


def _warehouse_engine(connection):
    return create_engine("postgresql://", creator=lambda: connection, poolclass=StaticPool)


def _read_bronze(source_table: str, engine, chunksize: int = 10000):
    return pd.read_sql_query(
        f'SELECT * FROM bronze."{source_table}"',
        engine,
        chunksize=chunksize,
    )


def _rename_columns(frame: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    return frame.rename(columns={source: target for source, target in mapping.items() if source in frame.columns})


def _deduplicate(frame: pd.DataFrame, key: str) -> pd.DataFrame:
    if key not in frame.columns:
        return frame.reset_index(drop=True)

    working = frame.copy()

    def _row_signature(row: pd.Series) -> str:
        payload = row.drop(labels=[key], errors="ignore").to_dict()
        normalized = json.dumps(payload, default=str, sort_keys=True)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    if "_load_date" in working.columns:
        working["_load_date"] = pd.to_datetime(working["_load_date"], errors="coerce")

    working["_dedup_tie_breaker"] = working.apply(_row_signature, axis=1)

    if "_load_date" in working.columns:
        working = working.sort_values(["_load_date", "_dedup_tie_breaker"], ascending=[False, True], kind="mergesort")
    else:
        working = working.sort_values(["_dedup_tie_breaker"], ascending=[True], kind="mergesort")

    result = working.drop_duplicates(subset=[key], keep="first").drop(columns=["_dedup_tie_breaker"], errors="ignore").reset_index(drop=True)
    return result


def _select_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    selected = list(columns)
    if "_record_hash" in frame.columns and "_record_hash" not in selected:
        selected.append("_record_hash")
    return frame.loc[:, selected]


def clean_sales_order_header(frame: pd.DataFrame) -> pd.DataFrame:
    result = _rename_columns(
        frame,
        {
            "SalesOrderID": "sales_order_id",
            "OrderDate": "order_date",
            "DueDate": "due_date",
            "ShipDate": "ship_date",
            "CustomerID": "customer_id",
            "SalesPersonID": "salesperson_id",
            "TerritoryID": "territory_id",
            "SubTotal": "subtotal",
            "TaxAmt": "tax_amt",
            "Freight": "freight",
            "TotalDue": "total_due",
            "OnlineOrderFlag": "is_online_order",
            "Status": "status_code",
            "_source_system": "_source_system",
            "_load_date": "_load_date",
        },
    )
    for column in ["order_date", "due_date", "ship_date"]:
        result[column] = pd.to_datetime(result[column], errors="coerce").dt.date
    for column in ["subtotal", "tax_amt", "freight", "total_due"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = _deduplicate(result, "sales_order_id")
    return _select_columns(result, [
        "sales_order_id", "order_date", "due_date", "ship_date", "customer_id",
        "salesperson_id", "territory_id", "subtotal", "tax_amt", "freight",
        "total_due", "is_online_order", "status_code", "_source_system", "_load_date",
    ]).sort_values("sales_order_id", kind="mergesort").reset_index(drop=True)


def clean_sales_order_detail(frame: pd.DataFrame) -> pd.DataFrame:
    result = _rename_columns(
        frame,
        {
            "SalesOrderID": "sales_order_id",
            "SalesOrderDetailID": "sales_order_detail_id",
            "ProductID": "product_id",
            "OrderQty": "order_qty",
            "UnitPrice": "unit_price",
            "UnitPriceDiscount": "unit_price_discount",
            "LineTotal": "line_total",
            "_source_system": "_source_system",
            "_load_date": "_load_date",
        },
    )
    result["order_qty"] = pd.to_numeric(result["order_qty"], errors="coerce").astype("Int64")
    for column in ["unit_price", "unit_price_discount", "line_total"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = _deduplicate(result, "sales_order_detail_id")
    return _select_columns(result, [
        "sales_order_id", "sales_order_detail_id", "product_id", "order_qty",
        "unit_price", "unit_price_discount", "line_total", "_source_system", "_load_date",
    ]).sort_values("sales_order_detail_id", kind="mergesort").reset_index(drop=True)


def clean_customer(
    frame: pd.DataFrame, person_frame: pd.DataFrame | None = None
) -> pd.DataFrame:
    result = _rename_columns(
        frame,
        {
            "CustomerID": "customer_id",
            "PersonID": "person_id",
            "StoreID": "store_id",
            "TerritoryID": "territory_id",
            "AccountNumber": "account_number",
            "_source_system": "_source_system",
            "_load_date": "_load_date",
        },
    )
    individual_mask = result["person_id"].notna()
    result["customer_type"] = individual_mask.map(
        {True: "INDIVIDUAL", False: "STORE"}
    )
    result["customer_name"] = result["account_number"].astype("string").str.strip()
    result["customer_name_source"] = "ACCOUNT"

    if individual_mask.any():
        if person_frame is None:
            raise RuntimeError(
                "Missing required Bronze dependency 'bronze.person' for "
                "individual customer enrichment."
            )

        person_clean = _rename_columns(
            person_frame,
            {
                "BusinessEntityID": "person_id",
                "FirstName": "first_name",
                "LastName": "last_name",
            },
        )
        required_person_columns = {"person_id", "first_name", "last_name"}
        missing_person_columns = required_person_columns.difference(person_clean.columns)
        if missing_person_columns:
            missing = ", ".join(sorted(missing_person_columns))
            raise RuntimeError(
                "Missing required columns in Bronze dependency 'bronze.person': "
                f"{missing}."
            )

        person_clean = person_clean[
            ["person_id", "first_name", "last_name"]
        ].drop_duplicates("person_id")
        result = result.merge(person_clean, on="person_id", how="left")
        unresolved = individual_mask & (
            result["first_name"].isna() | result["last_name"].isna()
        )
        if unresolved.any():
            unresolved_ids = result.loc[unresolved, "person_id"].tolist()
            raise RuntimeError(
                "Unresolved Person references for individual customers: "
                f"{unresolved_ids}."
            )
        result.loc[individual_mask, "customer_name"] = (
            result.loc[individual_mask, "first_name"].fillna("")
            + " "
            + result.loc[individual_mask, "last_name"].fillna("")
        ).str.strip()
        result.loc[individual_mask, "customer_name_source"] = "PERSON"
        result = result.drop(columns=["first_name", "last_name"], errors="ignore")

    result = _deduplicate(result, "customer_id")
    return _select_columns(result, [
        "customer_id", "person_id", "store_id", "territory_id", "account_number",
        "customer_name", "customer_type", "customer_name_source",
        "_source_system", "_load_date",
    ]).sort_values("customer_id", kind="mergesort").reset_index(drop=True)


def clean_sales_territory(frame: pd.DataFrame) -> pd.DataFrame:
    result = _rename_columns(
        frame,
        {
            "TerritoryID": "territory_id",
            "Name": "territory_name",
            "CountryRegionCode": "country_region_code",
            "Group": "territory_group",
            "_source_system": "_source_system",
            "_load_date": "_load_date",
        },
    )
    result["territory_name"] = result["territory_name"].astype("string").str.strip()
    result = _deduplicate(result, "territory_id")
    return _select_columns(result, [
        "territory_id", "territory_name", "country_region_code", "territory_group",
        "_source_system", "_load_date",
    ]).sort_values("territory_id", kind="mergesort").reset_index(drop=True)


def clean_sales_person(frame: pd.DataFrame, person_frame: pd.DataFrame = None) -> pd.DataFrame:
    if person_frame is None:
        raise RuntimeError(
            "Missing required Bronze dependency 'bronze.person' for Silver table 'sales_person'."
        )

    result = _rename_columns(
        frame,
        {
            "BusinessEntityID": "business_entity_id",
            "TerritoryID": "territory_id",
            "SalesQuota": "sales_quota",
            "Bonus": "bonus",
            "CommissionPct": "commission_pct",
            "_source_system": "_source_system",
            "_load_date": "_load_date",
        },
    )
    result["salesperson_id"] = result["business_entity_id"]
    
    person_clean = _rename_columns(
        person_frame,
        {
            "BusinessEntityID": "business_entity_id",
            "FirstName": "first_name",
            "LastName": "last_name",
        },
    )
    required_person_columns = {"business_entity_id", "first_name", "last_name"}
    missing_person_columns = required_person_columns.difference(person_clean.columns)
    if missing_person_columns:
        missing = ", ".join(sorted(missing_person_columns))
        raise RuntimeError(
            "Missing required columns in Bronze dependency 'bronze.person': "
            f"{missing}."
        )

    for column in ["sales_quota", "bonus", "commission_pct"]:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")

    person_clean = person_clean[["business_entity_id", "first_name", "last_name"]]
    result = result.merge(person_clean, on="business_entity_id", how="left")
    result["salesperson_name"] = (
        result["first_name"].fillna("") + " " + result["last_name"].fillna("")
    ).str.strip()
    result = result.drop(columns=["first_name", "last_name"], errors="ignore")

    result["salesperson_name"] = result["salesperson_name"].astype("string").str.strip()
    result = _deduplicate(result, "salesperson_id")
    return _select_columns(result, [
        "salesperson_id", "business_entity_id", "territory_id", "sales_quota", "bonus",
        "commission_pct", "salesperson_name", "_source_system", "_load_date",
    ]).sort_values("salesperson_id", kind="mergesort").reset_index(drop=True)


def clean_product(frame: pd.DataFrame) -> pd.DataFrame:
    result = _rename_columns(
        frame,
        {
            "ProductID": "product_id",
            "Name": "product_name",
            "ProductNumber": "product_number",
            "ProductLine": "product_line",
            "Class": "class",
            "Style": "style",
            "ListPrice": "list_price",
            "StandardCost": "standard_cost",
            "DiscontinuedDate": "discontinued_date",
            "_source_system": "_source_system",
            "_load_date": "_load_date",
        },
    )
    for column in ["list_price", "standard_cost"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result["product_name"] = result["product_name"].astype("string").str.strip()
    result["is_discontinued"] = result["discontinued_date"].notna()
    result = _deduplicate(result, "product_id")
    return _select_columns(result, [
        "product_id", "product_name", "product_number", "product_line", "class", "style",
        "list_price", "standard_cost", "is_discontinued", "_source_system", "_load_date",
    ]).sort_values("product_id", kind="mergesort").reset_index(drop=True)


CLEANERS = {
    "sales_order_header": clean_sales_order_header,
    "sales_order_detail": clean_sales_order_detail,
    "customer": clean_customer,
    "sales_territory": clean_sales_territory,
    "sales_person": clean_sales_person,
    "product": clean_product,
}


def run() -> Dict[str, Dict[str, int]]:
    """Legacy Silver entrypoint that delegates to the injectable job/service."""
    return SalesSilverJob().run()


if __name__ == "__main__":
    print(run())
