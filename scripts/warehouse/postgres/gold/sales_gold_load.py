"""Build the AdventureWorks sales Gold star schema from Silver tables."""

from __future__ import annotations

from datetime import date
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.shared.connectors.postgres_connector import PostgreSQLConnector


class DimensionBuildError(ValueError):
    """Raised when a Gold dimension cannot satisfy its input contract."""


class FactBuildError(ValueError):
    """Raised when fact input cannot preserve the approved line-item grain."""


def _select_dimension_rows(
    frame: pd.DataFrame,
    columns: list[str],
    key: str,
) -> pd.DataFrame:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise DimensionBuildError(f"Missing dimension columns: {missing}")
    if frame[key].isna().any():
        raise DimensionBuildError(f"Dimension key contains NULL values: {key}")
    return (
        frame[columns]
        .sort_values(columns, kind="mergesort", na_position="last")
        .drop_duplicates(key, keep="first")
        .reset_index(drop=True)
    )



def _engine(connection):
    return create_engine("postgresql://", creator=lambda: connection, poolclass=StaticPool)


def _read(engine, table: str) -> pd.DataFrame:
    return pd.read_sql_query(f'SELECT * FROM silver."{table}"', engine)


def build_dim_date(headers: pd.DataFrame) -> pd.DataFrame:
    if "order_date" not in headers.columns:
        raise DimensionBuildError("Missing dimension columns: ['order_date']")
    dates = pd.to_datetime(headers["order_date"], errors="coerce").dropna().dt.date
    if dates.empty:
        raise DimensionBuildError("Date dimension requires at least one valid order_date")
    start_date = min(dates)
    end_date = max(dates)
    values = pd.date_range(start=start_date, end=end_date, freq="D")
    return pd.DataFrame(
        {
            "date_id": [value.year * 10000 + value.month * 100 + value.day for value in values],
            "full_date": values.date,
            "year_number": values.year,
            "quarter_number": values.quarter,
            "month_number": values.month,
            "month_name": values.strftime("%B"),
            "day_number": values.day,
            "is_weekend": values.dayofweek >= 5,
        }
    )


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    return _select_dimension_rows(
        customers,
        ["customer_id", "customer_name", "person_id", "store_id", "territory_id", "account_number"],
        "customer_id",
    )


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    selected = _select_dimension_rows(
        products,
        [
            "product_id", "product_name", "product_number", "product_line", "class", "style",
            "list_price", "standard_cost", "is_discontinued",
        ],
        "product_id",
    )
    return selected.rename(columns={"class": "product_class", "style": "product_style"})


def build_dim_territory(territories: pd.DataFrame) -> pd.DataFrame:
    return _select_dimension_rows(
        territories,
        ["territory_id", "territory_name", "country_region_code", "territory_group"],
        "territory_id",
    )


def build_dim_salesperson(salespeople: pd.DataFrame) -> pd.DataFrame:
    return _select_dimension_rows(
        salespeople,
        [
            "salesperson_id", "business_entity_id", "territory_id", "sales_quota", "bonus",
            "commission_pct", "salesperson_name",
        ],
        "salesperson_id",
    )


def build_fact_sales(details: pd.DataFrame, headers: pd.DataFrame) -> pd.DataFrame:
    required_detail_columns = {
        "sales_order_id", "sales_order_detail_id", "product_id", "order_qty",
        "unit_price", "unit_price_discount", "line_total",
    }
    missing_details = sorted(required_detail_columns - set(details.columns))
    if missing_details:
        raise FactBuildError(f"Missing fact detail columns: {missing_details}")
    required_header_columns = {
        "sales_order_id", "order_date", "customer_id", "territory_id", "salesperson_id",
    }
    missing_headers = sorted(required_header_columns - set(headers.columns))
    if missing_headers:
        raise FactBuildError(f"Missing fact header columns: {missing_headers}")
    if details["sales_order_detail_id"].isna().any():
        raise FactBuildError("Fact key contains NULL values: sales_order_detail_id")
    if not details["sales_order_detail_id"].is_unique:
        raise FactBuildError("Fact key is not unique: sales_order_detail_id")
    if headers["sales_order_id"].isna().any() or not headers["sales_order_id"].is_unique:
        raise FactBuildError("Header key must be non-null and unique: sales_order_id")
    result = details.merge(
        headers[["sales_order_id", "order_date", "customer_id", "territory_id", "salesperson_id"]],
        on="sales_order_id", how="left", indicator=True,
        validate="many_to_one",
    )
    if (result["_merge"] != "both").any():
        missing_orders = result.loc[result["_merge"] != "both", "sales_order_id"].tolist()
        raise FactBuildError(
            f"Missing required headers for sales_order_id values: {missing_orders}"
        )
    result = result.drop(columns=["_merge"])
    result["order_date"] = pd.to_datetime(result["order_date"], errors="coerce")
    result["order_date_id"] = (
        result["order_date"].dt.year * 10000
        + result["order_date"].dt.month * 100
        + result["order_date"].dt.day
    ).astype("Int64")
    result["order_qty"] = pd.to_numeric(result["order_qty"], errors="coerce").astype("Int64")
    result["salesperson_id"] = pd.to_numeric(result["salesperson_id"], errors="coerce").astype("Int64")
    result["unit_price"] = pd.to_numeric(result["unit_price"], errors="coerce")
    result["unit_price_discount"] = pd.to_numeric(result["unit_price_discount"], errors="coerce").fillna(0)
    result["line_total"] = pd.to_numeric(result["line_total"], errors="coerce")
    gross_total = result["order_qty"] * result["unit_price"]
    result["discount_amount"] = gross_total - result["line_total"]
    result["net_sales"] = result["line_total"]
    return result[[
        "sales_order_id", "sales_order_detail_id", "order_date_id", "customer_id", "product_id",
        "territory_id", "salesperson_id", "order_qty", "unit_price", "discount_amount",
        "line_total", "net_sales",
    ]]


def _add_constraints(pg: PostgreSQLConnector) -> None:
    statements = [
        "ALTER TABLE gold.dim_customer ADD PRIMARY KEY (customer_id)",
        "ALTER TABLE gold.dim_product ADD PRIMARY KEY (product_id)",
        "ALTER TABLE gold.dim_date ADD PRIMARY KEY (date_id)",
        "ALTER TABLE gold.dim_territory ADD PRIMARY KEY (territory_id)",
        "ALTER TABLE gold.dim_salesperson ADD PRIMARY KEY (salesperson_id)",
        "ALTER TABLE gold.fact_sales ADD PRIMARY KEY (sales_order_detail_id)",
        "ALTER TABLE gold.fact_sales ADD FOREIGN KEY (order_date_id) REFERENCES gold.dim_date(date_id)",
        "ALTER TABLE gold.fact_sales ADD FOREIGN KEY (customer_id) REFERENCES gold.dim_customer(customer_id)",
        "ALTER TABLE gold.fact_sales ADD FOREIGN KEY (product_id) REFERENCES gold.dim_product(product_id)",
        "ALTER TABLE gold.fact_sales ADD FOREIGN KEY (territory_id) REFERENCES gold.dim_territory(territory_id)",
        "ALTER TABLE gold.fact_sales ADD FOREIGN KEY (salesperson_id) REFERENCES gold.dim_salesperson(salesperson_id)",
    ]
    for statement in statements:
        pg.execute_query(statement)


def _reset_gold_tables(pg: PostgreSQLConnector) -> None:
    for table in [
        "fact_sales",
        "dim_date",
        "dim_customer",
        "dim_product",
        "dim_territory",
        "dim_salesperson",
    ]:
        pg.execute_query(f"DROP TABLE IF EXISTS gold.{table} CASCADE")


def run() -> Dict[str, int]:
    with PostgreSQLConnector() as pg:
        _reset_gold_tables(pg)
        engine = _engine(pg.connection)
        headers = _read(engine, "sales_order_header_clean")
        details = _read(engine, "sales_order_detail_clean")
        customers = _read(engine, "customer_clean")
        territories = _read(engine, "sales_territory_clean")
        salespeople = _read(engine, "sales_person_clean")
        products = _read(engine, "product_clean")

        frames = {
            "dim_date": build_dim_date(headers),
            "dim_customer": build_dim_customer(customers),
            "dim_product": build_dim_product(products),
            "dim_territory": build_dim_territory(territories),
            "dim_salesperson": build_dim_salesperson(salespeople),
            "fact_sales": build_fact_sales(details, headers),
        }
        for table, frame in frames.items():
            frame.to_sql(table, engine, schema="gold", if_exists="replace", index=False, method="multi", chunksize=1000)
        _add_constraints(pg)
        return {table: len(frame) for table, frame in frames.items()}


if __name__ == "__main__":
    print(run())
