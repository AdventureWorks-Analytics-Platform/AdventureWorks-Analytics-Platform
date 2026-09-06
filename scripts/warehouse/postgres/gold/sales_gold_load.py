"""Build the AdventureWorks sales Gold star schema from Silver tables."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_gold_constraint_service import (
    PostgresGoldCandidateService,
)
from src.shared.ingestion.postgres_gold_publish_service import (
    PostgresGoldPublishService,
)
from src.features.Sales_Performance.jobs.sales_gold_job import (
    GOLD_TABLE_SPECS,
    FactBatch,
    GoldConstraintManager,
    SalesGoldJob,
    build_fact_batch,
)


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


class _PostgresGoldReader:
    def __init__(self, settings, batch_size):
        self.settings = settings
        self.batch_size = batch_size

    def __call__(self, table: str, *, source_snapshot_id: str) -> pd.DataFrame:
        del source_snapshot_id
        with PostgreSQLConnector(settings=self.settings) as pg:
            return _read(_engine(pg.connection), table)

    def fact_batches(self, *, source_snapshot_id: str, batch_size: int) -> Iterator[FactBatch]:
        del source_snapshot_id
        with PostgreSQLConnector(settings=self.settings) as pg:
            engine = _engine(pg.connection)
            bounds = pd.read_sql_query(
                'SELECT MIN(sales_order_detail_id) AS lower_bound, '
                'MAX(sales_order_detail_id) AS upper_bound '
                'FROM silver."sales_order_detail_clean"',
                engine,
            ).iloc[0]
            lower_bound = None
            maximum = bounds["upper_bound"]
            batch_number = 0
            headers = _read(engine, "sales_order_header_clean")
            while pd.notna(maximum) and (lower_bound is None or lower_bound < maximum):
                upper_bound = (
                    int(maximum)
                    if lower_bound is None
                    else min(int(maximum), int(lower_bound) + batch_size)
                )
                lower_sql = "" if lower_bound is None else (
                    f' AND "sales_order_detail_id" > {int(lower_bound)}'
                )
                details = pd.read_sql_query(
                    'SELECT * FROM silver."sales_order_detail_clean" '
                    f'WHERE 1=1{lower_sql} '
                    f'AND "sales_order_detail_id" <= {upper_bound} '
                    'ORDER BY "sales_order_detail_id"',
                    engine,
                )
                if details.empty:
                    break
                batch_number += 1
                yield build_fact_batch(
                    details,
                    headers,
                    batch_number=batch_number,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    source_snapshot_id=source_snapshot_id,
                )
                lower_bound = upper_bound


class _PostgresGoldPublisher:
    def __init__(self, settings):
        self.candidate_service = PostgresGoldCandidateService(settings=settings)
        self.publish_service = PostgresGoldPublishService(settings=settings)

    def prepare(self, frames, identity, specs):
        return self.candidate_service.prepare_candidate(frames, identity, specs)

    def publish(self, prepared, **kwargs):
        return self.publish_service.publish(prepared, **kwargs)


def _build_default_gold_job(settings=None) -> SalesGoldJob:
    from src.core.settings import get_settings

    resolved = settings or get_settings()
    reader = _PostgresGoldReader(resolved, resolved.batch_size)
    builders = {
        "dim_date": build_dim_date,
        "dim_customer": build_dim_customer,
        "dim_product": build_dim_product,
        "dim_territory": build_dim_territory,
        "dim_salesperson": build_dim_salesperson,
    }
    return SalesGoldJob(
        settings=resolved,
        reader=reader,
        builders=builders,
        constraint_manager=GoldConstraintManager(),
        publisher=_PostgresGoldPublisher(resolved),
        fact_batch_reader=reader.fact_batches,
    )


def run(
    *,
    pipeline_snapshot_id: str | None = None,
    silver_result: Mapping[str, Any] | None = None,
    job: SalesGoldJob | None = None,
) -> dict[str, Any]:
    """Delegate Gold execution to the injectable production job.

    This compatibility entrypoint intentionally owns no reset, retry, DDL,
    pandas write, or publication mechanics.
    """
    if not pipeline_snapshot_id or silver_result is None:
        raise ValueError(
            "legacy Gold run requires the gated pipeline_snapshot_id and silver_result"
        )
    return (job or _build_default_gold_job()).run(
        pipeline_snapshot_id=pipeline_snapshot_id,
        silver_result=silver_result,
    )
