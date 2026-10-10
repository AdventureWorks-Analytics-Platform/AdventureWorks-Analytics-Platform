import dataclasses

import pytest

from src.features.Person.jobs.person_bronze_job import (
    PERSON_TABLE_SPECS,
    PersonBronzeJob,
)
from src.features.Production.jobs.production_bronze_job import (
    PRODUCTION_TABLE_SPECS,
    ProductionBronzeJob,
)
from src.features.Sales_Performance.jobs.sales_bronze_job import (
    SALES_TABLE_SPECS,
    SalesBronzeJob,
)
from src.shared.ingestion.ingestion_models import TableSpec


def test_table_spec_is_immutable_and_exposes_qualified_names():
    spec = SALES_TABLE_SPECS[0]

    assert dataclasses.is_dataclass(spec)
    assert spec.source_name == "Sales.SalesOrderHeader"
    assert spec.target_name == "bronze.sales_order_header"
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.target_table = "changed"


def test_table_spec_rejects_unsafe_or_incomplete_metadata():
    with pytest.raises(ValueError, match="Invalid identifier"):
        TableSpec(
            "Sales", "SalesOrderHeader;DROP", "bronze", "sales_order_header",
            "SalesOrderID", ("SalesOrderID",), "SalesOrderID",
        )

    with pytest.raises(ValueError, match="primary_key"):
        TableSpec(
            "Sales", "SalesOrderHeader", "bronze", "sales_order_header",
            "SalesOrderID", ("CustomerID",), "CustomerID",
        )


def test_each_domain_owns_only_its_tables():
    assert [spec.source_table for spec in SALES_TABLE_SPECS] == [
        "SalesOrderHeader",
        "SalesOrderDetail",
        "Customer",
        "SalesTerritory",
        "SalesPerson",
    ]
    assert [spec.source_table for spec in PRODUCTION_TABLE_SPECS] == ["Product"]
    assert [spec.source_table for spec in PERSON_TABLE_SPECS] == ["Person"]

    assert isinstance(SalesBronzeJob(), SalesBronzeJob)
    assert isinstance(ProductionBronzeJob(), ProductionBronzeJob)
    assert isinstance(PersonBronzeJob(), PersonBronzeJob)


def test_domain_jobs_have_independent_spec_collections():
    sales_job = SalesBronzeJob()
    production_job = ProductionBronzeJob()
    person_job = PersonBronzeJob()

    assert sales_job.table_specs == SALES_TABLE_SPECS
    assert production_job.table_specs == PRODUCTION_TABLE_SPECS
    assert person_job.table_specs == PERSON_TABLE_SPECS
