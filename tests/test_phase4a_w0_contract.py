import inspect

from src.app.app import App
from src.features.Sales_Performance.domain.bronze.bronze_loader import BronzeLoader
from src.features.Sales_Performance.domain.bronze.bronze_validator import BronzeValidator
from src.features.Sales_Performance.domain.bronze.sales_extractor import SalesExtractor
from src.features.Sales_Performance.jobs.sales_bronze_ingestion_job import (
    SALES_TABLE_SPECS,
    SalesBronzeIngestionJob,
)
from src.jobs.platform_bootstrap import PlatformBootstrapJob
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.connectors.sql_server_connector import SQLServerConnector
from src.shared.services.connection_health_service import ConnectionHealthService


def test_phase4a_public_classes_remain_importable():
    public_classes = (
        App,
        PlatformBootstrapJob,
        ConnectionHealthService,
        SalesBronzeIngestionJob,
        SalesExtractor,
        BronzeLoader,
        BronzeValidator,
        SQLServerConnector,
        PostgreSQLConnector,
    )

    assert all(inspect.isclass(public_class) for public_class in public_classes)


def test_phase4a_entrypoint_and_legacy_job_signatures_are_preserved():
    assert list(inspect.signature(App).parameters) == [
        "bootstrap_job",
        "health_service",
        "bronze_job",
        "settings",
    ]
    assert list(inspect.signature(App.run).parameters) == ["self"]
    assert list(inspect.signature(SalesBronzeIngestionJob.run).parameters) == [
        "self",
        "mode",
        "load_date",
    ]


def test_phase4a_baseline_sales_job_inventory_is_explicit():
    source = inspect.getsource(SalesBronzeIngestionJob)

    for table_name in (
        "SalesOrderHeader",
        "SalesOrderDetail",
        "Customer",
        "SalesTerritory",
        "SalesPerson",
    ):
        assert any(spec.source_table == table_name for spec in SALES_TABLE_SPECS)

    assert "Product" not in source
    assert "Person" not in source
