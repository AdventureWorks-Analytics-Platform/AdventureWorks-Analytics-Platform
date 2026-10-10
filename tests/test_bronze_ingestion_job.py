from src.features.Sales_Performance.domain.bronze.bronze_validator import BronzeValidator
from src.features.Sales_Performance.domain.bronze.sales_extractor import SalesExtractor
from src.features.Sales_Performance.jobs.sales_bronze_ingestion_job import SalesBronzeIngestionJob


def test_sales_bronze_ingestion_job_exists():
    job = SalesBronzeIngestionJob()
    assert job is not None
    assert hasattr(job, "run")


def test_bronze_validator_reports_lineage_and_null_checks():
    validator = BronzeValidator()
    result = validator.validate_table(
        source_count=10,
        target_count=10,
        source_table="Sales.SalesOrderHeader",
        bronze_table="bronze.sales_order_header",
        lineage_columns=["_source_system", "_source_table", "_load_date", "_record_hash"],
        critical_columns={"SalesOrderID": 0, "CustomerID": 0},
        null_counts={"SalesOrderID": 0, "CustomerID": 0},
    )

    assert result["count_match"] is True
    assert result["validation_passed"] is True
    assert result["lineage_columns_ok"] is True
    assert result["critical_columns_ok"] is True


def test_sales_extractor_converts_row_objects_to_tuples(monkeypatch):
    class FakeRow:
        def __init__(self, values):
            self._values = list(values)

        def __iter__(self):
            return iter(self._values)

        def __len__(self):
            return len(self._values)

        def __getitem__(self, index):
            return self._values[index]

    class FakeCursor:
        description = [("SalesOrderID",), ("CustomerID",), ("OrderDate",)]

        def __init__(self):
            self.read = False

        def execute(self, query):
            return None

        def fetchmany(self, size):
            if self.read or not size:
                return []
            self.read = True
            return [FakeRow([101, 42, "2024-01-01"])]

        def close(self):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    class FakeSQLConnector:
        def __init__(self, settings=None):
            self.connection = FakeConnection()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "src.features.Sales_Performance.domain.bronze.sales_extractor.SQLServerConnector",
        FakeSQLConnector,
    )

    df = SalesExtractor().extract_table("Sales", "SalesOrderHeader")

    assert list(df.columns[:3]) == ["SalesOrderID", "CustomerID", "OrderDate"]
    assert df.shape[0] == 1
    assert set(["_source_system", "_source_table", "_load_date"]).issubset(df.columns)


def test_sales_extractor_adds_record_hash_lineage(monkeypatch):
    class FakeRow:
        def __init__(self, values):
            self._values = list(values)

        def __iter__(self):
            return iter(self._values)

        def __len__(self):
            return len(self._values)

        def __getitem__(self, index):
            return self._values[index]

    class FakeCursor:
        description = [("SalesOrderID",), ("CustomerID",), ("OrderDate",)]

        def __init__(self):
            self.read = False

        def execute(self, query):
            return None

        def fetchmany(self, size):
            if self.read or not size:
                return []
            self.read = True
            return [FakeRow([101, 42, "2024-01-01"])]

        def close(self):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

    class FakeSQLConnector:
        def __init__(self, settings=None):
            self.connection = FakeConnection()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "src.features.Sales_Performance.domain.bronze.sales_extractor.SQLServerConnector",
        FakeSQLConnector,
    )

    df = SalesExtractor().extract_table("Sales", "SalesOrderHeader")

    assert "_record_hash" in df.columns
    assert len(df["_record_hash"].iloc[0]) == 64
