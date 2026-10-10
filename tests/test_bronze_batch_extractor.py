import pandas as pd

from src.core.settings import Settings
from src.features.Sales_Performance.domain.bronze.sales_extractor import SalesExtractor
from src.shared.ingestion.ingestion_models import TableSpec


def test_iter_table_batches_uses_fetchmany_order_and_bounds(monkeypatch):
    class FakeCursor:
        description = [("ID",), ("Value",)]

        def __init__(self):
            self.fetch_sizes = []
            self.batches = [[[1, "a"], [2, "b"]], [[3, "c"]], []]

        def execute(self, query):
            self.query = query

        def fetchmany(self, size):
            self.fetch_sizes.append(size)
            return self.batches.pop(0)

        def close(self):
            pass

    class FakeConnection:
        def __init__(self):
            self.cursor_instance = FakeCursor()

        def cursor(self):
            return self.cursor_instance

    class FakeSQLConnector:
        last_cursor = None

        def __init__(self, settings=None):
            self.connection = FakeConnection()
            FakeSQLConnector.last_cursor = self.connection.cursor_instance

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "src.features.Sales_Performance.domain.bronze.sales_extractor.SQLServerConnector",
        FakeSQLConnector,
    )
    settings = Settings(
        sql_server_host="sql-host",
        postgres_username="warehouse-user",
        postgres_password="warehouse-secret",
        batch_size=2,
        _env_file=None,
    )
    spec = TableSpec(
        "Sales", "Example", "bronze", "example", "ID", ("ID",), "ID"
    )

    batches = list(SalesExtractor(settings=settings).iter_table_batches(spec))

    assert [batch.batch_number for batch in batches] == [1, 2]
    assert [(batch.lower_bound, batch.upper_bound) for batch in batches] == [
        (1, 2),
        (3, 3),
    ]
    assert all(batch.dataframe.shape[0] > 0 for batch in batches)
    assert batches[0].dataframe["_record_hash"].map(len).eq(64).all()
    assert FakeSQLConnector.last_cursor.query == "SELECT * FROM Sales.Example ORDER BY ID"
    assert FakeSQLConnector.last_cursor.fetch_sizes == [2, 2, 2]


def test_iter_table_batches_adds_lineage_to_each_batch(monkeypatch):
    class FakeCursor:
        description = [("ID",)]
        rows = [[[10]], []]

        def execute(self, query):
            self.query = query

        def fetchmany(self, size):
            return self.rows.pop(0)

        def close(self):
            pass

    class FakeSQLConnector:
        def __init__(self, settings=None):
            self.connection = self

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    monkeypatch.setattr(
        "src.features.Sales_Performance.domain.bronze.sales_extractor.SQLServerConnector",
        FakeSQLConnector,
    )
    settings = Settings(
        sql_server_host="sql-host",
        postgres_username="warehouse-user",
        postgres_password="warehouse-secret",
        _env_file=None,
    )
    spec = TableSpec("Sales", "Example", "bronze", "example", "ID", ("ID",), "ID")

    batch = next(SalesExtractor(settings=settings).iter_table_batches(spec))

    assert isinstance(batch.dataframe, pd.DataFrame)
    assert batch.dataframe.loc[0, "_source_table"] == "Sales.Example"
    assert batch.lower_bound == batch.upper_bound == 10
