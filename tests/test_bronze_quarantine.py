import pandas as pd
import pytest

from src.core.settings import Settings
from src.features.Sales_Performance.domain.bronze.bronze_validator import BronzeValidator
from src.shared.ingestion.domain_bronze_job import DomainBronzeJob
from src.shared.ingestion.ingestion_models import ExecutionIdentity, ExtractionBatch, TableSpec
from src.shared.ingestion.quarantine_service import QuarantineService
from src.shared.ingestion.staging_manager import StagingManager


def test_partition_rows_keeps_valid_rows_and_returns_queryable_rejections():
    spec = TableSpec("Sales", "Customer", "bronze", "customer", "ID", ("ID",), "ID")
    identity = ExecutionIdentity("run-1", "load-1", "batch-1")
    dataframe = pd.DataFrame(
        {"ID": [1, None], "Name": ["valid", "invalid"], "_record_hash": ["hash-1", "hash-2"]}
    )

    valid_rows, rejected_rows = BronzeValidator().partition_rows(dataframe, spec, identity)
    service = QuarantineService()
    for rejected in rejected_rows:
        service.record(rejected)

    assert valid_rows["ID"].tolist() == [1.0]
    assert service.count_for_load("load-1") == 1
    assert rejected_rows[0].reason == "NULL primary key: ID"
    assert rejected_rows[0].source_hash == "hash-2"


def test_partition_rows_fails_closed_for_missing_required_column():
    spec = TableSpec("Sales", "Customer", "bronze", "customer", "ID", ("ID", "Name"), "ID")
    identity = ExecutionIdentity("run-1", "load-1", "batch-1")

    with pytest.raises(ValueError, match="Missing required columns"):
        BronzeValidator().partition_rows(pd.DataFrame({"ID": [1]}), spec, identity)


def test_validate_table_fails_when_rejected_threshold_is_exceeded():
    result = BronzeValidator().validate_table(
        source_count=10,
        target_count=8,
        source_table="Sales.Customer",
        bronze_table="bronze.customer",
        rejected_count=2,
        rejected_threshold=1,
    )

    assert result["rejected_count"] == 2
    assert result["rejected_threshold_ok"] is False
    assert result["validation_passed"] is False
    assert any("threshold exceeded" in issue for issue in result["issues"])


class _Extractor:
    def __init__(self, batches, settings=None):
        self.batches = batches

    def iter_table_batches(self, spec, load_date):
        return iter(self.batches)


class _Loader:
    def __init__(self, settings=None):
        self.loaded = []

    def load(self, dataframe, target_schema, target_table, if_exists):
        self.loaded.append(dataframe)
        return len(dataframe), True


def _runtime_job(frame, quarantine, threshold=None, settings=None):
    spec = TableSpec("Sales", "Customer", "bronze", "customer", "ID", ("ID",), "ID")
    loader = _Loader()
    job = DomainBronzeJob(
        (spec,),
        extractor_factory=lambda settings=None: _Extractor(
            [ExtractionBatch(frame, 1, 1, len(frame))], settings
        ),
        loader_factory=lambda settings=None: loader,
        validator_factory=BronzeValidator,
        staging_manager=StagingManager(),
        quarantine_service=quarantine,
        rejected_threshold=threshold,
        settings=settings,
        sleeper=lambda _: None,
    )
    return job, loader


def test_domain_job_loads_valid_rows_and_records_rejected_rows():
    frame = pd.DataFrame(
        {
            "ID": [1, None],
            "_source_system": ["sqlserver", "sqlserver"],
            "_source_table": ["Sales.Customer", "Sales.Customer"],
            "_load_date": ["2026-09-04", "2026-09-04"],
            "_record_hash": ["hash-1", "hash-2"],
        }
    )
    quarantine = QuarantineService()
    job, loader = _runtime_job(frame, quarantine, threshold=1)

    result = job.run()["customer"]

    assert result["status"] == "SUCCESS_WITH_REJECTIONS"
    assert result["rows_read"] == 2
    assert result["rows_written"] == 1
    assert result["rows_rejected"] == 1
    assert len(loader.loaded) == 1
    assert loader.loaded[0]["ID"].tolist() == [1.0]
    assert quarantine.count_for_load(result["load_id"]) == 1


def test_domain_job_default_threshold_zero_fails_and_does_not_publish():
    frame = pd.DataFrame(
        {
            "ID": [1, None],
            "_source_system": ["sqlserver", "sqlserver"],
            "_source_table": ["Sales.Customer", "Sales.Customer"],
            "_load_date": ["2026-09-04", "2026-09-04"],
            "_record_hash": ["hash-1", "hash-2"],
        }
    )
    quarantine = QuarantineService()
    job, loader = _runtime_job(frame, quarantine)

    result = job.run()["customer"]

    assert job.rejected_threshold == 0
    assert result["status"] == "FAILED"
    assert result["rejected_threshold"] == 0
    assert result["published"] is False
    assert loader.loaded == []
    assert quarantine.count_for_load(result["load_id"]) == 1


def test_domain_job_uses_settings_threshold_override():
    frame = pd.DataFrame(
        {
            "ID": [1, None],
            "_source_system": ["sqlserver", "sqlserver"],
            "_source_table": ["Sales.Customer", "Sales.Customer"],
            "_load_date": ["2026-09-04", "2026-09-04"],
            "_record_hash": ["hash-1", "hash-2"],
        }
    )
    settings = Settings(
        sql_server_host="sql-host",
        postgres_username="warehouse-user",
        postgres_password="warehouse-secret",
        bronze_rejected_threshold=1,
        _env_file=None,
    )
    quarantine = QuarantineService()
    job, loader = _runtime_job(frame, quarantine, settings=settings)

    result = job.run()["customer"]

    assert job.rejected_threshold == 1
    assert result["status"] == "SUCCESS_WITH_REJECTIONS"
    assert result["rejected_threshold"] == 1
    assert result["published"] is True
    assert len(loader.loaded) == 1


def test_domain_job_schema_error_fails_closed_without_quarantine():
    frame = pd.DataFrame(
        {
            "_source_system": ["sqlserver"],
            "_source_table": ["Sales.Customer"],
            "_load_date": ["2026-09-04"],
        }
    )
    quarantine = QuarantineService()
    job, loader = _runtime_job(frame, quarantine)

    result = job.run()["customer"]

    assert loader.loaded == []
    assert quarantine.records == []
    assert result["status"] == "FAILED"
    assert result["error_type"] == "ValueError"
    assert "Missing required columns" in result["error_message"]