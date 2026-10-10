import pandas as pd
import pytest

from src.features.Sales_Performance.domain.bronze.bronze_validator import BronzeValidator
from src.shared.ingestion.domain_bronze_job import DomainBronzeJob
from src.shared.ingestion.ingestion_models import ExtractionBatch, TableSpec
from src.shared.ingestion.staging_manager import StagingManager


def _spec():
    return TableSpec(
        "Sales", "Customer", "bronze", "customer", "ID", ("ID",), "ID"
    )


def _valid_frame():
    return pd.DataFrame(
        {
            "ID": [1, 2],
            "_source_system": ["sqlserver", "sqlserver"],
            "_source_table": ["Sales.Customer", "Sales.Customer"],
            "_load_date": ["2026-09-04", "2026-09-04"],
            "_record_hash": ["hash-1", "hash-2"],
        }
    )


def test_full_validation_passes_and_publish_marks_staging_published():
    spec = _spec()
    report = BronzeValidator().validate_staging(_valid_frame(), spec, source_count=2)
    manager = StagingManager()
    staging = manager.create("customer", "new-run", "new-load")

    manager.mark_validated(staging.name, report)
    published = manager.publish("customer", staging.name)

    assert report["validation_passed"] is True
    assert published.published is True
    assert published.validation_report == report
    assert manager.published_staging("customer") == staging.name


def test_full_validation_failure_does_not_replace_previous_published_staging():
    spec = _spec()
    manager = StagingManager()
    old = manager.create("customer", "old-run", "old-load")
    manager.mark_validated(old.name, {"validation_passed": True})
    manager.publish("customer", old.name)

    invalid = _valid_frame().drop(columns=["_record_hash"])
    report = BronzeValidator().validate_staging(invalid, spec, source_count=2)
    new = manager.create("customer", "new-run", "new-load")

    assert report["validation_passed"] is False
    with pytest.raises(ValueError, match="did not pass"):
        manager.mark_validated(new.name, report)
    assert manager.published_staging("customer") == old.name


class _FakeExtractor:
    def __init__(self, batches, settings=None):
        self.batches = batches

    def iter_table_batches(self, spec, load_date):
        return iter(self.batches)


class _FakeLoader:
    def __init__(self, settings=None):
        pass

    def load(self, dataframe, target_schema, target_table, if_exists):
        return len(dataframe), True


def _job(batches, manager=None):
    return DomainBronzeJob(
        table_specs=(_spec(),),
        extractor_factory=lambda settings=None: _FakeExtractor(batches, settings),
        loader_factory=_FakeLoader,
        validator_factory=BronzeValidator,
        staging_manager=manager or StagingManager(),
    )


def test_domain_job_returns_standard_result_and_publishes_after_full_validation():
    frame = _valid_frame()
    result = _job([ExtractionBatch(frame, 1, 1, 2)]).run()["customer"]

    assert result["status"] == "SUCCESS"
    assert result["rows_read"] == 2
    assert result["rows_written"] == 2
    assert result["validation_passed"] is True
    assert result["published"] is True
    assert result["run_id"]
    assert result["load_id"]
    assert result["started_at"]
    assert result["finished_at"]


def test_domain_job_validation_failure_returns_failed_without_publishing():
    manager = StagingManager()
    old = manager.create("customer", "old-run", "old-load")
    manager.mark_validated(old.name, {"validation_passed": True})
    manager.publish("customer", old.name)
    invalid = _valid_frame().drop(columns=["_record_hash"])

    result = _job([ExtractionBatch(invalid, 1, 1, 2)], manager).run()["customer"]

    assert result["status"] == "FAILED"
    assert result["validation_passed"] is False
    assert result["published"] is False
    assert result["error_type"] == "ValidationError"
    assert manager.published_staging("customer") == old.name


def test_domain_job_empty_source_returns_zero_row_result_without_publish():
    manager = StagingManager()
    result = _job([], manager).run()["customer"]

    assert result["status"] == "SUCCESS"
    assert result["rows_read"] == 0
    assert result["rows_written"] == 0
    assert result["validation_report"]["empty_source"] is True
    assert result["published"] is False
    assert manager.published_staging("customer") is None
