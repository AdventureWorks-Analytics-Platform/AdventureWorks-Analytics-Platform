import pandas as pd
import pytest

from src.shared.ingestion.domain_bronze_job import DomainBronzeJob
from src.shared.ingestion.ingestion_models import (
    ExtractionBatch,
    IngestionStatus,
    TableSpec,
    deterministic_batch_id,
)
from src.shared.ingestion.reconciliation_service import ReconciliationService
from src.shared.ingestion.retry_policy import RetryPolicy
from src.shared.ingestion.staging_manager import StagingManager


def test_batch_identity_is_deterministic_for_same_source_boundary():
    first = deterministic_batch_id("Sales.Customer", "ID", 1, 100, "snapshot-1")
    second = deterministic_batch_id("Sales.Customer", "ID", 1, 100, "snapshot-1")

    assert first == second
    assert first != deterministic_batch_id("Sales.Customer", "ID", 1, 101, "snapshot-1")


def test_staging_skips_same_batch_hash_and_rejects_hash_drift():
    manager = StagingManager()
    staging = manager.create("customer", "run-1", "load-1")
    manager.write_batch(staging.name, "batch-1", 1, 2, 1, 2, "hash-1")

    repeated = manager.write_batch(staging.name, "batch-1", 1, 2, 1, 2, "hash-1")

    assert repeated.content_hash == "hash-1"
    assert manager.get(staging.name).rows_written == 2
    with pytest.raises(ValueError, match="different content hash"):
        manager.write_batch(staging.name, "batch-1", 1, 2, 1, 2, "hash-2")


def test_reconciliation_resolves_unknown_commit_before_retry():
    manager = StagingManager()
    staging = manager.create("customer", "run-1", "load-1")
    service = ReconciliationService(manager)

    assert service.resolve(staging.name, "batch-1", "hash-1") == "RETRY"
    manager.write_batch(staging.name, "batch-1", 1, 2, 1, 2, "hash-1")
    assert service.resolve(staging.name, "batch-1", "hash-1") == "SKIP"
    with pytest.raises(ValueError, match="different content hash"):
        service.resolve(staging.name, "batch-1", "hash-2")


class _RetryExtractor:
    def __init__(self, batch, settings=None):
        self.batch = batch

    def iter_table_batches(self, spec, load_date):
        return iter((self.batch,))


class _TransientLoader:
    attempts = 0

    def __init__(self, settings=None):
        pass

    def load(self, dataframe, target_schema, target_table, if_exists):
        type(self).attempts += 1
        if self.attempts == 1:
            raise TimeoutError("temporary timeout")
        return len(dataframe), True


class _AlwaysFailingLoader:
    def __init__(self, settings=None):
        pass

    def load(self, dataframe, target_schema, target_table, if_exists):
        raise ConnectionError("connection reset")


def test_domain_job_retries_transient_write_with_same_batch_identity():
    frame = pd.DataFrame(
        {
            "ID": [1],
            "_source_system": ["sqlserver"],
            "_source_table": ["Sales.Customer"],
            "_load_date": ["2026-09-04"],
            "_record_hash": ["hash-1"],
        }
    )
    batch = ExtractionBatch(frame, 1, 1, 1)
    spec = TableSpec("Sales", "Customer", "bronze", "customer", "ID", ("ID",), "ID")
    manager = StagingManager()
    _TransientLoader.attempts = 0
    job = DomainBronzeJob(
        (spec,),
        extractor_factory=lambda settings=None: _RetryExtractor(batch, settings),
        loader_factory=_TransientLoader,
        validator_factory=lambda: __import__(
            "src.features.Sales_Performance.domain.bronze.bronze_validator",
            fromlist=["BronzeValidator"],
        ).BronzeValidator(),
        staging_manager=manager,
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.01, max_delay_seconds=0.01),
        sleeper=lambda _: None,
    )

    result = job.run()["customer"]
    audit = job.audit_service.batches[0]

    assert result["status"] == "SUCCESS"
    assert result["attempt_count"] == 2
    assert result["duration_ms"] is not None
    assert audit.attempt_count == 2
    assert len(manager.get(result["staging_name"]).batch_ids) == 1
    statuses = [audit.status for audit in job.audit_service.runs]
    assert statuses[0:2] == [IngestionStatus.STARTED, IngestionStatus.READING]
    assert IngestionStatus.RETRYING in statuses
    assert IngestionStatus.VALIDATING in statuses
    assert IngestionStatus.PUBLISHED in statuses


def test_domain_job_retry_exhaustion_fails_without_publishing():
    frame = pd.DataFrame(
        {
            "ID": [1],
            "_source_system": ["sqlserver"],
            "_source_table": ["Sales.Customer"],
            "_load_date": ["2026-09-04"],
            "_record_hash": ["hash-1"],
        }
    )
    batch = ExtractionBatch(frame, 1, 1, 1)
    spec = TableSpec("Sales", "Customer", "bronze", "customer", "ID", ("ID",), "ID")
    manager = StagingManager()
    job = DomainBronzeJob(
        (spec,),
        extractor_factory=lambda settings=None: _RetryExtractor(batch, settings),
        loader_factory=_AlwaysFailingLoader,
        validator_factory=lambda: __import__(
            "src.features.Sales_Performance.domain.bronze.bronze_validator",
            fromlist=["BronzeValidator"],
        ).BronzeValidator(),
        staging_manager=manager,
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.01, max_delay_seconds=0.01),
        sleeper=lambda _: None,
    )

    result = job.run()["customer"]

    assert result["status"] == "FAILED"
    assert result["attempt_count"] == 2
    assert result["published"] is False
    assert manager.get(result["staging_name"]).batch_ids == ()
    assert IngestionStatus.RETRYING in [audit.status for audit in job.audit_service.runs]
    assert IngestionStatus.PUBLISHED not in [audit.status for audit in job.audit_service.runs]
