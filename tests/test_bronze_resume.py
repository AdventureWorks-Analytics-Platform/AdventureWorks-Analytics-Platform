import pandas as pd

from src.shared.ingestion.audit_service import AuditService
from src.shared.ingestion.checkpoint_manager import CheckpointManager
from src.shared.ingestion.domain_bronze_job import DomainBronzeJob
from src.shared.ingestion.ingestion_models import ExtractionBatch, TableSpec
from src.shared.ingestion.retry_policy import RetryPolicy
from src.shared.ingestion.staging_manager import StagingManager


class _ResumeExtractor:
    def __init__(self, batches, settings=None):
        self.batches = batches
        self.resume_calls = []

    def iter_table_batches(self, spec, load_date, start_after=None):
        self.resume_calls.append(start_after)
        if start_after is None:
            return iter(self.batches)
        return iter(
            batch for batch in self.batches if batch.lower_bound > start_after
        )


class _ResumeLoader:
    staging_schema = "bronze_staging"

    def __init__(self, settings=None):
        self.frames = {}
        self.fail_second_batch = True

    def load_batch_transactionally(
        self,
        dataframe,
        target_schema,
        target_table,
        batch_id,
        upper_bound,
        checkpoint_manager,
        content_hash,
        if_exists,
    ):
        if self.fail_second_batch and upper_bound == 2:
            raise TimeoutError("temporary write timeout")
        if if_exists == "replace":
            self.frames[target_table] = dataframe.copy()
        else:
            self.frames[target_table] = pd.concat(
                [self.frames.get(target_table, pd.DataFrame()), dataframe],
                ignore_index=True,
            )
        checkpoint_manager.mark_committed(batch_id)
        checkpoint_manager.advance(batch_id, upper_bound)
        return len(dataframe), True

    def read_staging(self, staging_name, spec):
        return self.frames.get(staging_name, pd.DataFrame()).copy()


def _frame(ids):
    return pd.DataFrame(
        {
            "ID": ids,
            "_source_system": ["sqlserver"] * len(ids),
            "_source_table": ["Sales.Customer"] * len(ids),
            "_load_date": ["2026-09-05"] * len(ids),
            "_record_hash": [f"hash-{item}" for item in ids],
        }
    )


def test_restart_resume_reuses_identity_appends_and_validates_full_staging():
    batches = (
        ExtractionBatch(_frame([1]), 1, 1, 1),
        ExtractionBatch(_frame([2]), 2, 2, 2),
    )
    spec = TableSpec("Sales", "Customer", "bronze", "customer", "ID", ("ID",), "ID")
    extractor = _ResumeExtractor(batches)
    loader = _ResumeLoader()
    checkpoint = CheckpointManager()
    audit = AuditService()
    job = DomainBronzeJob(
        (spec,),
        extractor_factory=lambda settings=None: extractor,
        loader_factory=lambda settings=None: loader,
        validator_factory=lambda: __import__(
            "src.features.Sales_Performance.domain.bronze.bronze_validator",
            fromlist=["BronzeValidator"],
        ).BronzeValidator(),
        staging_manager=StagingManager(),
        audit_service=audit,
        checkpoint_manager=checkpoint,
        retry_policy=RetryPolicy(max_attempts=1),
        sleeper=lambda _: None,
    )

    first = job.run(load_date=__import__("datetime").datetime(2026, 9, 5))[
        "customer"
    ]
    assert first["status"] == "FAILED"
    load_id = first["load_id"]

    loader.fail_second_batch = False
    resumed = job.run(
        load_date=__import__("datetime").datetime(2026, 9, 5),
        resume_load_ids={"bronze.customer": load_id},
    )["customer"]

    assert resumed["status"] == "SUCCESS"
    assert resumed["load_id"] == load_id
    assert resumed["rows_written"] == 2
    assert extractor.resume_calls[-1] == 1
    assert loader.frames[resumed["staging_name"]]["ID"].tolist() == [1, 2]
