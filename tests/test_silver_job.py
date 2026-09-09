import dataclasses

import pandas as pd
import pytest

from scripts.transformation.silver import sales_silver_clean
from src.features.Sales_Performance.jobs.sales_silver_job import (
    SALES_SILVER_TABLE_SPECS,
    SalesSilverJob,
    SilverValidationError,
    SilverTransformationJob,
)
from src.shared.ingestion.ingestion_models import TableSpec
from src.shared.ingestion.quarantine_service import QuarantineService
from src.shared.ingestion.checkpoint_manager import CheckpointManager
from src.shared.ingestion.retry_policy import RetryPolicy
from src.shared.ingestion.staging_manager import StagingManager


def test_silver_table_specs_are_immutable_and_qualified():
    spec = SALES_SILVER_TABLE_SPECS[0]

    assert dataclasses.is_dataclass(spec)
    assert spec.source_name == "bronze.sales_order_header"
    assert spec.target_name == "silver.sales_order_header_clean"
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.target_table = "changed"


def test_silver_job_is_injectable_and_preserves_dependency_order():
    job = SalesSilverJob()

    assert isinstance(job, SilverTransformationJob)
    assert job.table_specs == SALES_SILVER_TABLE_SPECS
    assert job.dependency_order == [
        "sales_order_header",
        "sales_order_detail",
        "customer",
        "sales_territory",
        "product",
        "sales_person",
    ]


def test_legacy_run_delegates_to_sales_silver_job(monkeypatch):
    captured = {"called": False}

    class DummyJob:
        def run(self):
            return {"status": "delegated"}

    def fake_builder(*args, **kwargs):
        captured["called"] = True
        return DummyJob()

    monkeypatch.setattr(sales_silver_clean, "SalesSilverJob", fake_builder)

    assert sales_silver_clean.run() == {"status": "delegated"}
    assert captured["called"] is True


def test_missing_person_dependency_fails_closed():
    def fake_reader(source_table, _settings):
        if source_table == "sales_person":
            return pd.DataFrame({"BusinessEntityID": [1], "TerritoryID": [2], "SalesQuota": [100]})
        if source_table == "person":
            raise FileNotFoundError("bronze.person missing")
        raise AssertionError(f"unexpected source table: {source_table}")

    job = SalesSilverJob(reader=fake_reader)

    with pytest.raises(RuntimeError, match="bronze.person.*sales_person"):
        job.run()


def test_silver_job_reads_chunks_with_batch_size_and_stable_order(monkeypatch):
    recorded = {}

    def fake_read_sql_query(query, con, chunksize=None):
        recorded["query"] = query
        recorded["chunksize"] = chunksize
        return [
            pd.DataFrame(
                {
                    "SalesOrderID": [1, 2],
                    "OrderDate": ["2024-01-01", "2024-01-02"],
                    "CustomerID": [10, 20],
                    "SalesPersonID": [5, 7],
                    "TerritoryID": [1, 1],
                    "Status": [5, 5],
                    "SubTotal": [15, 10],
                    "TaxAmt": [2, 1],
                    "Freight": [3, 2],
                    "TotalDue": [20, 13],
                    "OnlineOrderFlag": [False, True],
                    "_load_date": ["2026-08-31", "2026-08-31"],
                    "_source_system": ["AdventureWorks2012", "AdventureWorks2012"],
                }
            )
        ]

    monkeypatch.setattr("pandas.read_sql_query", fake_read_sql_query)
    job = SalesSilverJob(settings=type("Settings", (), {"batch_size": 2500})())

    rows = list(job.read_chunks("sales_order_header", "bronze.sales_order_header"))

    assert recorded["chunksize"] == 2500
    assert "ORDER BY" in recorded["query"]
    assert rows[0].iloc[0]["SalesOrderID"] == 1
    assert rows[0].iloc[1]["SalesOrderID"] == 2


def test_silver_job_assigns_deterministic_batch_identity_and_record_hashes(monkeypatch):
    def fake_read_sql_query(query, con, chunksize=None):
        return [
            pd.DataFrame(
                {
                    "SalesOrderID": [1, 2],
                    "OrderDate": ["2024-01-01", "2024-01-02"],
                    "CustomerID": [10, 20],
                    "SalesPersonID": [5, 7],
                    "TerritoryID": [1, 1],
                    "Status": [5, 5],
                    "SubTotal": [15, 10],
                    "TaxAmt": [2, 1],
                    "Freight": [3, 2],
                    "TotalDue": [20, 13],
                    "OnlineOrderFlag": [False, True],
                    "_load_date": ["2026-08-31", "2026-08-31"],
                    "_source_system": ["AdventureWorks2012", "AdventureWorks2012"],
                }
            )
        ]

    monkeypatch.setattr("pandas.read_sql_query", fake_read_sql_query)
    job = SalesSilverJob(settings=type("Settings", (), {"batch_size": 2500})())

    rows = list(job.read_chunks("sales_order_header", "bronze.sales_order_header", run_id="run-1", load_id="load-1"))

    assert rows[0]["run_id"].tolist() == ["run-1", "run-1"]
    assert rows[0]["load_id"].tolist() == ["load-1", "load-1"]
    assert rows[0]["batch_id"].nunique() == 1
    assert rows[0]["_record_hash"].map(len).eq(64).all()
    assert rows[0]["_record_hash"].tolist()[0] == rows[0]["_record_hash"].tolist()[0]


def test_silver_job_retries_transient_injected_reader():
    attempts = []

    def flaky_reader(_source_table, _settings):
        attempts.append(1)
        if len(attempts) == 1:
            raise TimeoutError("temporary read timeout")
        return pd.DataFrame({"sample_id": [1], "value": [10]})

    job = SilverTransformationJob(
        table_specs=(_sample_spec(),),
        reader=flaky_reader,
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.01),
        sleeper=lambda _delay: None,
    )

    rows = list(job.read_chunks("sample", "bronze.sample"))

    assert attempts == [1, 1]
    assert rows[0]["sample_id"].tolist() == [1]


def test_silver_job_does_not_retry_deterministic_reader_error():
    attempts = []

    def invalid_reader(_source_table, _settings):
        attempts.append(1)
        raise ValueError("invalid reader contract")

    job = SilverTransformationJob(
        table_specs=(_sample_spec(),),
        reader=invalid_reader,
        retry_policy=RetryPolicy(max_attempts=3),
        sleeper=lambda _delay: None,
    )

    with pytest.raises(ValueError, match="invalid reader contract"):
        list(job.read_chunks("sample", "bronze.sample"))

    assert attempts == [1]


def test_silver_job_transforms_each_chunk_without_concatenating_bronze(monkeypatch):
    spec = TableSpec(
        "bronze",
        "sample",
        "silver",
        "sample_clean",
        "sample_id",
        ("sample_id", "value"),
        "sample_id",
    )
    transformed_sizes = []
    written = []

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: (
            transformed_sizes.append(len(frame)) or frame.assign(value=frame["value"] * 2)
        ),
        writer=lambda frame, target_name: written.append((target_name, frame)),
    )

    def fake_read_chunks(*_args, **_kwargs):
        yield pd.DataFrame({"sample_id": [1], "value": [10]})
        yield pd.DataFrame({"sample_id": [2], "value": [20]})

    monkeypatch.setattr(job, "read_chunks", fake_read_chunks)

    result = job.run()

    assert transformed_sizes == [1, 1]
    assert result["sample_clean"]["source_count"] == 2
    assert result["sample_clean"]["target_count"] == 2
    assert result["sample_clean"]["rows_valid"] == 2
    assert result["sample_clean"]["rows_rejected"] == 0
    assert result["sample_clean"]["rejected_threshold"] == 0
    assert result["sample_clean"]["rows_deduplicated"] == 0
    assert result["sample_clean"]["rows_published"] == 2
    assert result["sample_clean"]["rejection_reasons"] == []
    assert result["sample_clean"]["run_id"]
    assert result["sample_clean"]["load_id"]
    assert result["sample_clean"]["batch_id"]
    assert result["sample_clean"]["stage"] == "silver"
    assert result["sample_clean"]["source_table"] == "bronze.sample"
    assert result["sample_clean"]["target_table"] == "silver.sample_clean"
    assert result["sample_clean"]["started_at"] <= result["sample_clean"]["finished_at"]
    assert result["sample_clean"]["error_type"] is None
    assert result["sample_clean"]["error_message"] is None
    assert written[0][0] == "sample_clean"
    assert written[0][1]["value"].tolist() == [20, 40]


def test_silver_job_fails_closed_when_bronze_input_schema_is_missing(monkeypatch):
    spec = SALES_SILVER_TABLE_SPECS[0]
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
    )

    def fake_read_chunks(*_args, **_kwargs):
        yield pd.DataFrame({"OrderDate": ["2026-09-05"]})

    monkeypatch.setattr(job, "read_chunks", fake_read_chunks)

    result = job.run()[spec.target_table]

    assert result["status"] == "FAILED"
    assert result["error_type"] == "SilverValidationError"
    assert "Missing required Bronze columns" in result["error_message"]
    assert result["stage"] == "silver"
    assert result["source_table"] == "bronze.sales_order_header"
    assert result["target_table"] == "silver.sales_order_header_clean"
    assert result["started_at"] <= result["finished_at"]


def test_silver_job_rejects_invalid_conversion_and_keeps_valid_rows():
    job = SalesSilverJob(reader=lambda _source_table, _settings: pd.DataFrame())
    frame = pd.DataFrame(
        {
            "SalesOrderID": [1, 2],
            "OrderDate": ["2026-09-05", "not-a-date"],
            "DueDate": ["2026-09-06", "2026-09-06"],
            "ShipDate": ["2026-09-06", "2026-09-06"],
            "CustomerID": [10, 20],
            "SalesPersonID": [30, 40],
            "TerritoryID": [1, 1],
            "SubTotal": [10, 20],
            "TaxAmt": [1, 2],
            "Freight": [1, 2],
            "TotalDue": [12, 24],
            "OnlineOrderFlag": [True, False],
            "Status": [5, 5],
        }
    )

    valid, rejected = job._partition_conversion_errors(
        frame, SALES_SILVER_TABLE_SPECS[0]
    )

    assert valid["SalesOrderID"].tolist() == [1]
    assert len(rejected) == 1
    assert rejected[0]["record_key"] == "2"
    assert rejected[0]["reason"] == "invalid date: OrderDate"


def test_silver_job_fails_closed_when_output_schema_has_unexpected_column(monkeypatch):
    spec = TableSpec(
        "bronze",
        "sample",
        "silver",
        "sample_clean",
        "sample_id",
        ("sample_id", "value"),
        "sample_id",
    )
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.assign(extra=1),
        writer=lambda _frame, _target_name: None,
    )

    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter([pd.DataFrame({"sample_id": [1], "value": [10]})]),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "FAILED"
    assert result["error_type"] == "SilverValidationError"
    assert "unexpected" in result["error_message"]


def _header_frame_with_one_invalid_row():
    return pd.DataFrame(
        {
            "SalesOrderID": [1, 2],
            "OrderDate": ["2026-09-05", "invalid-date"],
            "DueDate": ["2026-09-06", "2026-09-06"],
            "ShipDate": ["2026-09-06", "2026-09-06"],
            "CustomerID": [10, 20],
            "SalesPersonID": [30, 40],
            "TerritoryID": [1, 1],
            "SubTotal": [10, 20],
            "TaxAmt": [1, 2],
            "Freight": [1, 2],
            "TotalDue": [12, 24],
            "OnlineOrderFlag": [True, False],
            "Status": [5, 5],
            "_source_system": ["AdventureWorks2012", "AdventureWorks2012"],
            "_load_date": ["2026-09-05", "2026-09-05"],
            "run_id": ["run-1", "run-1"],
            "load_id": ["load-1", "load-1"],
            "batch_id": ["batch-1", "batch-1"],
            "_record_hash": ["hash-1", "hash-2"],
        }
    )


def test_silver_job_quarantines_rejections_and_keeps_valid_rows(monkeypatch):
    spec = SALES_SILVER_TABLE_SPECS[0]
    quarantine = QuarantineService()
    written = []
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        writer=lambda frame, _target_name: written.append(frame),
        quarantine_service=quarantine,
        rejected_threshold=1,
        transform_version="silver-v1-test",
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter([_header_frame_with_one_invalid_row()]),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "SUCCESS_WITH_REJECTIONS"
    assert result["rows_rejected"] == 1
    assert result["rows_published"] == 1
    assert len(written) == 1
    assert quarantine.count_for_load("load-1") == 1
    assert quarantine.records[0].transform_version == "silver-v1-test"
    assert quarantine.records[0].record_key == "2"
    assert "invalid date: OrderDate" in quarantine.records[0].reason


def test_silver_job_rejection_threshold_blocks_writer(monkeypatch):
    spec = SALES_SILVER_TABLE_SPECS[0]
    quarantine = QuarantineService()
    written = []
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        writer=lambda frame, _target_name: written.append(frame),
        quarantine_service=quarantine,
        rejected_threshold=0,
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter([_header_frame_with_one_invalid_row()]),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "FAILED"
    assert result["error_type"] == "SilverRejectionThresholdError"
    assert result["rows_rejected"] == 1
    assert result["rejected_threshold"] == 0
    assert written == []
    assert quarantine.count_for_load("load-1") == 1


def test_silver_job_fails_closed_for_duplicate_detail_grain():
    spec = SALES_SILVER_TABLE_SPECS[1]
    frame = pd.DataFrame({"sales_order_detail_id": [10, 10]})

    with pytest.raises(SilverValidationError, match="Duplicate detail grain"):
        SilverTransformationJob._validate_detail_grain(frame, spec)


def _sample_spec():
    return TableSpec(
        "bronze",
        "sample",
        "silver",
        "sample_clean",
        "sample_id",
        ("sample_id", "value"),
        "sample_id",
    )


def test_silver_job_stages_batches_checkpoints_and_deduplicates_globally(monkeypatch):
    spec = _sample_spec()
    staging_manager = StagingManager()
    checkpoint_manager = CheckpointManager()
    written = []
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        writer=lambda frame, _target_name: written.append(frame),
        staging_manager=staging_manager,
        checkpoint_manager=checkpoint_manager,
        sleeper=lambda _delay: None,
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [
                pd.DataFrame(
                    {"sample_id": [1], "value": [10], "_load_date": ["2026-09-04"]}
                ),
                pd.DataFrame(
                    {"sample_id": [1], "value": [20], "_load_date": ["2026-09-05"]}
                ),
            ]
        ),
    )

    result = job.run(run_id="run-1", load_id="load-1")[spec.target_table]

    staging = staging_manager.get(result["staging_name"])
    assert staging.run_id == "run-1"
    assert staging.load_id == "load-1"
    assert len(staging.batch_ids) == 2
    assert all(checkpoint_manager.get(batch_id) for batch_id in staging.batch_ids)
    assert result["rows_deduplicated"] == 1
    assert result["rows_published"] == 1
    assert written[0]["value"].tolist() == [20]


def test_silver_job_retries_transient_staging_write(monkeypatch):
    spec = _sample_spec()
    attempts = []

    def flaky_staging_writer(frame, staging_name):
        attempts.append((staging_name, len(frame)))
        if len(attempts) == 1:
            raise TimeoutError("temporary staging timeout")

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        writer=lambda _frame, _target_name: None,
        staging_writer=flaky_staging_writer,
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.01),
        sleeper=lambda _delay: None,
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [pd.DataFrame({"sample_id": [1], "value": [10]})]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["attempt_count"] == 2
    assert len(attempts) == 2


def test_silver_job_reconciliation_advances_checkpoint_after_unknown_commit(monkeypatch):
    spec = _sample_spec()
    checkpoint_manager = CheckpointManager()
    advance_attempts = []

    original_advance = checkpoint_manager.advance

    def fail_once(batch_id, upper_bound):
        advance_attempts.append(batch_id)
        if len(advance_attempts) == 1:
            raise TimeoutError("checkpoint timeout after staging commit")
        return original_advance(batch_id, upper_bound)

    checkpoint_manager.advance = fail_once
    staging_writes = []
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        writer=lambda _frame, _target_name: None,
        staging_writer=lambda frame, staging_name: staging_writes.append(staging_name),
        checkpoint_manager=checkpoint_manager,
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.01),
        sleeper=lambda _delay: None,
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [pd.DataFrame({"sample_id": [1], "value": [10]})]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["attempt_count"] == 2
    assert len(staging_writes) == 1
    assert checkpoint_manager.get(advance_attempts[0]) is not None


def test_silver_validation_gate_blocks_writer_and_marks_staging_failed(monkeypatch):
    spec = _sample_spec()
    staging_manager = StagingManager()
    written = []
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        writer=lambda frame, _target_name: written.append(frame),
        staging_manager=staging_manager,
        validator=lambda *_args: {
            "validation_passed": False,
            "issues": ["join validation failed"],
        },
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [pd.DataFrame({"sample_id": [1], "value": [10]})]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "FAILED"
    assert result["published"] is False
    assert written == []
    assert staging_manager.get(result["staging_name"]).lifecycle == "FAILED"


def test_silver_publish_service_is_called_only_after_validation(monkeypatch):
    spec = _sample_spec()
    staging_manager = StagingManager()
    published = []
    written = []

    class FakePublishService:
        def publish(self, target_table, staging_name, validation_report):
            published.append((target_table, staging_name, validation_report))
            return f"silver.{target_table}"

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        writer=lambda frame, _target_name: written.append(frame),
        staging_manager=staging_manager,
        publish_service=FakePublishService(),
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [pd.DataFrame({"sample_id": [1], "value": [10]})]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "SUCCESS"
    assert result["published"] is True
    assert result["published_target"] == "silver.sample_clean"
    assert len(published) == 1
    assert published[0][2]["validation_passed"] is True
    assert written == []
    assert staging_manager.published_staging(spec.target_table) == result["staging_name"]


def test_silver_publish_failure_preserves_previous_published_staging(monkeypatch):
    spec = _sample_spec()
    staging_manager = StagingManager()
    previous = staging_manager.create(spec.target_table, "old-run", "old-load")
    staging_manager.mark_validated(previous.name, {"validation_passed": True})
    staging_manager.publish(spec.target_table, previous.name)

    class FailingPublishService:
        def publish(self, _target_table, _staging_name, _validation_report):
            raise TimeoutError("publish timeout")

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        staging_manager=staging_manager,
        publish_service=FailingPublishService(),
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [pd.DataFrame({"sample_id": [1], "value": [10]})]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "FAILED"
    assert result["published"] is False
    assert staging_manager.published_staging(spec.target_table) == previous.name


def test_silver_publish_uses_deduplicated_staging_content_not_raw_batches(monkeypatch):
    """publish() renames the staging table as-is; it must reflect the deduplicated
    frame, not the raw per-batch appends, or cross-batch duplicates would ship."""
    spec = _sample_spec()
    staging_manager = StagingManager()
    per_batch_calls = []

    class FakeStagingWriter:
        def __call__(self, frame, staging_name):
            per_batch_calls.append(frame.copy())

        def replace_all(self, frame, staging_name):
            self.final_content = frame.copy()

    staging_writer = FakeStagingWriter()

    class FakePublishService:
        def publish(self, target_table, staging_name, validation_report):
            return f"silver.{target_table}"

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        staging_manager=staging_manager,
        staging_writer=staging_writer,
        publish_service=FakePublishService(),
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [
                pd.DataFrame({"sample_id": [1], "value": [10], "_load_date": ["2026-09-04"]}),
                pd.DataFrame({"sample_id": [1], "value": [20], "_load_date": ["2026-09-05"]}),
            ]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "SUCCESS"
    assert len(per_batch_calls) == 2  # both raw batches were staged, including the duplicate
    assert staging_writer.final_content["value"].tolist() == [20]  # but publish sees deduped content


class _FakeDedupService:
    def __init__(self, duplicate_detail_count=0):
        self.duplicate_detail_count = duplicate_detail_count
        self.deduplicate_calls = []
        self.count_calls = []

    def count_duplicate_keys(self, staging_name, primary_key):
        self.count_calls.append((staging_name, primary_key))
        return self.duplicate_detail_count

    def deduplicate(self, staging_name, primary_key):
        self.deduplicate_calls.append((staging_name, primary_key))
        return 1


def test_silver_job_uses_sql_dedup_service_and_skips_in_memory_concat(monkeypatch):
    """When a dedup_service is injected, no transformed chunk is ever accumulated
    for a global pandas concat/dedup; SQL owns dedup, staging_reader owns validation input."""
    spec = _sample_spec()
    staging_manager = StagingManager()
    dedup_service = _FakeDedupService()
    staging_reader_calls = []

    def staging_reader(staging_name):
        staging_reader_calls.append(staging_name)
        return pd.DataFrame({"sample_id": [1], "value": [20]})

    class FakePublishService:
        def publish(self, target_table, staging_name, validation_report):
            return f"silver.{target_table}"

    silver_chunks_seen = []
    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: (
            silver_chunks_seen.append(frame) or frame.copy()
        ),
        staging_manager=staging_manager,
        staging_writer=lambda _frame, _staging_name: None,
        staging_reader=staging_reader,
        dedup_service=dedup_service,
        publish_service=FakePublishService(),
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [
                pd.DataFrame({"sample_id": [1], "value": [10], "_load_date": ["2026-09-04"]}),
                pd.DataFrame({"sample_id": [1], "value": [20], "_load_date": ["2026-09-05"]}),
            ]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "SUCCESS"
    assert len(silver_chunks_seen) == 2  # both chunks were transformed and staged...
    assert dedup_service.count_calls == [(result["staging_name"], spec.primary_key)]
    assert dedup_service.deduplicate_calls == [(result["staging_name"], spec.primary_key)]
    assert staging_reader_calls == [result["staging_name"]]  # ...but only read back once for validation
    assert result["rows_deduplicated"] == 1


def test_silver_job_sql_dedup_fails_closed_on_duplicate_detail_grain(monkeypatch):
    spec = SALES_SILVER_TABLE_SPECS[1]
    assert spec.source_table == "sales_order_detail"
    staging_manager = StagingManager()
    dedup_service = _FakeDedupService(duplicate_detail_count=2)

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        staging_manager=staging_manager,
        staging_writer=lambda _frame, _staging_name: None,
        dedup_service=dedup_service,
        publish_service=object(),
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [
                pd.DataFrame(
                    {
                        "SalesOrderID": [1, 1],
                        "SalesOrderDetailID": [10, 10],
                        "ProductID": [1, 1],
                        "OrderQty": [1, 1],
                        "UnitPrice": [1.0, 1.0],
                        "UnitPriceDiscount": [0.0, 0.0],
                        "LineTotal": [1.0, 1.0],
                    }
                )
            ]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "FAILED"
    assert result["error_type"] == "SilverValidationError"
    assert "Duplicate detail grain" in result["error_message"]
    assert dedup_service.deduplicate_calls == []  # dedup never runs once grain check fails


class _FakeSqlValidator:
    def __init__(self, report):
        self.report = report
        self.calls = []

    def validate(self, staging_name, spec, source_count, rejected_count, rejected_threshold):
        self.calls.append((staging_name, source_count, rejected_count, rejected_threshold))
        return dict(self.report)


def test_silver_job_full_sql_path_never_reads_staging_into_pandas(monkeypatch):
    """With dedup_service + sql_validator, staging_reader must never be called at all."""
    spec = _sample_spec()
    staging_manager = StagingManager()
    sql_validator = _FakeSqlValidator(
        {"validation_passed": True, "target_count": 1, "issues": []}
    )
    staging_reader_calls = []
    snapshot_calls = []

    class FakeDedupServiceWithSnapshot(_FakeDedupService):
        def set_source_snapshot_id(self, staging_name, source_snapshot_id):
            snapshot_calls.append((staging_name, source_snapshot_id))

    dedup_service = FakeDedupServiceWithSnapshot()

    class FakePublishService:
        def publish(self, target_table, staging_name, validation_report):
            return f"silver.{target_table}"

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        staging_manager=staging_manager,
        staging_writer=lambda _frame, _staging_name: None,
        staging_reader=lambda name: staging_reader_calls.append(name) or pd.DataFrame(),
        dedup_service=dedup_service,
        sql_validator=sql_validator,
        publish_service=FakePublishService(),
    )
    monkeypatch.setattr(
        job,
        "read_chunks",
        lambda *_args, **_kwargs: iter(
            [pd.DataFrame({"sample_id": [1], "value": [10], "_load_date": ["2026-09-04"]})]
        ),
    )

    result = job.run()[spec.target_table]

    assert result["status"] == "SUCCESS"
    assert result["target_count"] == 1
    assert staging_reader_calls == []  # never read staging back into pandas
    assert len(sql_validator.calls) == 1
    assert snapshot_calls == [(result["staging_name"], result["run_id"])]


def test_silver_job_sql_validator_requires_publish_service():
    spec = _sample_spec()
    dedup_service = _FakeDedupService()
    sql_validator = _FakeSqlValidator({"validation_passed": True, "target_count": 0, "issues": []})

    job = SilverTransformationJob(
        table_specs=(spec,),
        reader=lambda _source_table, _settings: pd.DataFrame(),
        transformer=lambda _source_table, frame, _person_frame: frame.copy(),
        staging_writer=lambda _frame, _staging_name: None,
        dedup_service=dedup_service,
        sql_validator=sql_validator,
        writer=lambda _frame, _target_name: None,
    )

    with pytest.raises(RuntimeError, match="sql_validator requires publish_service"):
        job.run()


