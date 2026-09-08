from datetime import datetime
import hashlib
import inspect
import time
from typing import Callable, Dict, Iterable, Optional, TypeVar

import pandas as pd

from src.core.settings import Settings, get_settings
from src.shared.ingestion.ingestion_models import TableSpec
from src.shared.ingestion.audit_service import AuditService
from src.shared.ingestion.ingestion_models import (
    BatchLoadAudit,
    ExecutionIdentity,
    IngestionResult,
    IngestionStatus,
    RunAudit,
    TableLoadAudit,
    deterministic_batch_id,
    utc_now,
)
from src.shared.ingestion.reconciliation_service import ReconciliationService
from src.shared.ingestion.retry_policy import RetryPolicy, execute_with_retry
from src.shared.ingestion.quarantine_service import QuarantineService
from src.shared.ingestion.staging_manager import StagingManager

Component = TypeVar("Component")


class DomainBronzeJob:
    """Run shared Bronze mechanics for the specs owned by one domain."""

    def __init__(
        self,
        table_specs: Iterable[TableSpec],
        extractor_factory: Callable[..., Component],
        loader_factory: Callable[..., Component],
        validator_factory: Callable[..., Component],
        settings: Optional[Settings] = None,
        staging_manager: StagingManager | None = None,
        audit_service: AuditService | None = None,
        retry_policy: RetryPolicy | None = None,
        sleeper: Callable[[float], None] | None = None,
        quarantine_service: QuarantineService | None = None,
        rejected_threshold: int | None = None,
        reconciliation_service=None,
        publish_service=None,
        checkpoint_manager=None,
        staging_reader: Callable[..., pd.DataFrame] | None = None,
    ):
        self.settings = settings or get_settings()
        self.table_specs = tuple(table_specs)
        self.extractor = extractor_factory(settings=self.settings)
        self.loader = loader_factory(settings=self.settings)
        self.validator = validator_factory()
        self.staging_manager = staging_manager or StagingManager()
        self.audit_service = audit_service or AuditService()
        self.retry_policy = retry_policy or RetryPolicy(
            max_attempts=self.settings.retry_max_attempts,
            initial_delay_seconds=self.settings.retry_initial_delay_seconds,
            max_delay_seconds=self.settings.retry_max_delay_seconds,
        )
        self.sleeper = sleeper or time.sleep
        self.reconciliation = reconciliation_service or ReconciliationService(
            self.staging_manager
        )
        self.publish_service = publish_service
        self.checkpoint_manager = checkpoint_manager
        self.staging_reader = staging_reader or getattr(self.loader, "read_staging", None)
        self.quarantine_service = quarantine_service or QuarantineService()
        self.rejected_threshold = (
            self.settings.bronze_rejected_threshold
            if rejected_threshold is None
            else rejected_threshold
        )
        if self.rejected_threshold < 0:
            raise ValueError("rejected_threshold cannot be negative")

    def run(
        self,
        mode: str = "full",
        load_date: Optional[datetime] = None,
        resume_load_ids: dict[str, str] | None = None,
    ) -> Dict[str, Dict]:
        if load_date is None:
            load_date = datetime.now()

        reconcile = getattr(self.audit_service, "reconcile_stale_runs", None)
        if reconcile is not None:
            reconcile(self.settings.bronze_stale_run_timeout_seconds)

        results = {}
        for spec in self.table_specs:
            resume_load_id = (resume_load_ids or {}).get(spec.target_name)
            table_identity = self._identity_for_resume(resume_load_id)
            resume_after = self._resume_after(resume_load_id)
            is_resuming = resume_load_id is not None
            staging = self.staging_manager.create(
                spec.target_table,
                table_identity.run_id,
                table_identity.load_id,
            )
            started_at = utc_now()
            table_started_clock = time.perf_counter()
            self.audit_service.record_run(
                RunAudit(
                    table_identity.run_id,
                    "bronze",
                    mode,
                    IngestionStatus.STARTED,
                    started_at,
                )
            )
            self.audit_service.record_run(
                RunAudit(
                    table_identity.run_id,
                    "bronze",
                    mode,
                    IngestionStatus.READING,
                    started_at,
                )
            )
            existing_frame = self._read_existing_staging(staging.name, spec, is_resuming)
            rows_read = self._prior_count(resume_load_id, "rows_read")
            rows_written = self._prior_count(resume_load_id, "rows_written")
            rows_rejected = self._prior_count(resume_load_id, "rows_rejected")
            batch_frames = []
            if not existing_frame.empty:
                batch_frames.append(existing_frame)
            success = True
            attempt_count = 1
            batch_error = None
            prior_batch_count = len(self.audit_service.batches_for_load(resume_load_id)) if resume_load_id else 0
            for batch in self._iter_table_batches(spec, load_date, resume_after):
                batch_started_clock = time.perf_counter()
                effective_batch_number = batch.batch_number + prior_batch_count
                batch_id = deterministic_batch_id(
                    spec.source_name,
                    spec.ordering_key,
                    batch.lower_bound,
                    batch.upper_bound,
                    load_date,
                )
                content_hash = self._content_hash(batch.dataframe)
                rows_read += batch.row_count
                if _elapsed_seconds(table_started_clock) > self.settings.bronze_table_timeout_seconds:
                    success = False
                    batch_error = TimeoutError(
                        f"Bronze table timeout exceeded for {spec.source_name}"
                    )
                    break
                if _elapsed_seconds(batch_started_clock) > self.settings.bronze_batch_timeout_seconds:
                    success = False
                    batch_error = TimeoutError(
                        f"Bronze batch timeout exceeded for {spec.source_name}: "
                        f"batch_number={batch.batch_number}"
                    )
                    break
                try:
                    valid_dataframe, rejected_records = self.validator.partition_rows(
                        batch.dataframe,
                        spec,
                        ExecutionIdentity(
                            table_identity.run_id, table_identity.load_id, batch_id
                        ),
                    )
                except Exception as error:  # noqa: BLE001 - schema errors fail closed
                    success = False
                    batch_error = error
                    self.audit_service.record_batch(
                        BatchLoadAudit(
                            batch_id=batch_id,
                            load_id=table_identity.load_id,
                            batch_number=effective_batch_number,
                            lower_bound=str(batch.lower_bound) if batch.lower_bound is not None else None,
                            upper_bound=str(batch.upper_bound) if batch.upper_bound is not None else None,
                            rows_read=batch.row_count,
                            rows_written=0,
                            rows_rejected=0,
                            attempt_count=1,
                            status=IngestionStatus.FAILED,
                            committed_at=None,
                        )
                    )
                    break
                for rejected_record in rejected_records:
                    self.quarantine_service.record(rejected_record)
                batch_rejected = len(rejected_records)
                rows_rejected += batch_rejected
                if (
                    self.rejected_threshold is not None
                    and rows_rejected > self.rejected_threshold
                ):
                    success = False
                    batch_error = ValueError(
                        f"Rejected row threshold exceeded for {spec.source_name}: "
                        f"rejected_count={rows_rejected}, "
                        f"threshold={self.rejected_threshold}"
                    )
                    break
                if valid_dataframe.empty:
                    self.audit_service.record_batch(
                        BatchLoadAudit(
                            batch_id=batch_id,
                            load_id=table_identity.load_id,
                            batch_number=effective_batch_number,
                            lower_bound=str(batch.lower_bound) if batch.lower_bound is not None else None,
                            upper_bound=str(batch.upper_bound) if batch.upper_bound is not None else None,
                            rows_read=batch.row_count,
                            rows_written=0,
                            rows_rejected=batch_rejected,
                            attempt_count=1,
                            status=IngestionStatus.QUARANTINED,
                            committed_at=None,
                            duration_ms=_duration_ms(batch_started_clock),
                        )
                    )
                    continue
                batch_frames.append(valid_dataframe)
                reconciliation = self.reconciliation.resolve(
                    staging.name, batch_id, content_hash
                )
                if reconciliation == "SKIP":
                    written, batch_success = batch.row_count, True
                    batch_attempts = 1
                else:
                    try:
                        self.audit_service.record_run(
                            RunAudit(
                                table_identity.run_id,
                                "bronze",
                                mode,
                                IngestionStatus.LOADING,
                                started_at,
                            )
                        )

                        def on_retry(attempt, delay, error):
                            self.audit_service.record_run(
                                RunAudit(
                                    table_identity.run_id,
                                    "bronze",
                                    mode,
                                    IngestionStatus.RETRYING,
                                    started_at,
                                    error_count=1,
                                )
                            )

                        staging_schema = getattr(
                            self.loader, "staging_schema", spec.target_schema
                        )
                        if (
                            self.checkpoint_manager is not None
                            and hasattr(self.loader, "load_batch_transactionally")
                        ):
                            load_operation = lambda: self.loader.load_batch_transactionally(
                                valid_dataframe,
                                staging_schema,
                                staging.name,
                                batch_id,
                                batch.upper_bound,
                                self.checkpoint_manager,
                                content_hash,
                                "append" if is_resuming or effective_batch_number > 1 else "replace",
                            )
                        else:
                            load_operation = lambda: self.loader.load(
                                valid_dataframe,
                                staging_schema,
                                staging.name,
                                if_exists="append" if is_resuming or effective_batch_number > 1 else "replace",
                            )
                        (written, batch_success), batch_attempts, _ = execute_with_retry(
                            load_operation,
                            self.retry_policy,
                            self.sleeper,
                            on_retry=on_retry,
                        )
                        if _elapsed_seconds(batch_started_clock) > self.settings.bronze_batch_timeout_seconds:
                            raise TimeoutError(
                                f"Bronze batch timeout exceeded for {spec.source_name}: "
                                f"batch_number={batch.batch_number}"
                            )
                    except Exception as error:  # noqa: BLE001 - result boundary records failure
                        written, batch_success = 0, False
                        batch_attempts = self.retry_policy.max_attempts
                        batch_error = error
                attempt_count = max(attempt_count, batch_attempts)
                success = success and batch_success
                rows_written += written
                if batch_success:
                    self.staging_manager.write_batch(
                        staging.name,
                        batch_id,
                        effective_batch_number,
                        written,
                        batch.lower_bound,
                        batch.upper_bound,
                        content_hash,
                    )
                self.audit_service.record_batch(
                    BatchLoadAudit(
                        batch_id=batch_id,
                        load_id=table_identity.load_id,
                        batch_number=effective_batch_number,
                        lower_bound=str(batch.lower_bound) if batch.lower_bound is not None else None,
                        upper_bound=str(batch.upper_bound) if batch.upper_bound is not None else None,
                        rows_read=batch.row_count,
                        rows_written=written,
                        rows_rejected=batch_rejected,
                        attempt_count=batch_attempts,
                        status=(IngestionStatus.SUCCESS if batch_success else IngestionStatus.FAILED),
                        committed_at=utc_now() if batch_success else None,
                        content_hash=content_hash,
                        duration_ms=_duration_ms(batch_started_clock),
                    )
                )
                if not batch_success:
                    break

            self.audit_service.record_run(
                RunAudit(
                    table_identity.run_id,
                    "bronze",
                    mode,
                    IngestionStatus.VALIDATING,
                    started_at,
                )
            )
            self.audit_service.record_table_load(
                TableLoadAudit(
                    load_id=table_identity.load_id,
                    run_id=table_identity.run_id,
                    stage="bronze",
                    source_table=spec.source_name,
                    target_table=spec.target_name,
                    status=(IngestionStatus.SUCCESS if success else IngestionStatus.FAILED),
                    rows_read=rows_read,
                    rows_written=rows_written,
                    rows_rejected=rows_rejected,
                    attempt_count=1,
                    started_at=started_at,
                    finished_at=utc_now(),
                    duration_ms=_duration_ms(table_started_clock),
                )
            )

            finished_at = utc_now()
            validation_report = None
            validation_ok = False
            error_type = None
            error_message = None
            if batch_error is not None:
                error_type = type(batch_error).__name__
                error_message = str(batch_error)
            if success and batch_frames:
                try:
                    staged_dataframe = pd.concat(batch_frames, ignore_index=True)
                    validation_report = self.validator.validate_staging(
                        staged_dataframe,
                        spec,
                        source_count=rows_read - rows_rejected,
                        rejected_count=rows_rejected,
                        rejected_threshold=self.rejected_threshold,
                    )
                    validation_ok = validation_report["validation_passed"]
                    if not validation_ok:
                        error_type = "ValidationError"
                        error_message = "; ".join(validation_report.get("issues", []))
                    if validation_ok:
                        self.staging_manager.mark_validated(
                            staging.name, validation_report
                        )
                        if self.publish_service is not None:
                            self.publish_service.publish(
                                spec.target_table, staging.name, validation_report
                            )
                        self.staging_manager.publish(spec.target_table, staging.name)
                        self.staging_manager.mark_audit_complete(staging.name)
                        self.audit_service.record_run(
                            RunAudit(
                                table_identity.run_id,
                                "bronze",
                                mode,
                                IngestionStatus.PUBLISHED,
                                started_at,
                            )
                        )
                except Exception as error:  # noqa: BLE001 - result boundary records failure
                    error_type = type(error).__name__
                    error_message = str(error)
            elif not batch_frames:
                validation_report = {
                    "validation_passed": rows_read == 0,
                    "empty_source": rows_read == 0,
                    "issues": (
                        ["Source returned zero rows; publish was skipped"]
                        if rows_read == 0
                        else ["No valid rows remained after quarantine; publish was skipped"]
                    ),
                }
                if rows_read and batch_error is None:
                    error_type = "ValidationError"
                    error_message = validation_report["issues"][0]

            if not success or (rows_read and not validation_ok):
                self.staging_manager.mark_failed(staging.name)

            status = (
                IngestionStatus.SUCCESS
                if success and validation_ok
                else IngestionStatus.FAILED
            )
            if status is IngestionStatus.SUCCESS and rows_rejected:
                status = IngestionStatus.SUCCESS_WITH_REJECTIONS
            if not batch_frames and success:
                status = IngestionStatus.SUCCESS
            ingestion_result = IngestionResult(
                identity=table_identity,
                stage="bronze",
                source_table=spec.source_name,
                target_table=spec.target_name,
                status=status,
                rows_read=rows_read,
                rows_written=rows_written,
                rows_rejected=rows_rejected,
                attempt_count=attempt_count,
                started_at=started_at,
                finished_at=finished_at,
                error_type=error_type,
                error_message=error_message,
                duration_ms=_duration_ms(table_started_clock),
            )
            result = ingestion_result.to_dict()
            result.update(
                {
                    "source_count": rows_read,
                    "target_count": rows_written,
                    "validation_passed": validation_ok,
                    "validation_report": validation_report,
                    "rejected_threshold": self.rejected_threshold,
                    "staging_name": staging.name,
                    "published": (
                        self.staging_manager.published_staging(spec.target_table)
                        == staging.name
                    ),
                }
            )
            results[spec.target_table] = result
            self.audit_service.record_run(
                RunAudit(
                    table_identity.run_id,
                    "bronze",
                    mode,
                    status,
                    started_at,
                    finished_at,
                    1 if status is IngestionStatus.FAILED else 0,
                )
            )

        return results

    def _identity_for_resume(self, load_id: str | None) -> ExecutionIdentity:
        if load_id is None:
            return ExecutionIdentity.create()
        table_load = self.audit_service.get_table_load(load_id)
        if table_load is None:
            raise ValueError(f"Cannot resume unknown Bronze load_id: {load_id}")
        if table_load.status in {
            IngestionStatus.SUCCESS,
            IngestionStatus.SUCCESS_WITH_REJECTIONS,
        }:
            raise ValueError(f"Bronze load_id is already complete: {load_id}")
        return ExecutionIdentity(table_load.run_id, load_id, "")

    def _resume_after(self, load_id: str | None):
        if load_id is None:
            return None
        if self.checkpoint_manager is None:
            raise RuntimeError("Checkpoint manager is required for Bronze resume")
        batches = self.audit_service.batches_for_load(load_id)
        checkpoint = self.checkpoint_manager.latest_for_load(
            [batch.batch_id for batch in batches if batch.status is IngestionStatus.SUCCESS]
        )
        if checkpoint is None:
            raise RuntimeError(f"No committed checkpoint found for Bronze load_id: {load_id}")
        return checkpoint.upper_bound

    def _read_existing_staging(self, staging_name: str, spec: TableSpec, is_resuming: bool):
        if not is_resuming:
            return pd.DataFrame()
        if self.staging_reader is None:
            raise RuntimeError("Staging reader is required for Bronze resume validation")
        return self.staging_reader(staging_name, spec)

    def _prior_count(self, load_id: str | None, field: str) -> int:
        if load_id is None:
            return 0
        return sum(
            int(getattr(batch, field, 0))
            for batch in self.audit_service.batches_for_load(load_id)
            if batch.status is IngestionStatus.SUCCESS
        )

    def _iter_table_batches(self, spec: TableSpec, load_date: datetime, start_after):
        iterator = self.extractor.iter_table_batches
        if start_after is None:
            return iterator(spec, load_date)
        parameters = inspect.signature(iterator).parameters
        if "start_after" not in parameters:
            raise RuntimeError(
                f"Extractor does not support checkpoint resume for {spec.source_name}"
            )
        return iterator(spec, load_date, start_after=start_after)

    @staticmethod
    def _content_hash(dataframe) -> str:
        values = pd.util.hash_pandas_object(dataframe, index=True).values.tobytes()
        columns = "\x1f".join(str(column) for column in dataframe.columns).encode()
        return hashlib.sha256(columns + values).hexdigest()


def _duration_ms(started_clock: float) -> int:
    return max(0, int((time.perf_counter() - started_clock) * 1000))


def _elapsed_seconds(started_clock: float) -> float:
    return time.perf_counter() - started_clock
