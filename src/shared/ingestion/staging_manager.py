import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


_STAGING_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ACTIVE = "ACTIVE"
PUBLISHED = "PUBLISHED"
FAILED = "FAILED"
ABANDONED = "ABANDONED"
EXPIRED = "EXPIRED"


@dataclass(frozen=True)
class StagingTable:
    name: str
    target_table: str
    run_id: str
    load_id: str
    published: bool = False
    validated: bool = False
    rows_written: int = 0
    batch_ids: tuple[str, ...] = ()
    validation_report: dict | None = None
    batch_hashes: tuple[tuple[str, str], ...] = ()
    lifecycle: str = ACTIVE
    created_at: datetime | None = None
    audit_completed_at: datetime | None = None
    failed_at: datetime | None = None


@dataclass(frozen=True)
class StagingBatch:
    staging_name: str
    batch_id: str
    batch_number: int
    rows_written: int
    lower_bound: Any = None
    upper_bound: Any = None
    content_hash: str | None = None


class StagingManager:
    """Database-independent staging/publish contract for unit tests."""

    def __init__(self):
        self._staging: dict[str, StagingTable] = {}
        self._published: dict[str, str] = {}

    def create(
        self,
        target_table: str,
        run_id: str,
        load_id: str | None = None,
    ) -> StagingTable:
        self._validate_identifier(target_table)
        safe_run_id = self._safe_identity(run_id)
        resolved_load_id = load_id or run_id
        safe_load_id = self._safe_identity(resolved_load_id)
        name = f"{target_table}__{safe_run_id}__{safe_load_id}"
        if len(name) > 63:
            identity_hash = hashlib.sha256(name.encode()).hexdigest()[:16]
            name = f"{target_table[:40]}__{identity_hash}"
        existing = self._staging.get(name)
        if existing is not None:
            return existing
        staging = StagingTable(
            name=name,
            target_table=target_table,
            run_id=run_id,
            load_id=resolved_load_id,
            created_at=datetime.now(timezone.utc),
        )
        self._staging[name] = staging
        return staging

    def write_batch(
        self,
        staging_name: str,
        batch_id: str,
        batch_number: int,
        rows_written: int,
        lower_bound: Any = None,
        upper_bound: Any = None,
        content_hash: str | None = None,
    ) -> StagingBatch:
        if not batch_id or batch_number < 1 or rows_written < 0:
            raise ValueError("batch identity, number, and row count are invalid")
        staging = self._staging[staging_name]
        if batch_id in staging.batch_ids:
            existing_hash = dict(staging.batch_hashes).get(batch_id)
            if content_hash is not None and existing_hash == content_hash:
                return StagingBatch(
                    staging_name,
                    batch_id,
                    batch_number,
                    rows_written,
                    lower_bound,
                    upper_bound,
                    content_hash,
                )
            if content_hash is None:
                raise ValueError(f"batch already written to staging: {batch_id}")
            raise ValueError(f"batch identity has different content hash: {batch_id}")
        batch = StagingBatch(
            staging_name,
            batch_id,
            batch_number,
            rows_written,
            lower_bound,
            upper_bound,
            content_hash,
        )
        self._staging[staging_name] = StagingTable(
            name=staging.name,
            target_table=staging.target_table,
            run_id=staging.run_id,
            load_id=staging.load_id,
            published=staging.published,
            validated=staging.validated,
            rows_written=staging.rows_written + rows_written,
            batch_ids=staging.batch_ids + (batch_id,),
            validation_report=staging.validation_report,
            batch_hashes=(
                staging.batch_hashes + ((batch_id, content_hash),)
                if content_hash is not None
                else staging.batch_hashes
            ),
            lifecycle=staging.lifecycle,
            created_at=staging.created_at,
            audit_completed_at=staging.audit_completed_at,
            failed_at=staging.failed_at,
        )
        return batch

    def get(self, staging_name: str) -> StagingTable:
        return self._staging[staging_name]

    def mark_validated(
        self, staging_name: str, validation_report: dict | None = None
    ) -> StagingTable:
        staging = self._staging[staging_name]
        if validation_report is not None and not validation_report.get(
            "validation_passed", False
        ):
            raise ValueError("staging validation report did not pass")
        validated = StagingTable(
            name=staging.name,
            target_table=staging.target_table,
            run_id=staging.run_id,
            load_id=staging.load_id,
            published=False,
            validated=True,
            rows_written=staging.rows_written,
            batch_ids=staging.batch_ids,
            validation_report=validation_report,
            batch_hashes=staging.batch_hashes,
            lifecycle=staging.lifecycle,
            created_at=staging.created_at,
            audit_completed_at=staging.audit_completed_at,
            failed_at=staging.failed_at,
        )
        self._staging[staging_name] = validated
        return validated

    def publish(self, target_table: str, staging_name: str) -> StagingTable:
        self._validate_identifier(target_table)
        staging = self._staging[staging_name]
        if not staging.validated:
            raise RuntimeError("staging must be validated before publish")
        if staging.target_table != target_table:
            raise ValueError("staging target does not match publish target")
        published = StagingTable(
            name=staging.name,
            target_table=staging.target_table,
            run_id=staging.run_id,
            load_id=staging.load_id,
            published=True,
            validated=True,
            rows_written=staging.rows_written,
            batch_ids=staging.batch_ids,
            validation_report=staging.validation_report,
            batch_hashes=staging.batch_hashes,
            lifecycle=PUBLISHED,
            created_at=staging.created_at,
            audit_completed_at=staging.audit_completed_at,
            failed_at=staging.failed_at,
        )
        self._staging[staging_name] = published
        self._published[target_table] = staging_name
        return published

    def published_staging(self, target_table: str) -> str | None:
        return self._published.get(target_table)

    def batch_content_hash(self, staging_name: str, batch_id: str) -> str | None:
        return dict(self._staging[staging_name].batch_hashes).get(batch_id)

    def all_staging(self) -> tuple[StagingTable, ...]:
        return tuple(self._staging.values())

    def mark_audit_complete(
        self, staging_name: str, completed_at: datetime | None = None
    ) -> StagingTable:
        staging = self._staging[staging_name]
        updated = StagingTable(
            **{
                **staging.__dict__,
                "audit_completed_at": completed_at or datetime.now(timezone.utc),
            }
        )
        self._staging[staging_name] = updated
        return updated

    def mark_failed(
        self, staging_name: str, failed_at: datetime | None = None
    ) -> StagingTable:
        return self._mark_lifecycle(
            staging_name, FAILED, failed_at or datetime.now(timezone.utc)
        )

    def mark_abandoned(
        self, staging_name: str, failed_at: datetime | None = None
    ) -> StagingTable:
        return self._mark_lifecycle(
            staging_name, ABANDONED, failed_at or datetime.now(timezone.utc)
        )

    def expire(self, staging_name: str) -> StagingTable:
        return self._mark_lifecycle(
            staging_name, EXPIRED, self._staging[staging_name].failed_at
        )

    def _mark_lifecycle(
        self, staging_name: str, lifecycle: str, timestamp: datetime | None
    ) -> StagingTable:
        staging = self._staging[staging_name]
        updated = StagingTable(
            **{
                **staging.__dict__,
                "lifecycle": lifecycle,
                "failed_at": timestamp,
            }
        )
        self._staging[staging_name] = updated
        return updated

    def cleanup(self, staging_name: str) -> None:
        self._staging.pop(staging_name, None)

    @staticmethod
    def _validate_identifier(identifier: str) -> None:
        if not _STAGING_IDENTIFIER.fullmatch(identifier):
            raise ValueError(f"Invalid staging identifier: {identifier!r}")

    @classmethod
    def _safe_identity(cls, identity: str) -> str:
        normalized = identity.replace("-", "_")
        if normalized and normalized[0].isdigit():
            normalized = f"id_{normalized}"
        if len(normalized) > 20:
            normalized = f"id_{hashlib.sha256(identity.encode()).hexdigest()[:16]}"
        cls._validate_identifier(normalized)
        return normalized