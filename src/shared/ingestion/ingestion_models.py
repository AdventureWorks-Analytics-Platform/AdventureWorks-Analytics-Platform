from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import re
from typing import Any, Optional
from uuid import uuid4


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TableSpec:
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    primary_key: str
    required_columns: tuple[str, ...] = ()
    ordering_key: Optional[str] = None
    incremental_column: Optional[str] = None

    def __post_init__(self):
        identifiers = {
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "primary_key": self.primary_key,
            "ordering_key": self.ordering_key,
            "incremental_column": self.incremental_column,
        }
        for name, value in identifiers.items():
            if value is not None and not _IDENTIFIER_PATTERN.fullmatch(value):
                raise ValueError(f"Invalid identifier for {name}: {value!r}")

        if not self.required_columns:
            raise ValueError("required_columns must not be empty")
        if self.primary_key not in self.required_columns:
            raise ValueError("primary_key must be included in required_columns")
        if self.ordering_key is None:
            raise ValueError("ordering_key is required for stable reads")

    @property
    def source_name(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    @property
    def target_name(self) -> str:
        return f"{self.target_schema}.{self.target_table}"


@dataclass(frozen=True)
class ExtractionBatch:
    dataframe: Any
    batch_number: int
    lower_bound: Any = None
    upper_bound: Any = None

    @property
    def row_count(self) -> int:
        return len(self.dataframe)


class IngestionStatus(str, Enum):
    STARTED = "STARTED"
    READING = "READING"
    VALIDATING = "VALIDATING"
    LOADING = "LOADING"
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_REJECTIONS = "SUCCESS_WITH_REJECTIONS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    QUARANTINED = "QUARANTINED"
    PUBLISHED = "PUBLISHED"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def deterministic_batch_id(
    source_table: str,
    ordering_key: str,
    lower_bound: Any,
    upper_bound: Any,
    source_snapshot: Any,
) -> str:
    payload = json.dumps(
        {
            "source_table": source_table,
            "ordering_key": ordering_key,
            "lower_bound": str(lower_bound),
            "upper_bound": str(upper_bound),
            "source_snapshot": str(source_snapshot),
        },
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class ExecutionIdentity:
    run_id: str
    load_id: str
    batch_id: str

    @classmethod
    def create(cls) -> "ExecutionIdentity":
        return cls(str(uuid4()), str(uuid4()), str(uuid4()))


@dataclass
class IngestionResult:
    identity: ExecutionIdentity
    stage: str
    source_table: str
    target_table: str
    status: IngestionStatus = IngestionStatus.SUCCESS
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    attempt_count: int = 1
    started_at: datetime = field(default_factory=utc_now)
    finished_at: Optional[datetime] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None

    def __post_init__(self):
        if self.rows_read < 0 or self.rows_written < 0 or self.rows_rejected < 0:
            raise ValueError("row counts cannot be negative")
        if self.attempt_count < 1:
            raise ValueError("attempt_count must be at least 1")

    def to_dict(self) -> dict[str, Any]:
        result = {
            "run_id": self.identity.run_id,
            "load_id": self.identity.load_id,
            "batch_id": self.identity.batch_id,
            "stage": self.stage,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "status": self.status.value,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_rejected": self.rows_rejected,
            "attempt_count": self.attempt_count,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "duration_ms": self.duration_ms,
        }
        return result


@dataclass(frozen=True)
class RunAudit:
    run_id: str
    pipeline_name: str
    mode: str
    status: IngestionStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_count: int = 0
    error_type: Optional[str] = None
    error_message: Optional[str] = None


@dataclass(frozen=True)
class TableLoadAudit:
    load_id: str
    run_id: str
    stage: str
    source_table: str
    target_table: str
    status: IngestionStatus
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    attempt_count: int = 0
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    started_at: datetime = field(default_factory=utc_now)
    finished_at: Optional[datetime] = None
    duration_ms: Optional[int] = None


@dataclass(frozen=True)
class BatchLoadAudit:
    batch_id: str
    load_id: str
    batch_number: int
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    attempt_count: int = 0
    status: IngestionStatus = IngestionStatus.STARTED
    committed_at: Optional[datetime] = None
    content_hash: Optional[str] = None
    duration_ms: Optional[int] = None


@dataclass(frozen=True)
class RejectedRecord:
    run_id: str
    load_id: str
    batch_id: str
    source_table: str
    record_key: Optional[str]
    source_hash: Optional[str]
    reason: str
    rejected_at: datetime
    transform_version: str = "unknown"
    error_type: str = "TransformationError"
