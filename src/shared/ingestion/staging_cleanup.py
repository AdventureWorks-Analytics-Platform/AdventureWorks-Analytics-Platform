from datetime import datetime, timedelta, timezone

from src.shared.ingestion.staging_manager import (
    ABANDONED,
    ACTIVE,
    FAILED,
    PUBLISHED,
    StagingManager,
)


class StagingCleanupJob:
    """Expire abandoned staging and remove cleanup-eligible staging tables."""

    def __init__(
        self, staging_manager: StagingManager, retention: timedelta | None = None
    ):
        self.staging_manager = staging_manager
        self.retention = retention or timedelta(hours=24)

    def run(self, now: datetime | None = None) -> tuple[str, ...]:
        current_time = now or datetime.now(timezone.utc)
        cutoff = current_time - self.retention
        cleaned = []
        for staging in self.staging_manager.all_staging():
            if staging.lifecycle == ACTIVE:
                continue
            if staging.lifecycle == PUBLISHED:
                if staging.audit_completed_at is None:
                    continue
                self.staging_manager.cleanup(staging.name)
                cleaned.append(staging.name)
                continue
            if (
                staging.lifecycle in {FAILED, ABANDONED}
                and staging.failed_at is not None
            ):
                if staging.failed_at <= cutoff:
                    self.staging_manager.expire(staging.name)
                    self.staging_manager.cleanup(staging.name)
                    cleaned.append(staging.name)
        return tuple(cleaned)
