from datetime import datetime, timedelta, timezone

from src.shared.ingestion.staging_cleanup import StagingCleanupJob
from src.shared.ingestion.staging_manager import StagingManager


NOW = datetime(2026, 9, 4, tzinfo=timezone.utc)


def test_cleanup_keeps_active_and_recent_failed_staging():
    manager = StagingManager()
    active = manager.create("active_table", "run-1", "load-1")
    recent = manager.create("recent_table", "run-2", "load-2")
    manager.mark_failed(recent.name, NOW - timedelta(hours=23))

    cleaned = StagingCleanupJob(manager).run(NOW)

    assert cleaned == ()
    assert manager.get(active.name).lifecycle == "ACTIVE"
    assert manager.get(recent.name).lifecycle == "FAILED"


def test_cleanup_expires_failed_and_abandoned_after_retention():
    manager = StagingManager()
    failed = manager.create("failed_table", "run-1", "load-1")
    abandoned = manager.create("abandoned_table", "run-2", "load-2")
    manager.mark_failed(failed.name, NOW - timedelta(hours=24))
    manager.mark_abandoned(abandoned.name, NOW - timedelta(hours=25))

    cleaned = StagingCleanupJob(manager).run(NOW)

    assert set(cleaned) == {failed.name, abandoned.name}
    assert all(table.name not in cleaned for table in manager.all_staging())


def test_cleanup_removes_published_staging_only_after_audit_completion():
    manager = StagingManager()
    staging = manager.create("published_table", "run-1", "load-1")
    manager.mark_validated(staging.name, {"validation_passed": True})
    manager.publish("published_table", staging.name)

    assert StagingCleanupJob(manager).run(NOW) == ()
    manager.mark_audit_complete(staging.name, NOW)

    assert StagingCleanupJob(manager).run(NOW) == (staging.name,)
