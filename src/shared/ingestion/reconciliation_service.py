from src.shared.ingestion.staging_manager import StagingManager


class ReconciliationService:
    """Resolve an unknown batch commit before an operation is retried."""

    def __init__(self, staging_manager: StagingManager):
        self.staging_manager = staging_manager

    def resolve(self, staging_name: str, batch_id: str, content_hash: str) -> str:
        existing_hash = self.staging_manager.batch_content_hash(staging_name, batch_id)
        if existing_hash is None:
            return "RETRY"
        if existing_hash == content_hash:
            return "SKIP"
        raise ValueError(f"batch identity has different content hash: {batch_id}")
