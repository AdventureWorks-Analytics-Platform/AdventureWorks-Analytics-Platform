from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector


class PostgresReconciliationService:
    """Resolve unknown commit outcomes from durable batch evidence."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def resolve(
        self, staging_name: str, batch_id: str, content_hash: str
    ) -> str:
        del staging_name
        with PostgreSQLConnector(settings=self.settings) as connection:
            rows = connection.fetch_results(
                """
                SELECT content_hash
                FROM bronze.ingestion_batch_registry
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            if not rows:
                rows = connection.fetch_results(
                    """
                    SELECT content_hash
                    FROM bronze.batch_load_audit
                    WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
        if not rows:
            return "RETRY"
        if rows[0][0] == content_hash:
            return "SKIP"
        raise ValueError(f"batch identity has different content hash: {batch_id}")