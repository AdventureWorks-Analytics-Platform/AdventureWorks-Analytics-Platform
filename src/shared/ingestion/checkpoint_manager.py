from dataclasses import dataclass
from typing import Any

from sqlalchemy import text


@dataclass(frozen=True)
class Checkpoint:
    batch_id: str
    upper_bound: Any


class CheckpointManager:
    """In-memory checkpoint contract; persistence can be injected later."""

    def __init__(self):
        self._committed_batches: set[str] = set()
        self._checkpoints: dict[str, Checkpoint] = {}

    def mark_committed(self, batch_id: str) -> None:
        self._committed_batches.add(batch_id)

    def advance(self, batch_id: str, upper_bound: Any) -> Checkpoint:
        if batch_id not in self._committed_batches:
            raise RuntimeError("checkpoint cannot advance before data commit")
        checkpoint = Checkpoint(batch_id, upper_bound)
        self._checkpoints[batch_id] = checkpoint
        return checkpoint

    def get(self, batch_id: str) -> Checkpoint | None:
        return self._checkpoints.get(batch_id)

    def latest_for_load(
        self, batch_ids: list[str] | tuple[str, ...]
    ) -> Checkpoint | None:
        for batch_id in reversed(batch_ids):
            checkpoint = self.get(batch_id)
            if checkpoint is not None:
                return checkpoint
        return None


class PostgresCheckpointManager:
    """Persist checkpoints using the transaction supplied by the data writer."""

    def advance_in_transaction(
        self,
        connection,
        batch_id: str,
        upper_bound: Any,
        content_hash: str | None = None,
    ) -> Checkpoint:
        connection.execute(
            text(
                """
                INSERT INTO bronze.ingestion_checkpoint (batch_id, upper_bound)
                VALUES (:batch_id, :upper_bound)
                ON CONFLICT (batch_id) DO UPDATE
                SET upper_bound = EXCLUDED.upper_bound,
                    committed_at = CURRENT_TIMESTAMP
                """
            ),
            {"batch_id": batch_id, "upper_bound": str(upper_bound)},
        )
        if content_hash is not None:
            connection.execute(
                text(
                    """
                    INSERT INTO bronze.ingestion_batch_registry
                        (batch_id, content_hash, upper_bound)
                    VALUES (:batch_id, :content_hash, :upper_bound)
                    ON CONFLICT (batch_id) DO UPDATE
                    SET content_hash = EXCLUDED.content_hash,
                        upper_bound = EXCLUDED.upper_bound,
                        committed_at = CURRENT_TIMESTAMP
                    """
                ),
                {
                    "batch_id": batch_id,
                    "content_hash": content_hash,
                    "upper_bound": str(upper_bound),
                },
            )
        return Checkpoint(batch_id, upper_bound)

    def get(self, batch_id: str) -> Checkpoint | None:
        from src.shared.connectors.postgres_connector import PostgreSQLConnector

        with PostgreSQLConnector() as connection:
            rows = connection.fetch_results(
                "SELECT batch_id, upper_bound FROM bronze.ingestion_checkpoint "
                "WHERE batch_id = %s",
                (batch_id,),
            )
        return Checkpoint(*rows[0]) if rows else None

    def latest_for_load(
        self, batch_ids: list[str] | tuple[str, ...]
    ) -> Checkpoint | None:
        for batch_id in reversed(batch_ids):
            checkpoint = self.get(batch_id)
            if checkpoint is not None:
                return checkpoint
        return None
