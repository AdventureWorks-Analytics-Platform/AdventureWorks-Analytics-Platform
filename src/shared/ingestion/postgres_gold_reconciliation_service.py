"""Database-backed unknown-commit reconciliation for Gold fact batches."""

from __future__ import annotations

import re
from typing import Any, Iterable

from psycopg2 import sql

from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.ingestion_models import utc_now
from src.shared.ingestion.retry_policy import execute_with_retry


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REGISTRY_TABLE = "gold_batch_registry"


class PostgresGoldReconciliationError(RuntimeError):
    """Raised when durable Gold batch evidence conflicts."""


class PostgresGoldReconciliationService:
    """Resolve retry decisions from Gold audit, staging, and unique-key evidence."""

    def __init__(self, settings: Settings | None = None, connector_factory=None):
        self.settings = settings or get_settings()
        self.connector_factory = connector_factory or PostgreSQLConnector

    def ensure_registry(self) -> None:
        with self.connector_factory(settings=self.settings) as connection:
            connection.execute_query(
                f"""
                CREATE TABLE IF NOT EXISTS gold.{REGISTRY_TABLE} (
                    batch_id VARCHAR(128) PRIMARY KEY,
                    candidate_schema VARCHAR(63) NOT NULL,
                    target_table VARCHAR(128) NOT NULL,
                    content_hash VARCHAR(128) NOT NULL,
                    ordering_key VARCHAR(128) NOT NULL,
                    lower_bound TEXT,
                    upper_bound TEXT,
                    committed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def record_committed_batch(
        self,
        *,
        batch_id: str,
        candidate_schema: str,
        target_table: str,
        ordering_key: str,
        content_hash: str,
        lower_bound: Any = None,
        upper_bound: Any = None,
    ) -> None:
        self._validate_identifiers(candidate_schema, target_table, ordering_key)
        self.ensure_registry()
        with self.connector_factory(settings=self.settings) as connection:
            connection.execute_query(
                f"""
                INSERT INTO gold.{REGISTRY_TABLE}
                    (batch_id, candidate_schema, target_table, content_hash,
                     ordering_key, lower_bound, upper_bound, committed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id) DO UPDATE SET
                    candidate_schema = EXCLUDED.candidate_schema,
                    target_table = EXCLUDED.target_table,
                    content_hash = EXCLUDED.content_hash,
                    ordering_key = EXCLUDED.ordering_key,
                    lower_bound = EXCLUDED.lower_bound,
                    upper_bound = EXCLUDED.upper_bound,
                    committed_at = EXCLUDED.committed_at
                """,
                (
                    batch_id, candidate_schema, target_table, content_hash,
                    ordering_key, _stringify(lower_bound), _stringify(upper_bound), utc_now(),
                ),
            )

    def resolve(
        self,
        *,
        batch_id: str,
        content_hash: str,
        candidate_schema: str | None = None,
        target_table: str = "fact_sales",
        ordering_key: str = "sales_order_detail_id",
        unique_key_values: Iterable[Any] = (),
    ) -> str:
        self._validate_identifiers(*(item for item in (candidate_schema, target_table, ordering_key) if item))
        self.ensure_registry()
        with self.connector_factory(settings=self.settings) as connection:
            rows = connection.fetch_results(
                f"SELECT content_hash FROM gold.{REGISTRY_TABLE} WHERE batch_id = %s",
                (batch_id,),
            )
            if rows:
                if rows[0][0] == content_hash:
                    return "SKIP"
                raise PostgresGoldReconciliationError(
                    f"batch identity has different content hash: {batch_id}"
                )

            if candidate_schema:
                values = tuple(unique_key_values)
                if values:
                    self._validate_identifiers(candidate_schema, target_table, ordering_key)
                    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in values)
                    query = sql.SQL(
                        "SELECT 1 FROM {}.{} WHERE {} IN ({}) LIMIT 1"
                    ).format(
                        sql.Identifier(candidate_schema),
                        sql.Identifier(target_table),
                        sql.Identifier(ordering_key),
                        placeholders,
                    )
                    cursor = connection.connection.cursor()
                    try:
                        cursor.execute(query, values)
                        if cursor.fetchone() is not None:
                            return "SKIP"
                    finally:
                        cursor.close()
        return "RETRY"

    def execute_batch_with_retry(
        self,
        operation,
        *,
        batch_id: str,
        content_hash: str,
        candidate_schema: str,
        target_table: str = "fact_sales",
        ordering_key: str = "sales_order_detail_id",
        lower_bound: Any = None,
        upper_bound: Any = None,
        gold_load_id: str,
        policy,
        sleeper,
    ) -> tuple[Any, int, str]:
        """Commit one batch once, record identity, and reconcile before retry."""

        def guarded_operation():
            try:
                result = operation()
            except (TimeoutError, ConnectionError) as error:
                decision = self.resolve(
                    batch_id=batch_id,
                    content_hash=content_hash,
                    candidate_schema=candidate_schema,
                    target_table=target_table,
                    ordering_key=ordering_key,
                    unique_key_values=(),
                )
                if decision == "SKIP":
                    return {"reconciled": True, "batch_id": batch_id}
                raise error
            self.record_committed_batch(
                batch_id=batch_id,
                candidate_schema=candidate_schema,
                target_table=target_table,
                ordering_key=ordering_key,
                content_hash=content_hash,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
            )
            return result

        result, attempts, status = execute_with_retry(
            guarded_operation, policy, sleeper
        )
        return {
            "result": result,
            "attempt_count": attempts,
            "status": status.value,
            "batch_id": batch_id,
            "gold_load_id": gold_load_id,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "content_hash": content_hash,
        }

    @staticmethod
    def _validate_identifiers(*identifiers: str) -> None:
        for identifier in identifiers:
            if not _IDENTIFIER.fullmatch(identifier):
                raise PostgresGoldReconciliationError(
                    f"unsafe reconciliation identifier: {identifier!r}"
                )


def _stringify(value):
    return None if value is None else str(value)