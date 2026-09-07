"""Atomic PostgreSQL publication of versioned Gold candidate schemas."""

from __future__ import annotations

import re
import json
from typing import Any, Mapping

from psycopg2 import sql

from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_gold_constraint_service import (
    PostgresGoldConstraintError,
)
from src.shared.ingestion.ingestion_models import utc_now


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
POINTER_TABLE = "gold_current_pointer"
GOLD_POINTER_LOCK_KEY = "adventureworks_gold_current_pointer"


class PostgresGoldPublicationError(RuntimeError):
    """Raised when a versioned Gold candidate cannot be promoted."""


class PostgresGoldPublishService:
    """Atomically update the Gold current pointer without replacing Gold tables."""

    required_tables = frozenset({
        "dim_date", "dim_customer", "dim_product", "dim_territory",
        "dim_salesperson", "fact_sales",
    })

    def __init__(self, settings: Settings | None = None, connector_factory=None):
        self.settings = settings or get_settings()
        self.connector_factory = connector_factory or PostgreSQLConnector

    def publish(
        self,
        candidate: Mapping[str, Any],
        *,
        gold_run_id: str,
        source_snapshot_id: str,
        validation_report: Mapping[str, Any] | None = None,
        constraint_report: Mapping[str, Any] | None = None,
        kpi_report: Mapping[str, Any] | None = None,
        counts: Mapping[str, int] | None = None,
    ) -> dict[str, Any]:
        candidate_schema = str(candidate.get("candidate_schema", ""))
        gold_version = str(candidate.get("gold_version", ""))
        self._validate_identifier(candidate_schema)
        self._validate_version(gold_version)
        if candidate.get("source_snapshot_id") != source_snapshot_id:
            raise PostgresGoldPublicationError("candidate source snapshot does not match publish request")

        connector = self.connector_factory(settings=self.settings)
        try:
            with connector as connection:
                cursor = connection.connection.cursor()
                try:
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))",
                        (GOLD_POINTER_LOCK_KEY,),
                    )
                    self._ensure_pointer_table(cursor)
                    self._ensure_audit_table(cursor)
                    self._assert_candidate(cursor, candidate_schema)
                    previous = self._current_pointer(cursor)
                    self._write_pointer(
                        cursor,
                        gold_version=gold_version,
                        candidate_schema=candidate_schema,
                        source_snapshot_id=source_snapshot_id,
                        gold_run_id=gold_run_id,
                        previous_version=previous["gold_version"] if previous else None,
                    )
                    audit_id = self._write_publication_audit(
                        cursor,
                        candidate=candidate,
                        gold_run_id=gold_run_id,
                        gold_version=gold_version,
                        candidate_schema=candidate_schema,
                        source_snapshot_id=source_snapshot_id,
                        previous_version=previous["gold_version"] if previous else None,
                        validation_report=validation_report,
                        constraint_report=constraint_report,
                        kpi_report=kpi_report,
                        counts=counts,
                    )
                    connection.connection.commit()
                    current_pointer = {
                        "gold_version": gold_version,
                        "candidate_schema": candidate_schema,
                        "source_snapshot_id": source_snapshot_id,
                        "gold_run_id": gold_run_id,
                        "previous_version": previous["gold_version"] if previous else None,
                    }
                    return {
                        **current_pointer,
                        "current_pointer": current_pointer,
                        "published": True,
                        "audit_id": audit_id,
                    }
                except Exception:
                    connection.connection.rollback()
                    raise
                finally:
                    cursor.close()
        except PostgresGoldPublicationError:
            raise
        except Exception as exc:
            raise PostgresGoldPublicationError(
                f"Gold publish transaction failed: {type(exc).__name__}: {exc}"
            ) from exc

    @staticmethod
    def _ensure_pointer_table(cursor) -> None:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS gold.{POINTER_TABLE} (
                pointer_id SMALLINT PRIMARY KEY CHECK (pointer_id = 1),
                gold_version VARCHAR(32) NOT NULL,
                candidate_schema VARCHAR(63) NOT NULL,
                source_snapshot_id VARCHAR(128) NOT NULL,
                gold_run_id VARCHAR(128) NOT NULL,
                previous_version VARCHAR(32),
                published_at TIMESTAMPTZ NOT NULL
            )
            """
        )

    def _assert_candidate(self, cursor, candidate_schema: str) -> None:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (candidate_schema,),
        )
        actual = {row[0] for row in cursor.fetchall()}
        missing = self.required_tables - actual
        if missing:
            raise PostgresGoldPublicationError(
                f"candidate schema is incomplete: {sorted(missing)}"
            )

    @staticmethod
    def _ensure_audit_table(cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.gold_publication_audit (
                audit_id BIGSERIAL PRIMARY KEY,
                gold_run_id VARCHAR(128) NOT NULL,
                gold_version VARCHAR(32) NOT NULL,
                candidate_schema VARCHAR(63) NOT NULL,
                source_snapshot_id VARCHAR(128) NOT NULL,
                previous_version VARCHAR(32),
                status VARCHAR(32) NOT NULL,
                published BOOLEAN NOT NULL,
                validation_passed BOOLEAN NOT NULL,
                constraints_verified BOOLEAN NOT NULL,
                kpi_passed BOOLEAN NOT NULL,
                rows_read BIGINT NOT NULL DEFAULT 0,
                rows_written BIGINT NOT NULL DEFAULT 0,
                rows_rejected BIGINT NOT NULL DEFAULT 0,
                dimension_rows BIGINT NOT NULL DEFAULT 0,
                fact_rows BIGINT NOT NULL DEFAULT 0,
                validation_report JSONB NOT NULL DEFAULT '{}'::jsonb,
                constraint_report JSONB NOT NULL DEFAULT '{}'::jsonb,
                kpi_report JSONB NOT NULL DEFAULT '{}'::jsonb,
                published_at TIMESTAMPTZ NOT NULL
            )
            """
        )

    @staticmethod
    def _write_publication_audit(
        cursor,
        *,
        candidate,
        gold_run_id,
        gold_version,
        candidate_schema,
        source_snapshot_id,
        previous_version,
        validation_report,
        constraint_report,
        kpi_report,
        counts,
    ):
        counts = dict(counts or {})
        tables = candidate.get("tables", {})
        fact_rows = int(counts.get("fact_rows", tables.get("fact_sales", {}).get("rows", 0)))
        dimension_rows = int(counts.get(
            "dimension_rows",
            sum(meta.get("rows", 0) for name, meta in tables.items() if name != "fact_sales"),
        ))
        rows_written = int(counts.get("rows_written", dimension_rows + fact_rows))
        rows_read = int(counts.get("rows_read", rows_written))
        rows_rejected = int(counts.get("rows_rejected", 0))
        validation_report = dict(validation_report or {})
        constraint_report = dict(constraint_report or {})
        kpi_report = dict(kpi_report or {})
        cursor.execute(
            """
            INSERT INTO gold.gold_publication_audit (
                gold_run_id, gold_version, candidate_schema, source_snapshot_id,
                previous_version, status, published, validation_passed,
                constraints_verified, kpi_passed, rows_read, rows_written,
                rows_rejected, dimension_rows, fact_rows, validation_report,
                constraint_report, kpi_report, published_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s::jsonb, %s::jsonb, %s::jsonb, %s
            ) RETURNING audit_id
            """,
            (
                gold_run_id, gold_version, candidate_schema, source_snapshot_id,
                previous_version, "PUBLISHED", True,
                validation_report.get("validation_passed", True),
                constraint_report.get("constraints_verified", True),
                kpi_report.get("kpi_passed", True), rows_read, rows_written,
                rows_rejected, dimension_rows, fact_rows,
                json.dumps(validation_report, default=str),
                json.dumps(constraint_report, default=str),
                json.dumps(kpi_report, default=str), utc_now(),
            ),
        )
        return cursor.fetchone()[0]

    @staticmethod
    def _current_pointer(cursor) -> dict[str, Any] | None:
        cursor.execute(
            f"SELECT gold_version, candidate_schema, source_snapshot_id, gold_run_id "
            f"FROM gold.{POINTER_TABLE} WHERE pointer_id = 1 FOR UPDATE"
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "gold_version": row[0],
            "candidate_schema": row[1],
            "source_snapshot_id": row[2],
            "gold_run_id": row[3],
        }

    @staticmethod
    def _write_pointer(
        cursor,
        *,
        gold_version: str,
        candidate_schema: str,
        source_snapshot_id: str,
        gold_run_id: str,
        previous_version: str | None,
    ) -> None:
        cursor.execute(
            f"""
            INSERT INTO gold.{POINTER_TABLE}
                (pointer_id, gold_version, candidate_schema, source_snapshot_id,
                 gold_run_id, previous_version, published_at)
            VALUES (1, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pointer_id) DO UPDATE SET
                gold_version = EXCLUDED.gold_version,
                candidate_schema = EXCLUDED.candidate_schema,
                source_snapshot_id = EXCLUDED.source_snapshot_id,
                gold_run_id = EXCLUDED.gold_run_id,
                previous_version = EXCLUDED.previous_version,
                published_at = EXCLUDED.published_at
            """,
            (
                gold_version, candidate_schema, source_snapshot_id,
                gold_run_id, previous_version, utc_now(),
            ),
        )

    @staticmethod
    def _validate_identifier(identifier: str) -> None:
        if not _IDENTIFIER.fullmatch(identifier):
            raise PostgresGoldPublicationError(f"unsafe candidate schema: {identifier!r}")

    @staticmethod
    def _validate_version(version: str) -> None:
        if not re.fullmatch(r"v[0-9]{3,}", version):
            raise PostgresGoldPublicationError(f"unsafe Gold version: {version!r}")