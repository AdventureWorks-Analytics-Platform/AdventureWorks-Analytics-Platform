"""PostgreSQL adapter for Gold candidate DDL and constraint verification."""

from __future__ import annotations

import re
from typing import Any, Mapping

import pandas as pd
from psycopg2 import sql

from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.ingestion_models import utc_now


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostgresGoldConstraintError(RuntimeError):
    """Raised when Gold candidate DDL or catalog verification fails."""


class PostgresGoldConstraintService:
    """Create and verify a complete Gold candidate in one PostgreSQL transaction."""

    def __init__(self, settings: Settings | None = None, connector_factory=None):
        self.settings = settings or get_settings()
        self.connector_factory = connector_factory or PostgreSQLConnector

    def create_and_verify(
        self,
        candidate: Mapping[str, Any],
        specs: tuple[Any, ...],
        *,
        load_data: bool = True,
    ) -> dict[str, Any]:
        schema = str(candidate.get("candidate_schema", ""))
        self._validate_identifier(schema)
        tables = candidate.get("tables", {})
        frames = candidate.get("frames", {})
        if not tables:
            raise PostgresGoldConstraintError("candidate contains no Gold tables")

        connector = self.connector_factory(settings=self.settings)
        try:
            with connector as connection:
                return self._create_and_verify_connection(
                    connection, candidate, specs, load_data=load_data
                )
        except PostgresGoldConstraintError:
            raise
        except Exception as exc:
            raise PostgresGoldConstraintError(
                f"Gold candidate DDL transaction failed: {type(exc).__name__}: {exc}"
            ) from exc

    def _create_and_verify_connection(self, connection, candidate, specs, *, load_data):
        schema = str(candidate.get("candidate_schema", ""))
        tables = candidate.get("tables", {})
        frames = candidate.get("frames", {})
        cursor = connection.connection.cursor()
        try:
            self._execute(cursor, sql.SQL("CREATE SCHEMA IF NOT EXISTS {}" ).format(
                sql.Identifier(schema)
            ))
            create_statements, primary_key_statements, foreign_key_statements = (
                self._constraint_statements(schema, tables, specs)
            )
            for statement in create_statements:
                self._execute(cursor, statement)
            if load_data:
                for spec in specs:
                    self._insert_frame(
                        cursor, schema, spec.target_table, frames.get(spec.target_table)
                    )
            for statement in primary_key_statements:
                self._execute(cursor, statement)
            for statement in foreign_key_statements:
                self._execute(cursor, statement)
            report = self._inspect_catalog(cursor, schema, specs)
            if not report["constraints_verified"]:
                raise PostgresGoldConstraintError("; ".join(report["issues"]))
            connection.connection.commit()
            return report
        except Exception:
            connection.connection.rollback()
            raise
        finally:
            cursor.close()

    def _constraint_statements(self, schema, tables, specs):
        create_statements = []
        primary_key_statements = []
        foreign_key_statements = []
        for spec in specs:
            table_metadata = tables.get(spec.target_table)
            if table_metadata is None:
                raise PostgresGoldConstraintError(
                    f"candidate metadata missing table: {spec.target_table}"
                )
            ddl = table_metadata.get("ddl", ())
            if not ddl:
                raise PostgresGoldConstraintError(
                    f"candidate metadata missing DDL: {spec.target_table}"
                )
            create_statements.append(self._raw_sql(ddl[0]))
            primary_key_statements.append(self._raw_sql(ddl[1]))
            foreign_key_statements.extend(self._raw_sql(item) for item in ddl[2:])
        return create_statements, primary_key_statements, foreign_key_statements

    @staticmethod
    def _insert_frame(cursor, schema, table, frame) -> None:
        if frame is None or frame.empty:
            return
        columns = list(frame.columns)
        statement = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        rows = []
        for row in frame.itertuples(index=False, name=None):
            rows.append(
                tuple(
                    None
                    if _is_missing(value)
                    else value.item()
                    if hasattr(value, "item")
                    else value
                    for value in row
                )
            )
        cursor.executemany(statement, rows)

    def _inspect_catalog(self, cursor, schema, specs) -> dict[str, Any]:
        type_rows = self._fetchall(
            cursor,
            """
            SELECT table_name, column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position
            """,
            (schema,),
        )
        constraint_rows = self._fetchall(
            cursor,
            """
            SELECT tc.table_name, tc.constraint_name, tc.constraint_type
            FROM information_schema.table_constraints tc
            WHERE tc.constraint_schema = %s
              AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
            ORDER BY tc.table_name, tc.constraint_name
            """,
            (schema,),
        )
        expected_tables = {spec.target_table for spec in specs}
        actual_tables = {row[0] for row in type_rows}
        issues = []
        missing_tables = expected_tables - actual_tables
        if missing_tables:
            issues.append(f"missing candidate tables: {sorted(missing_tables)}")
        constraints = {(row[0], row[2]) for row in constraint_rows}
        for spec in specs:
            if (spec.target_table, "PRIMARY KEY") not in constraints:
                issues.append(f"missing primary key: {spec.target_table}")
            if spec.foreign_keys and not any(
                table == spec.target_table and kind == "FOREIGN KEY"
                for table, kind in constraints
            ):
                issues.append(f"missing foreign keys: {spec.target_table}")
        return {
            "constraints_verified": not issues,
            "issues": issues,
            "schema": schema,
            "tables": sorted(actual_tables),
            "column_metadata": type_rows,
            "constraint_metadata": constraint_rows,
            "verified_at": utc_now().isoformat(),
        }

    @staticmethod
    def _execute(cursor, statement):
        cursor.execute(statement)

    @staticmethod
    def _fetchall(cursor, query, params):
        cursor.execute(query, params)
        return cursor.fetchall()

    @staticmethod
    def _raw_sql(statement):
        if not isinstance(statement, str):
            raise PostgresGoldConstraintError("candidate DDL must be SQL text")
        return sql.SQL(statement)

    @staticmethod
    def _validate_identifier(identifier):
        if not _IDENTIFIER.fullmatch(identifier):
            raise PostgresGoldConstraintError(f"unsafe Gold schema identifier: {identifier!r}")


class PostgresGoldCandidateService:
    """Allocate and build a real versioned Gold candidate without publishing it."""

    _ALLOCATOR_LOCK_KEY = "adventureworks_gold_candidate_allocator"

    def __init__(self, settings: Settings | None = None, connector_factory=None):
        self.settings = settings or get_settings()
        self.connector_factory = connector_factory or PostgreSQLConnector
        self.constraint_service = PostgresGoldConstraintService(
            settings=self.settings, connector_factory=self.connector_factory
        )

    def prepare_candidate(self, frames, identity, specs) -> dict[str, Any]:
        connector = self.connector_factory(settings=self.settings)
        try:
            with connector as connection:
                cursor = connection.connection.cursor()
                try:
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))",
                        (self._ALLOCATOR_LOCK_KEY,),
                    )
                    version_number = self._next_version(cursor)
                    gold_version = f"v{version_number:03d}"
                    candidate = self._candidate_metadata(
                        frames, identity, specs, gold_version
                    )
                    report = self.constraint_service._create_and_verify_connection(
                        connection, candidate, specs, load_data=True
                    )
                    candidate["constraint_report"] = report
                    candidate["lifecycle"] = "CANDIDATE"
                    candidate["published"] = False
                    return candidate
                finally:
                    cursor.close()
        except PostgresGoldConstraintError:
            raise
        except Exception as exc:
            raise PostgresGoldConstraintError(
                f"Gold candidate preparation failed: {type(exc).__name__}: {exc}"
            ) from exc

    @staticmethod
    def _next_version(cursor) -> int:
        cursor.execute(
            """
            SELECT COALESCE(MAX((substring(schema_name FROM '^gold_v([0-9]+)$'))::INTEGER), 0) + 1
            FROM information_schema.schemata
            WHERE schema_name ~ '^gold_v[0-9]+$'
            """
        )
        return int(cursor.fetchone()[0])

    @staticmethod
    def _candidate_metadata(frames, identity, specs, gold_version):
        from src.features.Sales_Performance.jobs.sales_gold_job import (
            GoldConstraintManager,
        )

        ddl_builder = GoldConstraintManager.ddl_for_table
        candidate_schema = f"gold_{gold_version}"
        tables = {}
        candidate_frames = {}
        for spec in specs:
            frame = frames.get(spec.target_table)
            if frame is None:
                raise PostgresGoldConstraintError(
                    f"candidate is missing table: {spec.target_table}"
                )
            candidate_frame = frame.copy()
            candidate_frame["created_at"] = utc_now()
            candidate_frame["gold_version"] = gold_version
            candidate_frame["source_snapshot_id"] = identity.source_snapshot_id
            candidate_frames[spec.target_table] = candidate_frame
            tables[spec.target_table] = {
                "columns": tuple(candidate_frame.columns),
                "required_metadata": ("created_at", "gold_version", "source_snapshot_id"),
                "primary_key": (spec.primary_key,),
                "foreign_keys": dict(spec.foreign_keys),
                "rows": len(candidate_frame),
                "ddl": ddl_builder(candidate_schema, spec),
            }
        return {
            "candidate_schema": candidate_schema,
            "gold_version": gold_version,
            "source_snapshot_id": identity.source_snapshot_id,
            "staging_identity": identity.gold_load_id,
            "tables": tables,
            "frames": candidate_frames,
            "gold_run_id": identity.gold_run_id,
        }


def _is_missing(value):
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False