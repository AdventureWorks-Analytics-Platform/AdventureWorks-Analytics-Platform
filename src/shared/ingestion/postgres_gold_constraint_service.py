"""PostgreSQL adapter for Gold candidate DDL and constraint verification."""

from __future__ import annotations

import re
import time
from typing import Any, Mapping

import pandas as pd
from psycopg2 import sql

from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.ingestion_models import utc_now
from src.shared.ingestion.postgres_gold_reconciliation_service import (
    PostgresGoldReconciliationService,
)
from src.shared.ingestion.retry_policy import RetryPolicy


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DISCOUNT_ROUNDING_TOLERANCE = 1e-4


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

    def validate_candidate(
        self,
        candidate: Mapping[str, Any],
        specs: tuple[Any, ...],
        *,
        silver_kpi_baseline: Mapping[str, float] | None = None,
        kpi_tolerance: float = 0.02,
        require_silver_baseline: bool = False,
    ) -> dict[str, Any]:
        """Validate a persisted candidate and calculate its KPIs in PostgreSQL."""
        if kpi_tolerance < 0:
            raise ValueError("kpi_tolerance cannot be negative")
        schema = str(candidate.get("candidate_schema", ""))
        self._validate_identifier(schema)
        fact_table = sql.Identifier(schema, "fact_sales")
        issues: list[str] = []
        orphan_counts: dict[str, int] = {}
        required_non_null = (
            "sales_order_id",
            "order_date_id",
            "customer_id",
            "product_id",
            "territory_id",
            "order_qty",
            "unit_price",
            "discount_amount",
            "line_total",
            "net_sales",
        )
        try:
            with self.connector_factory(settings=self.settings) as connection:
                cursor = connection.connection.cursor()
                try:
                    for column in required_non_null:
                        cursor.execute(
                            sql.SQL("SELECT COUNT(*) FROM {} WHERE {} IS NULL").format(
                                fact_table, sql.Identifier(column)
                            )
                        )
                        null_count = int(cursor.fetchone()[0])
                        if null_count:
                            issues.append(
                                f"fact_sales.{column} contains {null_count} required NULL values"
                            )
                    fact_spec = next(
                        spec for spec in specs if spec.target_table == "fact_sales"
                    )
                    for column, (dimension, key) in fact_spec.foreign_keys.items():
                        cursor.execute(
                            sql.SQL(
                                "SELECT COUNT(*) FROM {} f WHERE f.{} IS NOT NULL "
                                "AND NOT EXISTS (SELECT 1 FROM {} d WHERE d.{} = f.{})"
                            ).format(
                                fact_table,
                                sql.Identifier(column),
                                sql.Identifier(schema, dimension),
                                sql.Identifier(key),
                                sql.Identifier(column),
                            )
                        )
                        count = int(cursor.fetchone()[0])
                        if count:
                            orphan_counts[column] = count
                            issues.append(
                                f"orphan references in fact_sales.{column}: {count}"
                            )
                    cursor.execute(
                        sql.SQL(
                            "SELECT COUNT(*) FROM {} WHERE "
                            "order_qty < 0 OR unit_price < -%s OR discount_amount < -%s "
                            "OR line_total < -%s OR net_sales < -%s "
                            "OR ABS(ROUND(order_qty * unit_price - line_total, 4) "
                            "- discount_amount) > %s "
                            "OR ABS(net_sales - line_total) > %s"
                        ).format(fact_table),
                        (
                            1e-9,
                            1e-9,
                            1e-9,
                            1e-9,
                            DISCOUNT_ROUNDING_TOLERANCE,
                            1e-9,
                        ),
                    )
                    measure_errors = int(cursor.fetchone()[0])
                    if measure_errors:
                        issues.append(
                            "fact_sales contains invalid measures or violates approved formulas"
                        )
                    cursor.execute(
                        sql.SQL(
                            "SELECT COALESCE(SUM(net_sales), 0), "
                            "COUNT(DISTINCT sales_order_id), COUNT(*), "
                            "COALESCE(SUM(order_qty), 0), "
                            "COALESCE(SUM(unit_price * order_qty), 0), "
                            "COALESCE(SUM(discount_amount), 0), "
                            "COUNT(DISTINCT customer_id) FROM {}"
                        ).format(fact_table)
                    )
                    (
                        revenue,
                        orders,
                        line_items,
                        units,
                        gross_sales,
                        discount_amount,
                        customers,
                    ) = cursor.fetchone()
                    if silver_kpi_baseline is None and require_silver_baseline:
                        source_snapshot_id = str(
                            candidate.get("source_snapshot_id", "")
                        )
                        cursor.execute(
                            "SELECT candidate_schema FROM silver.silver_current_pointer "
                            "WHERE pointer_id = 1 AND source_snapshot_id = %s",
                            (source_snapshot_id,),
                        )
                        silver_row = cursor.fetchone()
                        if silver_row is None:
                            raise PostgresGoldConstraintError(
                                "published Silver KPI baseline does not match "
                                f"source snapshot {source_snapshot_id!r}"
                            )
                        silver_schema = str(silver_row[0])
                        self._validate_identifier(silver_schema)
                        cursor.execute(
                            sql.SQL(
                                "SELECT COALESCE(SUM(d.line_total), 0), "
                                "COUNT(DISTINCT d.sales_order_id), COUNT(*), "
                                "COALESCE(SUM(d.order_qty), 0), "
                                "COALESCE(SUM(d.unit_price * d.order_qty), 0), "
                                "COALESCE(SUM(d.order_qty * d.unit_price - d.line_total), 0), "
                                "COUNT(DISTINCT h.customer_id) "
                                "FROM {} d JOIN {} h "
                                "ON d.sales_order_id = h.sales_order_id "
                                "WHERE d.source_snapshot_id = %s "
                                "AND h.source_snapshot_id = %s"
                            ).format(
                                sql.Identifier(
                                    silver_schema, "sales_order_detail_clean"
                                ),
                                sql.Identifier(
                                    silver_schema, "sales_order_header_clean"
                                ),
                            ),
                            (source_snapshot_id, source_snapshot_id),
                        )
                        (
                            base_revenue,
                            base_orders,
                            base_line_items,
                            base_units,
                            base_gross,
                            base_discount,
                            base_customers,
                        ) = cursor.fetchone()
                        base_revenue = float(base_revenue)
                        base_orders = float(base_orders)
                        base_units = float(base_units)
                        base_gross = float(base_gross)
                        base_discount = float(base_discount)
                        silver_kpi_baseline = {
                            "total_revenue": base_revenue,
                            "total_orders": base_orders,
                            "total_line_items": float(base_line_items),
                            "total_units": base_units,
                            "average_order_value": (
                                base_revenue / base_orders if base_orders else 0.0
                            ),
                            "average_item_price": (
                                base_gross / base_units if base_units else 0.0
                            ),
                            "discount_amount": base_discount,
                            "discount_rate": (
                                base_discount / base_gross if base_gross else 0.0
                            ),
                            "customer_count": float(base_customers),
                        }
                finally:
                    cursor.close()
        except Exception as exc:
            raise PostgresGoldConstraintError(
                f"Gold SQL validation failed: {type(exc).__name__}: {exc}"
            ) from exc

        total_revenue = float(revenue)
        total_orders = float(orders)
        total_units = float(units)
        gross = float(gross_sales)
        discount = float(discount_amount)
        actual = {
            "total_revenue": total_revenue,
            "total_orders": total_orders,
            "total_line_items": float(line_items),
            "total_units": total_units,
            "average_order_value": (
                total_revenue / total_orders if total_orders else 0.0
            ),
            "average_item_price": gross / total_units if total_units else 0.0,
            "discount_amount": discount,
            "discount_rate": discount / gross if gross else 0.0,
            "customer_count": float(customers),
        }
        comparisons: dict[str, Any] = {}
        for name, baseline in (silver_kpi_baseline or {}).items():
            expected = float(baseline)
            actual_value = actual.get(name, 0.0)
            variance = (
                (0.0 if actual_value == 0.0 else 1.0)
                if expected == 0.0
                else abs(actual_value - expected) / abs(expected)
            )
            passed = variance <= kpi_tolerance
            comparisons[name] = {
                "candidate": actual_value,
                "baseline": expected,
                "variance_pct": variance * 100,
                "within_tolerance": passed,
            }
            if not passed:
                issues.append(f"KPI mismatch for {name}: variance={variance:.6f}")
        kpi_issues = [issue for issue in issues if issue.startswith("KPI mismatch")]
        return {
            "validation_passed": not issues,
            "issues": issues,
            "duplicate_counts": {"fact_sales": 0},
            "orphan_counts": orphan_counts,
            "kpi_passed": not kpi_issues,
            "kpi_report": {
                "kpi_passed": not kpi_issues,
                "actual": actual,
                "baseline_source_snapshot_id": (
                    candidate.get("source_snapshot_id")
                    if require_silver_baseline or silver_kpi_baseline is not None
                    else None
                ),
                "comparisons": comparisons,
                "issues": kpi_issues,
            },
        }

    def _create_and_verify_connection(self, connection, candidate, specs, *, load_data):
        schema = str(candidate.get("candidate_schema", ""))
        tables = candidate.get("tables", {})
        frames = candidate.get("frames", {})
        cursor = connection.connection.cursor()
        try:
            self._execute(
                cursor,
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema)
                ),
            )
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
                    (
                        None
                        if _is_missing(value)
                        else value.item() if hasattr(value, "item") else value
                    )
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
            raise PostgresGoldConstraintError(
                f"unsafe Gold schema identifier: {identifier!r}"
            )


class PostgresGoldCandidateService:
    """Allocate and build a real versioned Gold candidate without publishing it."""

    _ALLOCATOR_LOCK_KEY = "adventureworks_gold_candidate_allocator"

    def __init__(self, settings: Settings | None = None, connector_factory=None):
        self.settings = settings or get_settings()
        self.connector_factory = connector_factory or PostgreSQLConnector
        self.constraint_service = PostgresGoldConstraintService(
            settings=self.settings, connector_factory=self.connector_factory
        )
        self.reconciliation_service = PostgresGoldReconciliationService(
            settings=self.settings, connector_factory=self.connector_factory
        )

    def prepare_streaming_candidate(self, frames, identity, specs) -> dict[str, Any]:
        """Create dimensions and an empty fact target before reading fact batches."""
        fact_spec = next(spec for spec in specs if spec.target_table == "fact_sales")
        complete_frames = dict(frames)
        if "fact_sales" in complete_frames:
            raise PostgresGoldConstraintError(
                "streaming candidate must not materialize fact_sales in memory"
            )
        self.reconciliation_service.ensure_registry()
        complete_frames["fact_sales"] = pd.DataFrame(columns=fact_spec.required_columns)
        connector = self.connector_factory(settings=self.settings)
        try:
            with connector as connection:
                cursor = connection.connection.cursor()
                try:
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))",
                        (self._ALLOCATOR_LOCK_KEY,),
                    )
                    gold_version = f"v{self._next_version(cursor):03d}"
                    candidate = self._candidate_metadata(
                        complete_frames, identity, specs, gold_version
                    )
                    schema = candidate["candidate_schema"]
                    self.constraint_service._execute(
                        cursor,
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            sql.Identifier(schema)
                        ),
                    )
                    create_statements, primary_key_statements, _ = (
                        self.constraint_service._constraint_statements(
                            schema, candidate["tables"], specs
                        )
                    )
                    for statement in create_statements:
                        self.constraint_service._execute(cursor, statement)
                    for spec in specs:
                        if spec.target_table != "fact_sales":
                            self.constraint_service._insert_frame(
                                cursor,
                                schema,
                                spec.target_table,
                                candidate["frames"][spec.target_table],
                            )
                    for statement in primary_key_statements:
                        self.constraint_service._execute(cursor, statement)
                    connection.connection.commit()
                except Exception:
                    connection.connection.rollback()
                    raise
                finally:
                    cursor.close()
            candidate["constraint_report"] = {
                "constraints_verified": False,
                "candidate_schema": schema,
                "pending_foreign_keys": True,
            }
            candidate["lifecycle"] = "CANDIDATE"
            candidate["published"] = False
            return candidate
        except PostgresGoldConstraintError:
            raise
        except Exception as exc:
            raise PostgresGoldConstraintError(
                f"Streaming Gold candidate preparation failed: {type(exc).__name__}: {exc}"
            ) from exc

    def write_fact_batch(self, candidate, batch, identity) -> dict[str, Any]:
        schema = str(candidate.get("candidate_schema", ""))
        table = "fact_sales"
        self.constraint_service._validate_identifier(schema)
        frame = batch.dataframe.copy()
        frame["created_at"] = utc_now()
        frame["gold_version"] = candidate["gold_version"]
        frame["source_snapshot_id"] = identity.source_snapshot_id
        keys = tuple(frame["sales_order_detail_id"].tolist())
        registry_batch_id = f"{schema}:{batch.batch_id}"

        def operation():
            with self.connector_factory(settings=self.settings) as connection:
                cursor = connection.connection.cursor()
                try:
                    self.constraint_service._insert_frame(cursor, schema, table, frame)
                    cursor.execute(
                        """
                        INSERT INTO gold.gold_batch_registry
                            (batch_id, candidate_schema, target_table, content_hash,
                             ordering_key, lower_bound, upper_bound, committed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (batch_id) DO NOTHING
                        """,
                        (
                            registry_batch_id,
                            schema,
                            table,
                            batch.content_hash,
                            "sales_order_detail_id",
                            (
                                None
                                if batch.lower_bound is None
                                else str(batch.lower_bound)
                            ),
                            (
                                None
                                if batch.upper_bound is None
                                else str(batch.upper_bound)
                            ),
                        ),
                    )
                    cursor.execute(
                        "SELECT content_hash, candidate_schema, target_table "
                        "FROM gold.gold_batch_registry WHERE batch_id = %s",
                        (registry_batch_id,),
                    )
                    registry_row = cursor.fetchone()
                    if registry_row != (batch.content_hash, schema, table):
                        raise PostgresGoldConstraintError(
                            "batch identity conflicts with existing registry entry: "
                            f"{registry_batch_id}"
                        )
                    connection.connection.commit()
                except Exception:
                    connection.connection.rollback()
                    raise
                finally:
                    cursor.close()
            return {"rows_written": len(frame)}

        outcome = self.reconciliation_service.execute_batch_with_retry(
            operation,
            batch_id=registry_batch_id,
            content_hash=batch.content_hash,
            candidate_schema=schema,
            target_table=table,
            ordering_key="sales_order_detail_id",
            unique_key_values=keys,
            lower_bound=batch.lower_bound,
            upper_bound=batch.upper_bound,
            gold_load_id=identity.gold_load_id,
            policy=RetryPolicy(
                max_attempts=self.settings.retry_max_attempts,
                initial_delay_seconds=self.settings.retry_initial_delay_seconds,
                max_delay_seconds=self.settings.retry_max_delay_seconds,
            ),
            sleeper=time.sleep,
        )
        if not outcome["result"].get("reconciled"):
            candidate["tables"][table]["rows"] += len(frame)
        return outcome

    def finalize_streaming_candidate(self, candidate, specs) -> dict[str, Any]:
        schema = str(candidate.get("candidate_schema", ""))
        self.constraint_service._validate_identifier(schema)
        connector = self.connector_factory(settings=self.settings)
        try:
            with connector as connection:
                cursor = connection.connection.cursor()
                try:
                    _, _, foreign_key_statements = (
                        self.constraint_service._constraint_statements(
                            schema, candidate["tables"], specs
                        )
                    )
                    for statement in foreign_key_statements:
                        self.constraint_service._execute(cursor, statement)
                    report = self.constraint_service._inspect_catalog(
                        cursor, schema, specs
                    )
                    if not report["constraints_verified"]:
                        raise PostgresGoldConstraintError("; ".join(report["issues"]))
                    connection.connection.commit()
                except Exception:
                    connection.connection.rollback()
                    raise
                finally:
                    cursor.close()
            candidate["constraint_report"] = report
            return report
        except PostgresGoldConstraintError:
            raise
        except Exception as exc:
            raise PostgresGoldConstraintError(
                f"Streaming Gold constraint finalization failed: {type(exc).__name__}: {exc}"
            ) from exc

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
                "required_metadata": (
                    "created_at",
                    "gold_version",
                    "source_snapshot_id",
                ),
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
