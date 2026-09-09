from uuid import uuid4

import pandas as pd
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector


class PostgresPublishService:
    """Atomically promote a validated staging table into published Bronze."""

    staging_schema = "bronze_staging"
    published_schema = "bronze"

    def __init__(
        self,
        settings: Settings | None = None,
        staging_schema: str | None = None,
        published_schema: str | None = None,
    ):
        self.settings = settings or get_settings()
        self.staging_schema = staging_schema or type(self).staging_schema
        self.published_schema = published_schema or type(self).published_schema

    def publish(
        self,
        target_table: str,
        staging_table: str,
        validation_report: dict | None = None,
    ) -> str:
        if validation_report is None or not validation_report.get("validation_passed"):
            raise ValueError("staging validation must pass before PostgreSQL publish")
        self._validate_identifier(target_table)
        self._validate_identifier(staging_table)

        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    "SELECT to_regclass(%s)",
                    (f"{self.staging_schema}.{staging_table}",),
                )
                if cursor.fetchone()[0] is None:
                    raise ValueError(f"staging table does not exist: {staging_table}")

                cursor.execute(
                    "SELECT to_regclass(%s)",
                    (f"{self.published_schema}.{target_table}",),
                )
                if cursor.fetchone()[0] is not None:
                    backup_table = f"{target_table}__previous__{uuid4().hex}"
                    self._validate_identifier(backup_table)
                    cursor.execute(
                        sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                            sql.Identifier(self.published_schema),
                            sql.Identifier(target_table),
                            sql.Identifier(backup_table),
                        )
                    )

                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} SET SCHEMA {}").format(
                        sql.Identifier(self.staging_schema),
                        sql.Identifier(staging_table),
                        sql.Identifier(self.published_schema),
                    )
                )
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(self.published_schema),
                        sql.Identifier(staging_table),
                        sql.Identifier(target_table),
                    )
                )
                connection.connection.commit()
            except Exception:
                connection.connection.rollback()
                raise
            finally:
                cursor.close()
        return f"{self.published_schema}.{target_table}"

    @staticmethod
    def _validate_identifier(identifier: str) -> None:
        if not identifier or not identifier.replace("_", "a").isalnum():
            raise ValueError(f"Invalid publish identifier: {identifier!r}")
        if not (identifier[0].isalpha() or identifier[0] == "_"):
            raise ValueError(f"Invalid publish identifier: {identifier!r}")


class PostgresSilverPublishService(PostgresPublishService):
    """Build Silver in a candidate schema and publish one current pointer."""

    staging_schema = "silver_staging"
    published_schema = "silver"
    pointer_table = "silver_current_pointer"

    def begin_snapshot(self, source_snapshot_id: str) -> str:
        candidate_schema = f"silver_v{source_snapshot_id.replace('-', '')}"
        self._validate_identifier(candidate_schema)
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                    .format(sql.Identifier(candidate_schema))
                )
                connection.connection.commit()
            except Exception:
                connection.connection.rollback()
                raise
            finally:
                cursor.close()
        return candidate_schema

    def finalize_snapshot(self, candidate_schema: str, source_snapshot_id: str) -> str:
        self._validate_identifier(candidate_schema)
        required = {
            "sales_order_header_clean", "sales_order_detail_clean", "customer_clean",
            "sales_territory_clean", "product_clean", "sales_person_clean",
        }
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                    (candidate_schema,),
                )
                missing = required - {row[0] for row in cursor.fetchall()}
                if missing:
                    raise ValueError(f"Silver candidate is incomplete: {sorted(missing)}")
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS silver.silver_current_pointer ("
                    "pointer_id SMALLINT PRIMARY KEY CHECK (pointer_id = 1), "
                    "candidate_schema VARCHAR(63) NOT NULL, "
                    "source_snapshot_id VARCHAR(128) NOT NULL, "
                    "published_at TIMESTAMPTZ NOT NULL)"
                )
                cursor.execute(
                    "INSERT INTO silver.silver_current_pointer "
                    "(pointer_id, candidate_schema, source_snapshot_id, published_at) "
                    "VALUES (1, %s, %s, CURRENT_TIMESTAMP) "
                    "ON CONFLICT (pointer_id) DO UPDATE SET "
                    "candidate_schema = EXCLUDED.candidate_schema, "
                    "source_snapshot_id = EXCLUDED.source_snapshot_id, "
                    "published_at = EXCLUDED.published_at",
                    (candidate_schema, source_snapshot_id),
                )
                connection.connection.commit()
            except Exception:
                connection.connection.rollback()
                raise
            finally:
                cursor.close()
        return f"silver.{self.pointer_table}"

    def cleanup_snapshot(self, candidate_schema: str) -> None:
        self._validate_identifier(candidate_schema)
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE")
                    .format(sql.Identifier(candidate_schema))
                )
                connection.connection.commit()
            except Exception:
                connection.connection.rollback()
                raise
            finally:
                cursor.close()

    def publish(
        self, target_table: str, staging_table: str,
        validation_report: dict | None = None, *, candidate_schema: str | None = None,
    ) -> str:
        if candidate_schema is None:
            return super().publish(target_table, staging_table, validation_report)
        if validation_report is None or not validation_report.get("validation_passed"):
            raise ValueError("staging validation must pass before PostgreSQL publish")
        self._validate_identifier(target_table)
        self._validate_identifier(staging_table)
        self._validate_identifier(candidate_schema)
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    "SELECT to_regclass(%s)",
                    (f"{self.staging_schema}.{staging_table}",),
                )
                if cursor.fetchone()[0] is None:
                    raise ValueError(f"staging table does not exist: {staging_table}")
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} SET SCHEMA {}")
                    .format(sql.Identifier(self.staging_schema), sql.Identifier(staging_table),
                            sql.Identifier(candidate_schema))
                )
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME TO {}")
                    .format(sql.Identifier(candidate_schema), sql.Identifier(staging_table),
                            sql.Identifier(target_table))
                )
                connection.connection.commit()
            except Exception:
                connection.connection.rollback()
                raise
            finally:
                cursor.close()
        return f"{candidate_schema}.{target_table}"


def build_silver_sql_dedup_kwargs(settings: Settings | None = None) -> dict:
    """`SalesSilverJob(**kwargs)` extras enabling the SQL-side dedup/validate path.

    Returns `{}` unless `settings.silver_sql_dedup_enabled` is `True`, so the
    pandas-based default remains production behavior until explicitly opted in
    (see phase4c_silverlayer_execution.md 6.5.9/6.5.10).
    """
    resolved_settings = settings or get_settings()
    if not getattr(resolved_settings, "silver_sql_dedup_enabled", False):
        return {}
    return {
        "dedup_service": PostgresSilverDedupService(resolved_settings),
        "sql_validator": PostgresSilverStagingValidator(resolved_settings),
    }


class PostgresSilverStagingWriter:
    """Persist Silver staging frames before the atomic publish step."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self._initialized_tables: set[str] = set()

    def __call__(self, frame: pd.DataFrame, staging_table: str) -> None:
        PostgresPublishService._validate_identifier(staging_table)
        if_exists = "append" if staging_table in self._initialized_tables else "replace"
        self._write(frame, staging_table, if_exists)
        self._initialized_tables.add(staging_table)

    def replace_all(self, frame: pd.DataFrame, staging_table: str) -> None:
        """Overwrite the full staged table content, e.g. after global dedup."""
        PostgresPublishService._validate_identifier(staging_table)
        self._write(frame, staging_table, "replace")
        self._initialized_tables.add(staging_table)

    def _write(self, frame: pd.DataFrame, staging_table: str, if_exists: str) -> None:
        with PostgreSQLConnector(settings=self.settings) as pg_conn:
            engine = create_engine(
                "postgresql://",
                creator=lambda: pg_conn.connection,
                poolclass=StaticPool,
            )
            try:
                frame.to_sql(
                    staging_table,
                    engine,
                    schema="silver_staging",
                    if_exists=if_exists,
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
                pg_conn.connection.commit()
            except Exception:
                pg_conn.connection.rollback()
                raise
            finally:
                engine.dispose()


class PostgresSilverDedupService:
    """Global Silver dedup executed directly on the staged Postgres table.

    Runs a SQL window-function delete against `silver_staging.<name>` so the
    duplicate rows never need to be materialized as one in-memory pandas frame.
    """

    staging_schema = "silver_staging"

    def __init__(self, settings: Settings | None = None, staging_schema: str | None = None):
        self.settings = settings or get_settings()
        self.staging_schema = staging_schema or type(self).staging_schema

    def count_duplicate_keys(self, staging_table: str, primary_key: str) -> int:
        PostgresPublishService._validate_identifier(staging_table)
        PostgresPublishService._validate_identifier(primary_key)
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    sql.SQL(
                        "SELECT count({pk}) - count(DISTINCT {pk}) FROM {schema}.{table}"
                    ).format(
                        pk=sql.Identifier(primary_key),
                        schema=sql.Identifier(self.staging_schema),
                        table=sql.Identifier(staging_table),
                    )
                )
                return int(cursor.fetchone()[0] or 0)
            finally:
                cursor.close()

    def deduplicate(self, staging_table: str, primary_key: str) -> int:
        """Delete all but the most recent row per key; returns rows removed.

        Ties are broken deterministically by `_load_date DESC, _record_hash DESC`,
        matching the historical pandas `_global_deduplicate()` ordering rule.
        """
        PostgresPublishService._validate_identifier(staging_table)
        PostgresPublishService._validate_identifier(primary_key)
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    sql.SQL(
                        "WITH ranked AS ("
                        "SELECT ctid, ROW_NUMBER() OVER ("
                        "PARTITION BY {pk} "
                        "ORDER BY {load_date} DESC NULLS LAST, {record_hash} DESC NULLS LAST"
                        ") AS rn FROM {schema}.{table}"
                        ") "
                        "DELETE FROM {schema}.{table} "
                        "WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1)"
                    ).format(
                        pk=sql.Identifier(primary_key),
                        load_date=sql.Identifier("_load_date"),
                        record_hash=sql.Identifier("_record_hash"),
                        schema=sql.Identifier(self.staging_schema),
                        table=sql.Identifier(staging_table),
                    )
                )
                rows_removed = cursor.rowcount
                connection.connection.commit()
                return int(rows_removed or 0)
            except Exception:
                connection.connection.rollback()
                raise
            finally:
                cursor.close()

    def set_source_snapshot_id(self, staging_table: str, source_snapshot_id: str) -> None:
        """Stamp every staged row with the run's snapshot id via SQL, no frame needed."""
        PostgresPublishService._validate_identifier(staging_table)
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    sql.SQL(
                        "ALTER TABLE {schema}.{table} "
                        "ADD COLUMN IF NOT EXISTS source_snapshot_id text"
                    ).format(
                        schema=sql.Identifier(self.staging_schema),
                        table=sql.Identifier(staging_table),
                    )
                )
                cursor.execute(
                    sql.SQL(
                        "UPDATE {schema}.{table} SET source_snapshot_id = %s"
                    ).format(
                        schema=sql.Identifier(self.staging_schema),
                        table=sql.Identifier(staging_table),
                    ),
                    (source_snapshot_id,),
                )
                connection.connection.commit()
            except Exception:
                connection.connection.rollback()
                raise
            finally:
                cursor.close()


class PostgresSilverStagingValidator:
    """SQL-only pre-publish validation; never materializes the staged table in pandas.

    Mirrors `SilverTransformationJob._default_staging_validator()`'s checks and
    return shape, but computed entirely with COUNT-style queries against Postgres.
    """

    staging_schema = "silver_staging"
    allowed_metadata_columns = {
        "_source_system", "_source_table", "_load_date", "_record_hash",
        "run_id", "load_id", "batch_id", "source_snapshot_id",
    }

    def __init__(self, settings: Settings | None = None, staging_schema: str | None = None):
        self.settings = settings or get_settings()
        self.staging_schema = staging_schema or type(self).staging_schema

    def validate(
        self,
        staging_table: str,
        spec: object,
        source_count: int,
        rejected_count: int,
        rejected_threshold: int,
    ) -> dict:
        PostgresPublishService._validate_identifier(staging_table)
        issues: list[str] = []
        with PostgreSQLConnector(settings=self.settings) as connection:
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (self.staging_schema, staging_table),
                )
                columns = {row[0] for row in cursor.fetchall()}
                required_columns = set(spec.required_columns)
                missing_columns = sorted(required_columns - columns)
                unexpected_columns = sorted(
                    columns - required_columns - self.allowed_metadata_columns
                )
                if missing_columns:
                    issues.append(f"Missing staging columns: {missing_columns}")
                if unexpected_columns:
                    issues.append(f"Unexpected staging columns: {unexpected_columns}")

                cursor.execute(
                    sql.SQL("SELECT count(*) FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.staging_schema),
                        table=sql.Identifier(staging_table),
                    )
                )
                target_count = int(cursor.fetchone()[0] or 0)

                null_key_count = 0
                duplicate_key_count = 0
                if spec.primary_key in columns:
                    cursor.execute(
                        sql.SQL(
                            "SELECT count(*) FILTER (WHERE {pk} IS NULL), "
                            "count({pk}) - count(DISTINCT {pk}) FROM {schema}.{table}"
                        ).format(
                            pk=sql.Identifier(spec.primary_key),
                            schema=sql.Identifier(self.staging_schema),
                            table=sql.Identifier(staging_table),
                        )
                    )
                    row = cursor.fetchone()
                    null_key_count = int(row[0] or 0)
                    duplicate_key_count = int(row[1] or 0)
                    if null_key_count:
                        issues.append(f"NULL staging primary keys: {null_key_count}")
                    if duplicate_key_count:
                        issues.append(f"Duplicate staging primary keys: {duplicate_key_count}")

                join_ok = True
                if spec.source_table == "sales_person" and "salesperson_name" in columns:
                    cursor.execute(
                        sql.SQL(
                            "SELECT count(*) FROM {schema}.{table} "
                            "WHERE coalesce(trim(salesperson_name), '') = ''"
                        ).format(
                            schema=sql.Identifier(self.staging_schema),
                            table=sql.Identifier(staging_table),
                        )
                    )
                    blank_count = int(cursor.fetchone()[0] or 0)
                    join_ok = blank_count == 0
                    if not join_ok:
                        issues.append("sales_person Person join produced blank salesperson_name")
            finally:
                cursor.close()

        threshold_ok = rejected_count <= rejected_threshold
        if not threshold_ok:
            issues.append(
                f"Rejected row threshold exceeded: rejected_count={rejected_count}, "
                f"threshold={rejected_threshold}"
            )

        return {
            "validation_passed": not issues,
            "schema_ok": not missing_columns and not unexpected_columns,
            "primary_key_nulls": null_key_count,
            "duplicate_primary_keys": duplicate_key_count,
            "required_joins_ok": join_ok,
            "rejected_threshold_ok": threshold_ok,
            "source_count": source_count,
            "target_count": target_count,
            "rejected_count": rejected_count,
            "issues": issues,
        }