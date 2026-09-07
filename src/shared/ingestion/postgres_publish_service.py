from uuid import uuid4

import pandas as pd
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_ingestion_schema import ensure_ingestion_schema


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
        ensure_ingestion_schema(self.settings)

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


class PostgresSilverStagingWriter:
    """Persist Silver staging frames before the atomic publish step."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self._initialized_tables: set[str] = set()
        ensure_ingestion_schema(self.settings)

    def __call__(self, frame: pd.DataFrame, staging_table: str) -> None:
        PostgresPublishService._validate_identifier(staging_table)
        if_exists = "append" if staging_table in self._initialized_tables else "replace"
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
                self._initialized_tables.add(staging_table)
            except Exception:
                pg_conn.connection.rollback()
                raise
            finally:
                engine.dispose()