from typing import Tuple

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.core.settings import Settings, get_settings
from src.shared.ingestion.checkpoint_manager import PostgresCheckpointManager


class BronzeLoader:
    """Load DataFrames into PostgreSQL Bronze tables."""

    staging_schema = "bronze_staging"

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def load(self, df: pd.DataFrame, target_schema: str, target_table: str, if_exists: str = "replace") -> Tuple[int, bool]:
        with PostgreSQLConnector(settings=self.settings) as pg_conn:
            with pg_conn.connection.cursor() as cursor:
                cursor.execute(
                    "SET statement_timeout = %s",
                    (self.settings.bronze_query_timeout_seconds * 1000,),
                )
            pg_conn.connection.commit()
            engine = create_engine(
                "postgresql://",
                creator=lambda: pg_conn.connection,
                poolclass=StaticPool,
            )
            try:
                df.to_sql(
                    target_table,
                    engine,
                    schema=target_schema,
                    if_exists=if_exists,
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
                pg_conn.connection.commit()
                return len(df), True
            except Exception:
                pg_conn.connection.rollback()
                raise
            finally:
                engine.dispose()

    def read_staging(self, staging_table: str, spec) -> pd.DataFrame:
        """Read the complete run-specific staging table for resume validation."""
        with PostgreSQLConnector(settings=self.settings) as pg_conn:
            engine = create_engine(
                "postgresql://",
                creator=lambda: pg_conn.connection,
                poolclass=StaticPool,
            )
            try:
                return pd.read_sql_query(
                    f'SELECT * FROM "{self.staging_schema}"."{staging_table}"',
                    engine,
                )
            except Exception:
                pg_conn.connection.rollback()
                raise
            finally:
                engine.dispose()

    def load_batch_transactionally(
        self,
        df: pd.DataFrame,
        target_schema: str,
        target_table: str,
        batch_id: str,
        upper_bound,
        checkpoint_manager: PostgresCheckpointManager,
        content_hash: str | None = None,
        if_exists: str = "append",
    ) -> Tuple[int, bool]:
        """Write one batch and its checkpoint in the same database transaction."""
        with PostgreSQLConnector(settings=self.settings) as pg_conn:
            with pg_conn.connection.cursor() as cursor:
                cursor.execute(
                    "SET statement_timeout = %s",
                    (self.settings.bronze_batch_timeout_seconds * 1000,),
                )
            pg_conn.connection.commit()
            engine = create_engine(
                "postgresql://",
                creator=lambda: pg_conn.connection,
                poolclass=StaticPool,
            )
            try:
                with engine.begin() as transaction:
                    df.to_sql(
                        target_table,
                        transaction,
                        schema=target_schema,
                        if_exists=if_exists,
                        index=False,
                        method="multi",
                        chunksize=1000,
                    )
                    if content_hash is None:
                        checkpoint_manager.advance_in_transaction(
                            transaction, batch_id, upper_bound
                        )
                    else:
                        checkpoint_manager.advance_in_transaction(
                            transaction, batch_id, upper_bound, content_hash
                        )
                return len(df), True
            finally:
                engine.dispose()
