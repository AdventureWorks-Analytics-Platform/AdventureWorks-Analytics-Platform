from datetime import datetime
import hashlib
import json
from typing import Optional

import pandas as pd

from src.shared.connectors.sql_server_connector import SQLServerConnector
from src.core.settings import Settings, get_settings
from src.shared.ingestion.ingestion_models import ExtractionBatch, TableSpec


class SalesExtractor:
    """Extract sales-domain source tables from SQL Server."""

    def __init__(
        self,
        source_system: str = "AdventureWorks2012",
        settings: Optional[Settings] = None,
    ):
        self.source_system = source_system
        self.settings = settings or get_settings()

    def extract_table(
        self,
        source_schema: str,
        source_table: str,
        load_date: Optional[datetime] = None,
    ):
        legacy_spec = TableSpec(
            source_schema=source_schema,
            source_table=source_table,
            target_schema="bronze",
            target_table=source_table.lower(),
            primary_key="_legacy_row_key",
            required_columns=("_legacy_row_key",),
            ordering_key="_legacy_row_key",
        )
        batches = self.iter_table_batches(
            legacy_spec,
            load_date=load_date,
            legacy_query=True,
        )
        frames = [batch.dataframe for batch in batches]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def iter_table_batches(
        self,
        spec: TableSpec,
        load_date: Optional[datetime] = None,
        legacy_query: bool = False,
        start_after=None,
    ):
        if load_date is None:
            load_date = datetime.now()

        if spec.ordering_key is None:
            raise ValueError("TableSpec ordering_key is required for batch extraction")
        ordering_key = spec.ordering_key

        query = f"SELECT * FROM {spec.source_name}"
        if not legacy_query:
            if start_after is not None:
                query += f" WHERE {ordering_key} > ?"
            query += f" ORDER BY {ordering_key}"

        with SQLServerConnector(settings=self.settings) as sql_conn:
            cursor = sql_conn.connection.cursor()
            try:
                if hasattr(cursor, "timeout"):
                    cursor.timeout = self.settings.bronze_query_timeout_seconds
                elif hasattr(sql_conn.connection, "timeout"):
                    sql_conn.connection.timeout = (
                        self.settings.bronze_query_timeout_seconds
                    )
                if start_after is None:
                    cursor.execute(query)
                else:
                    cursor.execute(query, start_after)
                columns = [description[0] for description in cursor.description]
                batch_number = 1
                while True:
                    rows = cursor.fetchmany(self.settings.batch_size)
                    if not rows:
                        break
                    normalized_rows = [tuple(row) for row in rows]
                    df = pd.DataFrame(normalized_rows, columns=columns)
                    self._add_lineage(df, spec.source_name, load_date)
                    lower_bound, upper_bound = self._batch_bounds(df, ordering_key)
                    yield ExtractionBatch(df, batch_number, lower_bound, upper_bound)
                    batch_number += 1
            finally:
                cursor.close()

    def _add_lineage(
        self, df: pd.DataFrame, source_table: str, load_date: datetime
    ) -> None:
        df["_source_system"] = self.source_system
        df["_source_table"] = source_table
        df["_load_date"] = load_date

        def compute_record_hash(row: pd.Series) -> str:
            payload = row.drop(
                labels=["_source_system", "_source_table", "_load_date"],
                errors="ignore",
            ).to_dict()
            normalized = json.dumps(payload, default=str, sort_keys=True)
            return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

        df["_record_hash"] = df.apply(compute_record_hash, axis=1)

    @staticmethod
    def _batch_bounds(df: pd.DataFrame, ordering_key: str):
        if ordering_key not in df.columns or df.empty:
            return None, None
        return df[ordering_key].iloc[0], df[ordering_key].iloc[-1]
