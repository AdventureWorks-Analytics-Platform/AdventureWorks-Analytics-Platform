from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable
from typing import Any

import pandas as pd

from src.core.settings import Settings, get_settings
from src.shared.ingestion.checkpoint_manager import CheckpointManager
from src.shared.ingestion.ingestion_models import (
    ExecutionIdentity,
    RejectedRecord,
    TableSpec,
    utc_now,
)
from src.shared.ingestion.quarantine_service import QuarantineService
from src.shared.ingestion.reconciliation_service import ReconciliationService
from src.shared.ingestion.retry_policy import RetryPolicy, execute_with_retry
from src.shared.ingestion.staging_manager import StagingManager


class SilverValidationError(ValueError):
    """Raised when a Silver input or output contract fails closed."""


class SilverRejectionThresholdError(ValueError):
    """Raised when persisted row rejections exceed the configured policy."""


SILVER_INPUT_REQUIRED_COLUMNS = {
    "sales_order_header": (
        "SalesOrderID", "OrderDate", "DueDate", "ShipDate", "CustomerID",
        "SalesPersonID", "TerritoryID", "SubTotal", "TaxAmt", "Freight",
        "TotalDue", "OnlineOrderFlag", "Status",
    ),
    "sales_order_detail": (
        "SalesOrderID", "SalesOrderDetailID", "ProductID", "OrderQty",
        "UnitPrice", "UnitPriceDiscount", "LineTotal",
    ),
    "customer": (
        "CustomerID", "PersonID", "StoreID", "TerritoryID", "AccountNumber",
    ),
    "sales_territory": ("TerritoryID", "Name", "CountryRegionCode", "Group"),
    "sales_person": (
        "BusinessEntityID", "TerritoryID", "SalesQuota", "Bonus", "CommissionPct",
    ),
    "product": (
        "ProductID", "Name", "ProductNumber", "ProductLine", "Class", "Style",
        "ListPrice", "StandardCost", "DiscontinuedDate",
    ),
}

SILVER_CONVERSION_RULES = {
    "sales_order_header": {
        "date": ("OrderDate", "DueDate", "ShipDate"),
        "numeric": ("SubTotal", "TaxAmt", "Freight", "TotalDue"),
    },
    "sales_order_detail": {
        "integer": ("OrderQty",),
        "numeric": ("UnitPrice", "UnitPriceDiscount", "LineTotal"),
    },
    "customer": {"string": ("AccountNumber",)},
    "sales_territory": {"string": ("Name", "CountryRegionCode", "Group")},
    "product": {
        "string": ("Name", "ProductNumber"),
        "numeric": ("ListPrice", "StandardCost"),
    },
    "sales_person": {"numeric": ("SalesQuota", "Bonus", "CommissionPct")},
}

SILVER_INPUT_KEY_COLUMNS = {
    "sales_order_header": "SalesOrderID",
    "sales_order_detail": "SalesOrderDetailID",
    "customer": "CustomerID",
    "sales_territory": "TerritoryID",
    "sales_person": "BusinessEntityID",
    "product": "ProductID",
}

SILVER_OUTPUT_NUMERIC_COLUMNS = {
    "sales_order_header": ("subtotal", "tax_amt", "freight", "total_due"),
    "sales_order_detail": ("unit_price", "unit_price_discount", "line_total"),
    "product": ("list_price", "standard_cost"),
    "sales_person": ("sales_quota", "bonus", "commission_pct"),
}


SALES_SILVER_TABLE_SPECS = (
    TableSpec(
        "bronze",
        "sales_order_header",
        "silver",
        "sales_order_header_clean",
        "sales_order_id",
        (
            "sales_order_id",
            "order_date",
            "due_date",
            "ship_date",
            "customer_id",
            "salesperson_id",
            "territory_id",
            "subtotal",
            "tax_amt",
            "freight",
            "total_due",
            "is_online_order",
            "status_code",
        ),
        "sales_order_id",
    ),
    TableSpec(
        "bronze",
        "sales_order_detail",
        "silver",
        "sales_order_detail_clean",
        "sales_order_detail_id",
        (
            "sales_order_id",
            "sales_order_detail_id",
            "product_id",
            "order_qty",
            "unit_price",
            "unit_price_discount",
            "line_total",
        ),
        "sales_order_detail_id",
    ),
    TableSpec(
        "bronze",
        "customer",
        "silver",
        "customer_clean",
        "customer_id",
        (
            "customer_id",
            "person_id",
            "store_id",
            "territory_id",
            "account_number",
            "customer_name",
            "customer_type",
            "customer_name_source",
        ),
        "customer_id",
    ),
    TableSpec(
        "bronze",
        "sales_territory",
        "silver",
        "sales_territory_clean",
        "territory_id",
        (
            "territory_id",
            "territory_name",
            "country_region_code",
            "territory_group",
        ),
        "territory_id",
    ),
    TableSpec(
        "bronze",
        "product",
        "silver",
        "product_clean",
        "product_id",
        (
            "product_id",
            "product_name",
            "product_number",
            "product_line",
            "class",
            "style",
            "list_price",
            "standard_cost",
            "is_discontinued",
        ),
        "product_id",
    ),
    TableSpec(
        "bronze",
        "sales_person",
        "silver",
        "sales_person_clean",
        "salesperson_id",
        (
            "salesperson_id",
            "business_entity_id",
            "territory_id",
            "sales_quota",
            "bonus",
            "commission_pct",
            "salesperson_name",
        ),
        "salesperson_id",
    ),
)


class SilverTransformationJob:
    """Encapsulate Silver transformations behind a reusable, injectable job contract."""

    allowed_silver_statuses = {"SUCCESS", "SUCCESS_WITH_REJECTIONS"}

    def __init__(
        self,
        table_specs: Iterable[TableSpec] | None = None,
        settings: Settings | None = None,
        reader: Callable[[str, object], pd.DataFrame] | None = None,
        transformer: Callable[[str, pd.DataFrame, pd.DataFrame | None], pd.DataFrame] | None = None,
        writer: Callable[[pd.DataFrame, str], None] | None = None,
        quarantine_service: QuarantineService | None = None,
        rejected_threshold: int | None = None,
        transform_version: str | None = None,
        staging_manager: StagingManager | None = None,
        checkpoint_manager: CheckpointManager | None = None,
        retry_policy: RetryPolicy | None = None,
        sleeper: Callable[[float], None] | None = None,
        staging_writer: Callable[[pd.DataFrame, str], None] | None = None,
        validator: Callable[..., dict] | None = None,
        publish_service: Any | None = None,
        dedup_service: Any | None = None,
        staging_reader: Callable[[str], pd.DataFrame] | None = None,
        sql_validator: Any | None = None,
    ):
        self.settings = settings or get_settings()
        self.table_specs = tuple(table_specs or SALES_SILVER_TABLE_SPECS)
        self.reader = reader or self._default_reader
        self.transformer = transformer or self._default_transformer
        self.writer = writer or self._default_writer
        self.quarantine_service = quarantine_service or QuarantineService()
        self.rejected_threshold = (
            getattr(self.settings, "silver_rejected_threshold", 0)
            if rejected_threshold is None
            else rejected_threshold
        )
        if self.rejected_threshold < 0:
            raise ValueError("rejected_threshold cannot be negative")
        self.transform_version = transform_version or getattr(
            self.settings, "silver_transform_version", "silver-v1"
        )
        self.staging_manager = staging_manager or StagingManager()
        self.reconciliation = ReconciliationService(self.staging_manager)
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.retry_policy = retry_policy or RetryPolicy(
            max_attempts=getattr(self.settings, "retry_max_attempts", 3),
            initial_delay_seconds=getattr(
                self.settings, "retry_initial_delay_seconds", 1.0
            ),
            max_delay_seconds=getattr(self.settings, "retry_max_delay_seconds", 30.0),
        )
        self.sleeper = sleeper or time.sleep
        self._staging_frames: dict[str, list[pd.DataFrame]] = {}
        self.staging_writer = staging_writer or self._default_staging_writer
        self.validator = validator or self._default_staging_validator
        self.publish_service = publish_service
        self.dedup_service = dedup_service
        self.staging_reader = staging_reader or self._default_staging_reader
        self.sql_validator = sql_validator
        self.dependency_order = [spec.source_table for spec in self.table_specs]

    def _default_reader(self, source_table: str, settings: object) -> pd.DataFrame:
        chunks = list(
            self.read_chunks(
                source_table,
                f'bronze."{source_table}"',
                chunksize=getattr(settings, "batch_size", 10000),
            )
        )
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def _ordering_key_for(self, source_table: str) -> str:
        mapping = {
            "sales_order_header": "SalesOrderID",
            "sales_order_detail": "SalesOrderDetailID",
            "customer": "CustomerID",
            "sales_territory": "TerritoryID",
            "sales_person": "BusinessEntityID",
            "product": "ProductID",
            "person": "BusinessEntityID",
        }
        return mapping.get(source_table, source_table)

    def _with_batch_lineage(
        self,
        frame: pd.DataFrame,
        source_table: str,
        run_id: str | None = None,
        load_id: str | None = None,
        batch_id: str | None = None,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame.copy()

        output = frame.copy()
        output["run_id"] = run_id or "run-unknown"
        output["load_id"] = load_id or "load-unknown"
        output["batch_id"] = batch_id or hashlib.sha256(
            json.dumps(
                {
                    "source_table": source_table,
                    "row_count": len(output),
                    "first_row": output.iloc[0].dropna().to_dict(),
                },
                default=str,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

        def _record_hash(row: pd.Series) -> str:
            payload = row.drop(labels=["run_id", "load_id", "batch_id"], errors="ignore").to_dict()
            normalized = json.dumps(payload, default=str, sort_keys=True)
            return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

        output["_record_hash"] = output.apply(_record_hash, axis=1)
        return output

    def read_chunks(
        self,
        source_table: str,
        source_name: str | None = None,
        chunksize: int | None = None,
        run_id: str | None = None,
        load_id: str | None = None,
    ):
        default_reader = getattr(self._default_reader, "__func__", self._default_reader)
        current_reader = getattr(self.reader, "__func__", self.reader)
        if current_reader is not default_reader:
            frame, _, _ = execute_with_retry(
                lambda: self.reader(source_table, self.settings),
                self.retry_policy,
                self.sleeper,
            )
            yield self._with_batch_lineage(frame, source_table, run_id=run_id, load_id=load_id)
            return

        batch_size = int(chunksize or getattr(self.settings, "batch_size", 10000))
        ordering_key = self._ordering_key_for(source_table)
        resolved_source = source_name or f'bronze."{source_table}"'
        query = f'SELECT * FROM {resolved_source} ORDER BY "{ordering_key}"'

        from scripts.transformation.silver.sales_silver_clean import _warehouse_engine
        from src.shared.connectors.postgres_connector import PostgreSQLConnector

        with PostgreSQLConnector() as pg_conn:
            engine = _warehouse_engine(pg_conn.connection)
            try:
                for batch_number, chunk in enumerate(
                    pd.read_sql_query(query, engine, chunksize=batch_size), start=1
                ):
                    batch_id = hashlib.sha256(
                        json.dumps(
                            {
                                "source_table": source_table,
                                "run_id": run_id,
                                "load_id": load_id,
                                "batch_number": batch_number,
                                "ordering_key": ordering_key,
                            },
                            default=str,
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest()
                    yield self._with_batch_lineage(
                        chunk,
                        source_table,
                        run_id=run_id,
                        load_id=load_id,
                        batch_id=batch_id,
                    )
            finally:
                engine.dispose()

    def _default_transformer(
        self,
        source_table: str,
        bronze_frame: pd.DataFrame,
        person_frame: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        from scripts.transformation.silver.sales_silver_clean import CLEANERS

        if source_table in {"customer", "sales_person"}:
            return CLEANERS[source_table](bronze_frame, person_frame)
        return CLEANERS[source_table](bronze_frame)

    def _validate_input_schema(self, frame: pd.DataFrame, spec: TableSpec) -> None:
        required_columns = SILVER_INPUT_REQUIRED_COLUMNS.get(
            spec.source_table, spec.required_columns
        )
        missing_columns = [
            column for column in required_columns if column not in frame.columns
        ]
        if missing_columns:
            raise SilverValidationError(
                f"Missing required Bronze columns for {spec.source_name}: "
                f"{missing_columns}"
            )

    def _partition_conversion_errors(
        self, frame: pd.DataFrame, spec: TableSpec
    ) -> tuple[pd.DataFrame, tuple[dict[str, object], ...]]:
        rules = SILVER_CONVERSION_RULES.get(spec.source_table, {})
        rejected_mask = pd.Series(False, index=frame.index)
        reasons: dict[object, list[str]] = {}

        for column in rules.get("date", ()):
            converted = pd.to_datetime(frame[column], errors="coerce")
            invalid = frame[column].notna() & converted.isna()
            for index in frame.index[invalid]:
                reasons.setdefault(index, []).append(f"invalid date: {column}")
            rejected_mask |= invalid

        for column in rules.get("numeric", ()):
            converted = pd.to_numeric(frame[column], errors="coerce")
            invalid = frame[column].notna() & converted.isna()
            for index in frame.index[invalid]:
                reasons.setdefault(index, []).append(f"invalid numeric: {column}")
            rejected_mask |= invalid

        for column in rules.get("integer", ()):
            converted = pd.to_numeric(frame[column], errors="coerce")
            invalid = frame[column].notna() & (
                converted.isna() | converted.mod(1).ne(0)
            )
            for index in frame.index[invalid]:
                reasons.setdefault(index, []).append(f"invalid integer: {column}")
            rejected_mask |= invalid

        for column in rules.get("string", ()):
            invalid = frame[column].notna() & frame[column].astype("string").str.strip().eq("")
            for index in frame.index[invalid]:
                reasons.setdefault(index, []).append(f"blank required string: {column}")
            rejected_mask |= invalid

        rejected = tuple(
            {
                "run_id": self._optional_string(frame.loc[index].get("run_id")) or "run-unknown",
                "load_id": self._optional_string(frame.loc[index].get("load_id")) or "load-unknown",
                "batch_id": self._optional_string(frame.loc[index].get("batch_id")) or "batch-unknown",
                "record_key": str(
                    frame.loc[index].get(
                        SILVER_INPUT_KEY_COLUMNS.get(spec.source_table, spec.primary_key),
                        index,
                    )
                ),
                "source_hash": self._optional_string(frame.loc[index].get("_record_hash")),
                "reason": "; ".join(reason_list),
            }
            for index, reason_list in reasons.items()
        )
        return frame.loc[~rejected_mask].copy(), rejected

    @staticmethod
    def _optional_string(value: object) -> str | None:
        return None if pd.isna(value) else str(value)

    def _validate_output_schema(self, frame: pd.DataFrame, spec: TableSpec) -> None:
        missing_columns = [
            column for column in spec.required_columns if column not in frame.columns
        ]
        allowed_metadata = {
            "_source_system", "_source_table", "_load_date", "_record_hash",
            "run_id", "load_id", "batch_id", "source_snapshot_id",
        }
        unexpected_columns = [
            column for column in frame.columns
            if column not in set(spec.required_columns) | allowed_metadata
        ]
        if missing_columns or unexpected_columns:
            raise SilverValidationError(
                f"Invalid Silver output schema for {spec.target_name}: "
                f"missing={missing_columns}, unexpected={unexpected_columns}"
            )
        if not frame.empty and frame[spec.primary_key].isna().any():
            raise SilverValidationError(
                f"NULL Silver primary key for {spec.target_name}: {spec.primary_key}"
            )
        invalid_types = [
            column for column in SILVER_OUTPUT_NUMERIC_COLUMNS.get(spec.source_table, ())
            if column in frame.columns and not pd.api.types.is_numeric_dtype(frame[column])
        ]
        if invalid_types:
            raise SilverValidationError(
                f"Invalid Silver output types for {spec.target_name}: "
                f"numeric columns={invalid_types}"
            )

    def _default_staging_validator(
        self,
        frame: pd.DataFrame,
        spec: TableSpec,
        source_count: int,
        rejected_count: int,
    ) -> dict[str, object]:
        issues: list[str] = []
        missing_columns = [
            column for column in spec.required_columns if column not in frame.columns
        ]
        allowed_metadata = {
            "_source_system", "_source_table", "_load_date", "_record_hash",
            "run_id", "load_id", "batch_id", "source_snapshot_id",
        }
        unexpected_columns = [
            column for column in frame.columns
            if column not in set(spec.required_columns) | allowed_metadata
        ]
        if missing_columns:
            issues.append(f"Missing staging columns: {missing_columns}")
        if unexpected_columns:
            issues.append(f"Unexpected staging columns: {unexpected_columns}")

        null_key_count = (
            int(frame[spec.primary_key].isna().sum())
            if spec.primary_key in frame.columns
            else 0
        )
        duplicate_key_count = (
            int(frame[spec.primary_key].duplicated().sum())
            if spec.primary_key in frame.columns
            else 0
        )
        if null_key_count:
            issues.append(f"NULL staging primary keys: {null_key_count}")
        if duplicate_key_count:
            issues.append(f"Duplicate staging primary keys: {duplicate_key_count}")

        join_ok = True
        if spec.source_table == "sales_person" and "salesperson_name" in frame.columns:
            join_ok = frame["salesperson_name"].fillna("").astype("string").str.strip().ne("").all()
            if not join_ok:
                issues.append("sales_person Person join produced blank salesperson_name")

        threshold_ok = rejected_count <= self.rejected_threshold
        if not threshold_ok:
            issues.append(
                f"Rejected row threshold exceeded: rejected_count={rejected_count}, "
                f"threshold={self.rejected_threshold}"
            )

        return {
            "validation_passed": not issues,
            "schema_ok": not missing_columns and not unexpected_columns,
            "primary_key_nulls": null_key_count,
            "duplicate_primary_keys": duplicate_key_count,
            "required_joins_ok": join_ok,
            "rejected_threshold_ok": threshold_ok,
            "source_count": source_count,
            "target_count": len(frame),
            "rejected_count": rejected_count,
            "issues": issues,
        }

    def _record_rejections(
        self,
        spec: TableSpec,
        rejections: tuple[dict[str, object], ...],
    ) -> list[RejectedRecord]:
        records = []
        for rejection in rejections:
            record = RejectedRecord(
                run_id=str(rejection["run_id"]),
                load_id=str(rejection["load_id"]),
                batch_id=str(rejection["batch_id"]),
                source_table=spec.source_name,
                record_key=rejection["record_key"],
                source_hash=rejection["source_hash"],
                reason=str(rejection["reason"]),
                rejected_at=utc_now(),
                transform_version=self.transform_version,
                error_type="ConversionError",
            )
            self.quarantine_service.record(record)
            records.append(record)
        return records

    @staticmethod
    def _rejection_summaries(records: list[RejectedRecord]) -> list[dict[str, object]]:
        return [
            {
                "record_key": record.record_key,
                "source_hash": record.source_hash,
                "reason": record.reason,
                "transform_version": record.transform_version,
                "error_type": record.error_type,
            }
            for record in records
        ]

    def _default_writer(self, silver_frame: pd.DataFrame, target_name: str) -> None:
        if self.publish_service is None:
            raise RuntimeError(
                "Refusing unsafe to_sql(if_exists='replace') write: no publish_service "
                "or custom writer was injected. Inject a publish_service (e.g. "
                "PostgresSilverPublishService) for atomic staging publish."
            )
        from scripts.transformation.silver.sales_silver_clean import _warehouse_engine
        from src.shared.connectors.postgres_connector import PostgreSQLConnector

        with PostgreSQLConnector() as pg_conn:
            engine = _warehouse_engine(pg_conn.connection)
            silver_frame.to_sql(
                target_name,
                engine,
                schema="silver",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

    def _default_staging_writer(self, silver_frame: pd.DataFrame, staging_name: str) -> None:
        self._staging_frames.setdefault(staging_name, []).append(silver_frame.copy())

    def _default_staging_reader(self, staging_name: str) -> pd.DataFrame:
        frames = self._staging_frames.get(staging_name, [])
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _rewrite_staged_content(self, silver_frame: pd.DataFrame, staging_name: str) -> None:
        """Overwrite per-batch staged rows with the globally deduplicated frame.

        Without this, `publish_service.publish()` renames the raw staging table
        (still containing cross-batch duplicates) instead of the deduplicated result.
        """
        replace_all = getattr(self.staging_writer, "replace_all", None)
        if callable(replace_all):
            replace_all(silver_frame, staging_name)
        else:
            self._staging_frames[staging_name] = [silver_frame.copy()]

    @staticmethod
    def _resolve_row_count(
        silver_frame: pd.DataFrame | None, validation_report: dict | None
    ) -> int:
        """Row count for results, without requiring a materialized frame.

        Falls back to the SQL validator's `target_count` on the full SQL path
        (`silver_frame is None`), where staging is never read back into pandas.
        """
        if silver_frame is not None:
            return len(silver_frame)
        return int((validation_report or {}).get("target_count", 0))

    @staticmethod
    def _content_hash(frame: pd.DataFrame) -> str:
        payload = frame.to_dict(orient="records")
        normalized = json.dumps(payload, default=str, sort_keys=True)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _batch_id_for(
        self,
        bronze_frame: pd.DataFrame,
        source_table: str,
        batch_number: int,
        content_hash: str,
    ) -> str:
        if "batch_id" in bronze_frame.columns and not bronze_frame.empty:
            batch_id = self._optional_string(bronze_frame.iloc[0].get("batch_id"))
            if batch_id:
                return batch_id
        return hashlib.sha256(
            json.dumps(
                {
                    "source_table": source_table,
                    "batch_number": batch_number,
                    "content_hash": content_hash,
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

    def _upper_bound_for(self, bronze_frame: pd.DataFrame, source_table: str):
        source_key = SILVER_INPUT_KEY_COLUMNS.get(
            source_table, self._ordering_key_for(source_table)
        )
        if source_key in bronze_frame.columns and not bronze_frame.empty:
            return bronze_frame[source_key].iloc[-1]
        return None

    def _commit_staged_batch(
        self,
        staging_name: str,
        bronze_frame: pd.DataFrame,
        silver_frame: pd.DataFrame,
        source_table: str,
        batch_number: int,
    ) -> tuple[int, int]:
        content_hash = self._content_hash(silver_frame)
        batch_id = self._batch_id_for(
            bronze_frame, source_table, batch_number, content_hash
        )
        upper_bound = self._upper_bound_for(bronze_frame, source_table)

        def commit():
            resolution = self.reconciliation.resolve(
                staging_name, batch_id, content_hash
            )
            if resolution == "SKIP":
                self.checkpoint_manager.mark_committed(batch_id)
                self.checkpoint_manager.advance(batch_id, upper_bound)
                return len(silver_frame)
            self.staging_writer(silver_frame, staging_name)
            self.staging_manager.write_batch(
                staging_name,
                batch_id,
                batch_number,
                len(silver_frame),
                upper_bound=upper_bound,
                content_hash=content_hash,
            )
            self.checkpoint_manager.mark_committed(batch_id)
            self.checkpoint_manager.advance(batch_id, upper_bound)
            return len(silver_frame)

        written, attempt_count, _ = execute_with_retry(
            commit, self.retry_policy, self.sleeper
        )
        return written, attempt_count

    def _global_deduplicate(
        self, frame: pd.DataFrame, spec: TableSpec
    ) -> tuple[pd.DataFrame, int]:
        if frame.empty or spec.primary_key not in frame.columns:
            return frame.reset_index(drop=True), 0

        working = frame.copy()
        if "_record_hash" not in working.columns:
            working["_record_hash"] = working.apply(
                lambda row: hashlib.sha256(
                    json.dumps(row.to_dict(), default=str, sort_keys=True).encode("utf-8")
                ).hexdigest(),
                axis=1,
            )
        else:
            working["_record_hash"] = working["_record_hash"].fillna("")
        if "_load_date" in working.columns:
            working["_load_date"] = pd.to_datetime(
                working["_load_date"], errors="coerce"
            )
            working = working.sort_values(
                ["_load_date", "_record_hash"],
                ascending=[False, False],
                kind="mergesort",
            )
        else:
            working = working.sort_values(
                ["_record_hash"], ascending=[False], kind="mergesort"
            )
        before = len(working)
        result = (
            working.drop_duplicates(subset=[spec.primary_key], keep="first")
            .reset_index(drop=True)
        )
        return result, before - len(result)

    @staticmethod
    def _validate_detail_grain(frame: pd.DataFrame, spec: TableSpec) -> None:
        if spec.source_table != "sales_order_detail" or spec.primary_key not in frame.columns:
            return
        duplicate_count = int(frame[spec.primary_key].duplicated().sum())
        if duplicate_count:
            raise SilverValidationError(
                f"Duplicate detail grain for {spec.target_name}: "
                f"{spec.primary_key} duplicates={duplicate_count}"
            )

    @staticmethod
    def _table_batch_id(run_id: str, load_id: str, spec: TableSpec) -> str:
        payload = f"{run_id}:{load_id}:{spec.source_name}:{spec.target_name}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _standard_table_result(
        self,
        spec: TableSpec,
        run_id: str,
        load_id: str,
        batch_id: str,
        started_at,
        finished_at,
        status: str,
        rows_read: int,
        rows_written: int,
        rows_rejected: int,
        attempt_count: int,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, object]:
        return {
            "run_id": run_id,
            "load_id": load_id,
            "batch_id": batch_id,
            "stage": "silver",
            "source_table": spec.source_name,
            "target_table": spec.target_name,
            "status": status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_rejected": rows_rejected,
            "rejected_threshold": self.rejected_threshold,
            "attempt_count": attempt_count,
            "started_at": started_at,
            "finished_at": finished_at,
            "error_type": error_type,
            "error_message": error_message,
        }

    def _validate_execution_order(self) -> None:
        ordered_tables = [spec.source_table for spec in self.table_specs]
        if ordered_tables != self.dependency_order:
            raise RuntimeError(
                "Silver execution order mismatch: "
                f"expected={self.dependency_order}, got={ordered_tables}"
            )

    def _validate_required_dependencies(self) -> None:
        if not any(
            source_table in {"customer", "sales_person"}
            for source_table in self.dependency_order
        ):
            return
        if not any(spec.source_table == "sales_person" for spec in self.table_specs):
            raise RuntimeError(
                "Silver execution order includes 'sales_person' without its target spec."
            )

    def _load_required_dependency_frames(self) -> dict[str, pd.DataFrame]:
        frames: dict[str, pd.DataFrame] = {}
        requires_person_for_salesperson = "sales_person" in self.dependency_order
        requires_person_for_customer = "customer" in self.dependency_order
        if not (requires_person_for_salesperson or requires_person_for_customer):
            return frames
        try:
            frames["bronze.person"] = self.reader("person", self.settings)
        except Exception as exc:  # noqa: BLE001 - resolved by customer row policy
            if requires_person_for_salesperson:
                raise RuntimeError(
                    "Required dependency 'bronze.person' for sales_person "
                    "transformation could not be loaded "
                    f"({exc})."
                ) from exc
            frames["bronze.person"] = None
        return frames

    def run(
        self, run_id: str | None = None, load_id: str | None = None
    ) -> dict[str, dict[str, int]]:
        self._validate_execution_order()
        self._validate_required_dependencies()

        results: dict[str, dict[str, int]] = {}
        dependency_frames = self._load_required_dependency_frames()
        person_frame = dependency_frames.get("bronze.person")
        identity = ExecutionIdentity.create()
        resolved_run_id = run_id or identity.run_id
        resolved_load_id = load_id or identity.load_id
        candidate_schema = None
        snapshot_started = hasattr(self.publish_service, "begin_snapshot")
        if snapshot_started:
            candidate_schema = self.publish_service.begin_snapshot(resolved_run_id)

        for spec in self.table_specs:
            table_started_at = utc_now()
            table_batch_id = self._table_batch_id(
                resolved_run_id, resolved_load_id, spec
            )
            staging = self.staging_manager.create(
                spec.target_table, resolved_run_id, resolved_load_id
            )
            silver_chunks: list[pd.DataFrame] = []
            source_count = 0
            rejected_records: list[RejectedRecord] = []
            batch_number = 0
            attempt_count = 1

            def apply_standard_result(
                result: dict[str, object],
                status: str,
                rows_written: int,
                error_type: str | None = None,
                error_message: str | None = None,
            ) -> None:
                result.update(
                    self._standard_table_result(
                        spec,
                        resolved_run_id,
                        resolved_load_id,
                        table_batch_id,
                        table_started_at,
                        utc_now(),
                        status,
                        source_count,
                        rows_written,
                        len(rejected_records),
                        attempt_count,
                        error_type,
                        error_message,
                    )
                )

            try:
                for bronze_chunk in self.read_chunks(
                    spec.source_table,
                    spec.source_name,
                    chunksize=getattr(self.settings, "batch_size", 10000),
                ):
                    source_count += len(bronze_chunk)
                    if bronze_chunk.empty:
                        continue
                    batch_number += 1
                    self._validate_input_schema(bronze_chunk, spec)
                    valid_chunk, chunk_rejections = self._partition_conversion_errors(
                        bronze_chunk, spec
                    )
                    rejected_records.extend(self._record_rejections(spec, chunk_rejections))
                    if len(rejected_records) > self.rejected_threshold:
                        raise SilverRejectionThresholdError(
                            f"Rejected row threshold exceeded for {spec.source_name}: "
                            f"rejected_count={len(rejected_records)}, "
                            f"threshold={self.rejected_threshold}"
                        )
                    if valid_chunk.empty:
                        _, attempts = self._commit_staged_batch(
                            staging.name,
                            bronze_chunk,
                            pd.DataFrame(),
                            spec.source_table,
                            batch_number,
                        )
                        attempt_count = max(attempt_count, attempts)
                        continue
                    silver_chunk = self.transformer(
                        spec.source_table, valid_chunk, person_frame
                    )
                    if self.dedup_service is None:
                        silver_chunks.append(silver_chunk)
                    staged_silver_chunk = silver_chunk.copy()
                    if not staged_silver_chunk.empty:
                        staged_silver_chunk["source_snapshot_id"] = resolved_run_id
                    _, attempts = self._commit_staged_batch(
                        staging.name,
                        bronze_chunk,
                        staged_silver_chunk,
                        spec.source_table,
                        batch_number,
                    )
                    attempt_count = max(attempt_count, attempts)
            except (SilverValidationError, SilverRejectionThresholdError) as exc:
                self.staging_manager.mark_failed(staging.name)
                results[spec.target_table] = {
                    "status": "FAILED",
                    "source_count": source_count,
                    "target_count": 0,
                    "rows_read": source_count,
                    "rows_valid": 0,
                    "rows_rejected": len(rejected_records),
                    "rows_deduplicated": 0,
                    "rows_published": 0,
                    "rejection_reasons": self._rejection_summaries(rejected_records),
                    "attempt_count": attempt_count,
                    "staging_name": staging.name,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
                apply_standard_result(
                    results[spec.target_table],
                    "FAILED",
                    0,
                    type(exc).__name__,
                    str(exc),
                )
                continue
            except Exception as exc:
                self.staging_manager.mark_failed(staging.name)
                error = RuntimeError(
                    f"Silver table '{spec.source_table}' failed because required Bronze source "
                    f"'{spec.source_name}' could not be read or staged: "
                    f"{type(exc).__name__}: {exc}"
                )
                results[spec.target_table] = {
                    "staging_name": staging.name,
                    "published": False,
                }
                apply_standard_result(
                    results[spec.target_table],
                    "FAILED",
                    0,
                    type(error).__name__,
                    str(error),
                )
                continue

            if spec.source_table == "sales_person" and person_frame is None:
                self.staging_manager.mark_failed(staging.name)
                raise RuntimeError(
                    "Silver table 'sales_person' failed because required dependency "
                    "'bronze.person' is unavailable."
                )

            if self.dedup_service is not None:
                try:
                    duplicate_detail_count = self.dedup_service.count_duplicate_keys(
                        staging.name, spec.primary_key
                    )
                except Exception as exc:
                    duplicate_detail_exc = SilverValidationError(
                        f"Duplicate-key check failed for {spec.target_name}: {exc}"
                    )
                else:
                    duplicate_detail_exc = None
                    if spec.source_table == "sales_order_detail" and duplicate_detail_count:
                        duplicate_detail_exc = SilverValidationError(
                            f"Duplicate detail grain for {spec.target_name}: "
                            f"{spec.primary_key} duplicates={duplicate_detail_count}"
                        )
                if duplicate_detail_exc is not None:
                    self.staging_manager.mark_failed(staging.name)
                    results[spec.target_table] = {
                        "status": "FAILED",
                        "source_count": source_count,
                        "target_count": 0,
                        "rows_read": source_count,
                        "rows_valid": source_count - len(rejected_records),
                        "rows_rejected": len(rejected_records),
                        "rows_deduplicated": 0,
                        "rows_published": 0,
                        "rejection_reasons": self._rejection_summaries(rejected_records),
                        "attempt_count": attempt_count,
                        "staging_name": staging.name,
                        "published": False,
                        "error_type": type(duplicate_detail_exc).__name__,
                        "error_message": str(duplicate_detail_exc),
                    }
                    apply_standard_result(
                        results[spec.target_table],
                        "FAILED",
                        0,
                        type(duplicate_detail_exc).__name__,
                        str(duplicate_detail_exc),
                    )
                    continue
                # SQL-side dedup mutates the staged Postgres table directly, so no
                # in-memory concat/rewrite is needed; read back only for validation.
                rows_deduplicated = self.dedup_service.deduplicate(
                    staging.name, spec.primary_key
                )
                if self.sql_validator is not None:
                    if self.publish_service is None:
                        raise RuntimeError(
                            "sql_validator requires publish_service: the legacy "
                            "in-memory writer cannot run without a materialized frame."
                        )
                    # Full SQL path: staging is never read back into pandas at all.
                    validation_report = self.sql_validator.validate(
                        staging.name, spec, source_count, len(rejected_records),
                        self.rejected_threshold,
                    )
                    set_snapshot_id = getattr(
                        self.dedup_service, "set_source_snapshot_id", None
                    )
                    if callable(set_snapshot_id):
                        set_snapshot_id(staging.name, resolved_run_id)
                    silver_frame = None
                else:
                    silver_frame = self.staging_reader(staging.name)
                    if not silver_frame.empty:
                        silver_frame["source_snapshot_id"] = resolved_run_id
                    validation_report = None
            else:
                if silver_chunks:
                    silver_frame = pd.concat(silver_chunks, ignore_index=True)
                else:
                    silver_frame = pd.DataFrame()
                try:
                    self._validate_detail_grain(silver_frame, spec)
                except SilverValidationError as exc:
                    self.staging_manager.mark_failed(staging.name)
                    results[spec.target_table] = {
                        "status": "FAILED",
                        "source_count": source_count,
                        "target_count": len(silver_frame),
                        "rows_read": source_count,
                        "rows_valid": source_count - len(rejected_records),
                        "rows_rejected": len(rejected_records),
                        "rows_deduplicated": 0,
                        "rows_published": 0,
                        "rejection_reasons": self._rejection_summaries(rejected_records),
                        "attempt_count": attempt_count,
                        "staging_name": staging.name,
                        "published": False,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                    apply_standard_result(
                        results[spec.target_table],
                        "FAILED",
                        0,
                        type(exc).__name__,
                        str(exc),
                    )
                    continue
                silver_frame, rows_deduplicated = self._global_deduplicate(
                    silver_frame, spec
                )
                if not silver_frame.empty:
                    silver_frame["source_snapshot_id"] = resolved_run_id
                self._rewrite_staged_content(silver_frame, staging.name)
                validation_report = None
            if validation_report is None:
                try:
                    self._validate_output_schema(silver_frame, spec)
                except SilverValidationError as exc:
                    self.staging_manager.mark_failed(staging.name)
                    results[spec.target_table] = {
                        "status": "FAILED",
                        "source_count": source_count,
                        "target_count": 0,
                        "rows_read": source_count,
                        "rows_valid": source_count - len(rejected_records),
                        "rows_rejected": len(rejected_records),
                        "rows_deduplicated": 0,
                        "rows_published": 0,
                        "rejection_reasons": self._rejection_summaries(rejected_records),
                        "attempt_count": attempt_count,
                        "staging_name": staging.name,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                    apply_standard_result(
                        results[spec.target_table],
                        "FAILED",
                        0,
                        type(exc).__name__,
                        str(exc),
                    )
                    continue
                validation_report = self.validator(
                    silver_frame,
                    spec,
                    source_count,
                    len(rejected_records),
                )
            if not validation_report.get("validation_passed", False):
                self.staging_manager.mark_failed(staging.name)
                results[spec.target_table] = {
                    "status": "FAILED",
                    "source_count": source_count,
                    "target_count": self._resolve_row_count(silver_frame, validation_report),
                    "rows_read": source_count,
                    "rows_valid": source_count - len(rejected_records),
                    "rows_rejected": len(rejected_records),
                    "rows_deduplicated": rows_deduplicated,
                    "rows_published": 0,
                    "rejection_reasons": self._rejection_summaries(rejected_records),
                    "attempt_count": attempt_count,
                    "staging_name": staging.name,
                    "validation_report": validation_report,
                    "published": False,
                    "error_type": "ValidationError",
                    "error_message": "; ".join(validation_report.get("issues", [])),
                }
                apply_standard_result(
                    results[spec.target_table],
                    "FAILED",
                    0,
                    "ValidationError",
                    "; ".join(validation_report.get("issues", [])),
                )
                continue

            try:
                self.staging_manager.mark_validated(
                    staging.name, validation_report
                )
                if self.publish_service is not None:
                    if candidate_schema is not None:
                        published_target = self.publish_service.publish(
                            spec.target_table, staging.name, validation_report,
                            candidate_schema=candidate_schema,
                        )
                    else:
                        published_target = self.publish_service.publish(
                            spec.target_table, staging.name, validation_report
                        )
                else:
                    self.writer(silver_frame, spec.target_table)
                    published_target = spec.target_name
                self.staging_manager.publish(spec.target_table, staging.name)
            except Exception as exc:
                self.staging_manager.mark_failed(staging.name)
                results[spec.target_table] = {
                    "status": "FAILED",
                    "source_count": source_count,
                    "target_count": self._resolve_row_count(silver_frame, validation_report),
                    "rows_read": source_count,
                    "rows_valid": source_count - len(rejected_records),
                    "rows_rejected": len(rejected_records),
                    "rows_deduplicated": rows_deduplicated,
                    "rows_published": 0,
                    "rejection_reasons": self._rejection_summaries(rejected_records),
                    "attempt_count": attempt_count,
                    "staging_name": staging.name,
                    "validation_report": validation_report,
                    "published": False,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
                apply_standard_result(
                    results[spec.target_table],
                    "FAILED",
                    0,
                    type(exc).__name__,
                    str(exc),
                )
                continue
            rows_rejected = len(rejected_records)
            rows_valid = source_count - rows_rejected
            rows_written = self._resolve_row_count(silver_frame, validation_report)
            results[spec.target_table] = {
                "status": "SUCCESS_WITH_REJECTIONS" if rows_rejected else "SUCCESS",
                "source_count": source_count,
                "target_count": rows_written,
                "rows_read": source_count,
                "rows_valid": rows_valid,
                "rows_rejected": rows_rejected,
                "rows_deduplicated": rows_deduplicated,
                "rows_published": rows_written,
                "rejection_reasons": self._rejection_summaries(rejected_records),
                "attempt_count": attempt_count,
                "staging_name": staging.name,
                "validation_report": validation_report,
                "published": True,
                "published_target": published_target,
            }
            apply_standard_result(
                results[spec.target_table],
                "SUCCESS_WITH_REJECTIONS" if rows_rejected else "SUCCESS",
                rows_written,
            )
        if candidate_schema is not None:
            successful = len(results) == len(self.table_specs) and all(
                item.get("status") in self.allowed_silver_statuses for item in results.values()
            )
            if successful:
                self.publish_service.finalize_snapshot(candidate_schema, resolved_run_id)
            else:
                self.publish_service.cleanup_snapshot(candidate_schema)
        return results


class SalesSilverJob(SilverTransformationJob):
    def __init__(self, settings: Settings | None = None, **kwargs: Any):
        super().__init__(
            table_specs=SALES_SILVER_TABLE_SPECS,
            settings=settings,
            **kwargs,
        )
        self.dependency_order = [
            "sales_order_header",
            "sales_order_detail",
            "customer",
            "sales_territory",
            "product",
            "sales_person",
        ]
