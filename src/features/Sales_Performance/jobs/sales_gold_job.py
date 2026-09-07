"""Injectable orchestration contract for the Phase 4D Gold layer.

This module deliberately contains no database writes. Database-specific staging,
constraint, and publication behavior is supplied through injected dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from contextlib import contextmanager
from datetime import timedelta
import threading
from typing import Any, Callable, Mapping

import pandas as pd

from src.core.settings import Settings, get_settings
from src.shared.ingestion.ingestion_models import (
    ExecutionIdentity,
    IngestionResult,
    IngestionStatus,
    deterministic_batch_id,
    utc_now,
)


GOLD_TABLE_NAMES = (
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_territory",
    "dim_salesperson",
    "fact_sales",
)

FULL_READ_DIMENSION_TARGETS = frozenset(GOLD_TABLE_NAMES[:-1])
GOLD_METADATA_COLUMNS = ("created_at", "gold_version", "source_snapshot_id")
MEASURE_TOLERANCE = 1e-9
GOLD_SQL_TYPES = {
    "date_id": "INTEGER", "full_date": "DATE", "year_number": "SMALLINT",
    "quarter_number": "SMALLINT", "month_number": "SMALLINT", "month_name": "VARCHAR(20)",
    "day_number": "SMALLINT", "is_weekend": "BOOLEAN", "customer_id": "INTEGER",
    "customer_name": "VARCHAR(255)", "person_id": "INTEGER", "store_id": "INTEGER",
    "territory_id": "INTEGER", "account_number": "VARCHAR(50)", "product_id": "INTEGER",
    "product_name": "VARCHAR(255)", "product_number": "VARCHAR(50)", "product_line": "VARCHAR(2)",
    "product_class": "VARCHAR(2)", "product_style": "VARCHAR(2)", "list_price": "NUMERIC(19,4)",
    "standard_cost": "NUMERIC(19,4)", "is_discontinued": "BOOLEAN", "territory_name": "VARCHAR(255)",
    "country_region_code": "VARCHAR(10)", "territory_group": "VARCHAR(255)",
    "salesperson_id": "INTEGER", "business_entity_id": "INTEGER", "sales_quota": "NUMERIC(19,4)",
    "bonus": "NUMERIC(19,4)", "commission_pct": "NUMERIC(19,4)", "salesperson_name": "VARCHAR(255)",
    "sales_order_id": "INTEGER", "sales_order_detail_id": "INTEGER", "order_date_id": "INTEGER",
    "order_qty": "INTEGER", "unit_price": "NUMERIC(19,4)", "discount_amount": "NUMERIC(19,4)",
    "line_total": "NUMERIC(19,4)", "net_sales": "NUMERIC(19,4)",
    "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()", "gold_version": "VARCHAR(32) NOT NULL",
    "source_snapshot_id": "VARCHAR(128) NOT NULL",
}


@dataclass(frozen=True)
class FactBatch:
    """Stable-key fact batch identity and payload contract."""

    dataframe: pd.DataFrame
    batch_number: int
    lower_bound: Any
    upper_bound: Any
    batch_id: str
    content_hash: str

    def __post_init__(self) -> None:
        if self.batch_number < 1 or not self.batch_id or not self.content_hash:
            raise ValueError("fact batch identity is incomplete")
        if self.lower_bound is not None and self.upper_bound is not None:
            if self.lower_bound >= self.upper_bound:
                raise ValueError("fact batch bounds must be increasing")


def frame_content_hash(frame: pd.DataFrame) -> str:
    payload = frame.to_json(orient="records", date_format="iso", default_handler=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_fact_batch(
    details: pd.DataFrame,
    headers: pd.DataFrame,
    *,
    batch_number: int,
    lower_bound: Any,
    upper_bound: Any,
    source_snapshot_id: str,
) -> FactBatch:
    from scripts.warehouse.postgres.gold.sales_gold_load import build_fact_sales

    if "sales_order_detail_id" not in details.columns:
        raise ValueError("Fact batch is missing sales_order_detail_id")
    if details.empty:
        raise ValueError("Fact batch cannot be empty")
    ordered = details.sort_values("sales_order_detail_id", kind="mergesort").reset_index(drop=True)
    keys = ordered["sales_order_detail_id"]
    if lower_bound is not None and not (keys > lower_bound).all():
        raise ValueError("Fact batch contains keys outside its lower bound")
    if upper_bound is not None and not (keys <= upper_bound).all():
        raise ValueError("Fact batch contains keys outside its upper bound")
    batch_id = deterministic_batch_id(
        "sales_order_detail_clean", "sales_order_detail_id",
        lower_bound, upper_bound, source_snapshot_id,
    )
    return FactBatch(
        dataframe=build_fact_sales(ordered, headers),
        batch_number=batch_number,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        batch_id=batch_id,
        content_hash=frame_content_hash(ordered),
    )


def stable_key_batch_query(
    source_table: str,
    ordering_key: str,
    lower_bound: Any,
    upper_bound: Any,
) -> str:
    """Return the required half-open stable-key predicate for an injected reader."""
    safe_identifier = lambda value: all(
        part.replace("_", "").isalnum() and not part[0].isdigit()
        for part in value.split(".")
        if part
    )
    if not safe_identifier(source_table) or not safe_identifier(ordering_key):
        raise ValueError("source table and ordering key must be safe identifiers")
    lower = "NULL" if lower_bound is None else repr(lower_bound)
    upper = "NULL" if upper_bound is None else repr(upper_bound)
    lower_predicate = "" if lower_bound is None else f' AND "{ordering_key}" > {lower}'
    upper_predicate = "" if upper_bound is None else f' AND "{ordering_key}" <= {upper}'
    return (
        f'SELECT * FROM {source_table} WHERE 1=1{lower_predicate}{upper_predicate}'
        f' ORDER BY "{ordering_key}"'
    )


def validate_fact_frame(frame: pd.DataFrame) -> None:
    required = {
        "sales_order_detail_id", "sales_order_id", "order_date_id", "customer_id",
        "product_id", "territory_id", "salesperson_id", "order_qty", "unit_price",
        "discount_amount", "line_total", "net_sales",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"fact_sales is missing required columns: {missing}")
    if frame["sales_order_detail_id"].isna().any():
        raise ValueError("fact_sales key contains NULL values")
    if not frame["sales_order_detail_id"].is_unique:
        raise ValueError("fact_sales key is not unique")
    for column in ("order_qty", "unit_price", "discount_amount", "line_total", "net_sales"):
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not values.map(pd.api.types.is_number).all():
            raise ValueError(f"fact_sales.{column} contains non-numeric values")
        if not values.map(
            lambda value: pd.notna(value) and value >= -MEASURE_TOLERANCE
        ).all():
            raise ValueError(f"fact_sales.{column} contains negative values")


def _kpi_metrics(fact: pd.DataFrame) -> dict[str, float]:
    total_revenue = float(pd.to_numeric(fact["net_sales"], errors="coerce").sum())
    total_orders = float(fact["sales_order_id"].nunique())
    total_line_items = float(len(fact))
    total_units = float(pd.to_numeric(fact["order_qty"], errors="coerce").sum())
    gross_sales = float(
        (pd.to_numeric(fact["unit_price"], errors="coerce")
         * pd.to_numeric(fact["order_qty"], errors="coerce")).sum()
    )
    discount_amount = float(pd.to_numeric(fact["discount_amount"], errors="coerce").sum())
    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "total_line_items": total_line_items,
        "total_units": total_units,
        "average_order_value": total_revenue / total_orders if total_orders else 0.0,
        "average_item_price": gross_sales / total_units if total_units else 0.0,
        "discount_amount": discount_amount,
        "discount_rate": discount_amount / gross_sales if gross_sales else 0.0,
        "customer_count": float(fact["customer_id"].nunique()),
    }


def _relative_variance(actual: float, expected: float) -> float:
    if expected == 0:
        return 0.0 if actual == 0 else 1.0
    return abs(actual - expected) / abs(expected)


class GoldIntegrityValidator:
    """Validate candidate Gold frames before constraints or publication."""

    def __init__(
        self,
        *,
        silver_kpi_baseline: Mapping[str, float] | None = None,
        kpi_tolerance: float = 0.02,
    ):
        if kpi_tolerance < 0:
            raise ValueError("kpi_tolerance cannot be negative")
        self.silver_kpi_baseline = dict(silver_kpi_baseline or {})
        self.kpi_tolerance = kpi_tolerance

    def __call__(
        self,
        frames: Mapping[str, pd.DataFrame],
        specs: tuple[GoldTableSpec, ...],
        identity: GoldExecutionIdentity,
    ) -> dict[str, Any]:
        issues: list[str] = []
        duplicate_counts: dict[str, int] = {}
        orphan_counts: dict[str, int] = {}
        for spec in specs:
            frame = frames.get(spec.target_table)
            if frame is None:
                issues.append(f"missing candidate table: {spec.target_table}")
                continue
            try:
                if spec.build_strategy == "full_read":
                    validate_dimension_frame(frame, spec)
                else:
                    validate_fact_frame(frame)
            except ValueError as exc:
                issues.append(str(exc))
                if spec.primary_key in frame.columns:
                    duplicate_counts[spec.target_table] = int(frame[spec.primary_key].duplicated().sum())

        fact = frames.get("fact_sales")
        if fact is not None:
            for column, (dimension, key) in GOLD_TABLE_SPECS[-1].foreign_keys.items():
                if column not in fact.columns or dimension not in frames:
                    continue
                values = fact[column].dropna()
                known = frames[dimension][key].dropna()
                count = int((~values.isin(known)).sum())
                if count:
                    orphan_counts[column] = count
                    issues.append(f"orphan references in fact_sales.{column}: {count}")
            required_non_null = [
                "sales_order_id", "order_date_id", "customer_id", "product_id",
                "territory_id", "order_qty", "unit_price", "line_total", "net_sales",
            ]
            for column in required_non_null:
                if fact[column].isna().any():
                    issues.append(f"fact_sales.{column} contains required NULL values")
            expected_discount = (
                pd.to_numeric(fact["order_qty"], errors="coerce")
                * pd.to_numeric(fact["unit_price"], errors="coerce")
                - pd.to_numeric(fact["line_total"], errors="coerce")
            ).round(4)
            actual_discount = pd.to_numeric(
                fact["discount_amount"], errors="coerce"
            )
            if not expected_discount.sub(actual_discount).abs().le(
                MEASURE_TOLERANCE
            ).all():
                issues.append("fact_sales.discount_amount does not match approved formula")
            if not expected_discount.ge(-MEASURE_TOLERANCE).all():
                issues.append("fact_sales.discount_amount contains negative values")
            if not pd.to_numeric(fact["net_sales"], errors="coerce").eq(
                pd.to_numeric(fact["line_total"], errors="coerce")
            ).all():
                issues.append("fact_sales.net_sales does not match line_total")

        kpi_report = self._validate_kpis(fact) if fact is not None else {
            "kpi_passed": False, "comparisons": {}, "issues": ["fact_sales is missing"]
        }
        issues.extend(kpi_report.get("issues", []))
        return {
            "validation_passed": not issues,
            "issues": issues,
            "duplicate_counts": duplicate_counts,
            "orphan_counts": orphan_counts,
            "kpi_passed": kpi_report["kpi_passed"],
            "kpi_report": kpi_report,
            "gold_run_id": identity.gold_run_id,
            "source_snapshot_id": identity.source_snapshot_id,
        }

    def _validate_kpis(self, fact: pd.DataFrame) -> dict[str, Any]:
        actual = _kpi_metrics(fact)
        if not self.silver_kpi_baseline:
            return {"kpi_passed": True, "comparisons": {}, "issues": []}
        comparisons = {}
        issues = []
        for name, baseline in self.silver_kpi_baseline.items():
            variance = _relative_variance(actual.get(name, 0.0), float(baseline))
            passed = variance <= self.kpi_tolerance
            comparisons[name] = {
                "candidate": actual.get(name, 0.0),
                "baseline": float(baseline),
                "variance_pct": variance * 100,
                "within_tolerance": passed,
            }
            if not passed:
                issues.append(f"KPI mismatch for {name}: variance={variance:.6f}")
        return {"kpi_passed": not issues, "comparisons": comparisons, "issues": issues}

SILVER_TARGETS = (
    "sales_order_header_clean",
    "sales_order_detail_clean",
    "customer_clean",
    "product_clean",
    "sales_territory_clean",
    "sales_person_clean",
)


@dataclass(frozen=True)
class GoldTableSpec:
    """Immutable Gold table contract used by the job and its collaborators."""

    source_table: str
    target_table: str
    primary_key: str
    required_columns: tuple[str, ...]
    expected_types: Mapping[str, str] = field(default_factory=dict)
    foreign_keys: Mapping[str, tuple[str, str]] = field(default_factory=dict)
    build_strategy: str = "full_read"

    def __post_init__(self) -> None:
        if self.target_table not in GOLD_TABLE_NAMES:
            raise ValueError(f"Unsupported Gold target: {self.target_table}")
        if not self.required_columns or self.primary_key not in self.required_columns:
            raise ValueError("primary_key must be included in required_columns")
        if self.source_table not in SILVER_TARGETS:
            raise ValueError(f"Unsupported Silver source: {self.source_table}")
        if self.build_strategy not in {"full_read", "stable_key_batch"}:
            raise ValueError(f"Unsupported Gold build strategy: {self.build_strategy}")


class GoldConstraintError(ValueError):
    """Raised when a Gold candidate cannot satisfy its staging contract."""


class GoldPublicationError(RuntimeError):
    """Raised when a candidate cannot be promoted without changing the pointer."""


class GoldRunAlreadyActive(RuntimeError):
    """Raised when a second Gold run attempts to enter the publication lifecycle."""


class UnknownCommitError(TimeoutError):
    """The client lost the outcome of an atomic write and must reconcile first."""


class GoldConstraintManager:
    """Build and verify Gold constraints before a candidate can be published.

    The manager is database-independent. A PostgreSQL adapter can execute the
    returned DDL later, but no published table is touched by this contract.
    """

    def create_candidate_schema(
        self,
        frames: Mapping[str, pd.DataFrame],
        identity: GoldExecutionIdentity,
        specs: tuple[GoldTableSpec, ...],
        *,
        gold_version: str = "v001",
    ) -> dict[str, Any]:
        candidate_schema = f"gold_{gold_version}"
        tables: dict[str, dict[str, Any]] = {}
        candidate_frames: dict[str, pd.DataFrame] = {}
        for spec in specs:
            frame = frames.get(spec.target_table)
            if frame is None:
                raise GoldConstraintError(f"candidate is missing table: {spec.target_table}")
            candidate_frame = frame.copy()
            candidate_frame["created_at"] = utc_now()
            candidate_frame["gold_version"] = gold_version
            candidate_frame["source_snapshot_id"] = identity.source_snapshot_id
            candidate_frames[spec.target_table] = candidate_frame
            columns = tuple(candidate_frame.columns)
            tables[spec.target_table] = {
                "columns": columns,
                "required_metadata": GOLD_METADATA_COLUMNS,
                "primary_key": (spec.primary_key,),
                "foreign_keys": dict(spec.foreign_keys),
                "rows": len(candidate_frame),
                "ddl": self.ddl_for_table(candidate_schema, spec),
            }
        candidate = {
            "candidate_schema": candidate_schema,
            "gold_version": gold_version,
            "source_snapshot_id": identity.source_snapshot_id,
            "staging_identity": identity.gold_load_id,
            "tables": tables,
            "frames": candidate_frames,
        }
        report = self.verify(candidate, specs)
        if not report["constraints_verified"]:
            raise GoldConstraintError("; ".join(report["issues"]))
        candidate["constraint_report"] = report
        return candidate

    def verify(
        self,
        candidate: Mapping[str, Any],
        specs: tuple[GoldTableSpec, ...],
    ) -> dict[str, Any]:
        candidate = candidate.get("candidate", candidate)
        issues: list[str] = []
        primary_keys: dict[str, tuple[str, ...]] = {}
        foreign_keys: dict[str, dict[str, tuple[str, str]]] = {}
        tables = candidate.get("tables", {})
        frames = candidate.get("frames", {})
        for spec in specs:
            table = tables.get(spec.target_table)
            if table is None:
                issues.append(f"candidate metadata missing table: {spec.target_table}")
                continue
            columns = set(table.get("columns", ()))
            if spec.target_table in frames:
                columns.update(frames[spec.target_table].columns)
            missing = set(spec.required_columns) - columns
            if missing:
                issues.append(f"{spec.target_table} missing columns: {sorted(missing)}")
            metadata = set(table.get("required_metadata", GOLD_METADATA_COLUMNS))
            if not set(GOLD_METADATA_COLUMNS).issubset(metadata):
                issues.append(f"{spec.target_table} missing Gold metadata contract")
            if spec.primary_key not in columns:
                issues.append(f"{spec.target_table} missing primary key column: {spec.primary_key}")
            primary_keys[spec.target_table] = (spec.primary_key,)
            foreign_keys[spec.target_table] = dict(spec.foreign_keys)
            declared_fks = table.get("foreign_keys", spec.foreign_keys)
            if declared_fks != dict(spec.foreign_keys):
                issues.append(f"{spec.target_table} foreign-key declaration mismatch")
        if candidate.get("source_snapshot_id") in (None, ""):
            issues.append("candidate source_snapshot_id is required")
        if candidate.get("gold_version") in (None, ""):
            issues.append("candidate gold_version is required")
        return {
            "constraints_verified": not issues,
            "issues": issues,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "metadata_columns": GOLD_METADATA_COLUMNS,
            "candidate_schema": candidate.get("candidate_schema"),
        }

    @staticmethod
    def ddl_for_table(candidate_schema: str, spec: GoldTableSpec) -> tuple[str, ...]:
        qualified = f'"{candidate_schema}"."{spec.target_table}"'
        columns = [
            f'"{column}" {GOLD_SQL_TYPES[column]}'
            for column in (*spec.required_columns, *GOLD_METADATA_COLUMNS)
        ]
        statements = [
            f"CREATE TABLE IF NOT EXISTS {qualified} ({', '.join(columns)})",
            f"ALTER TABLE {qualified} ADD CONSTRAINT "
            f'"pk_{spec.target_table}" PRIMARY KEY ("{spec.primary_key}")',
        ]
        for column, (target_table, target_column) in spec.foreign_keys.items():
            statements.append(
                f"ALTER TABLE {qualified} ADD CONSTRAINT "
                f'"fk_{spec.target_table}_{column}" FOREIGN KEY ("{column}") '
                f'REFERENCES "{candidate_schema}"."{target_table}" ("{target_column}")'
            )
        return tuple(statements)


class GoldPublishService:
    """Versioned Gold candidate publisher with an atomic current pointer.

    This implementation is intentionally database-independent. It models the
    transaction boundary and lifecycle rules that a PostgreSQL adapter must
    preserve: candidates are validated first, then the pointer changes once.
    """

    def __init__(
        self,
        constraint_manager: GoldConstraintManager,
        *,
        retention: timedelta | None = None,
    ):
        self.constraint_manager = constraint_manager
        self.retention = retention if retention is not None else timedelta(hours=48)
        self._lock = threading.RLock()
        self._active_run_id: str | None = None
        self._version_number = 0
        self._current_version: str | None = None
        self._versions: dict[str, dict[str, Any]] = {}
        self._candidates: dict[str, dict[str, Any]] = {}

    @contextmanager
    def active_run(self, run_id: str):
        self.start_run(run_id)
        try:
            yield
        finally:
            self.finish_run(run_id)

    def start_run(self, run_id: str) -> None:
        with self._lock:
            if self._active_run_id is not None and self._active_run_id != run_id:
                raise GoldRunAlreadyActive(
                    f"Gold run already active: {self._active_run_id}"
                )
            self._active_run_id = run_id

    def finish_run(self, run_id: str) -> None:
        with self._lock:
            if self._active_run_id == run_id:
                self._active_run_id = None

    @property
    def current_version(self) -> str | None:
        with self._lock:
            return self._current_version

    def prepare(
        self,
        frames: Mapping[str, pd.DataFrame],
        identity: GoldExecutionIdentity,
        specs: tuple[GoldTableSpec, ...],
    ) -> dict[str, Any]:
        with self._lock:
            self._version_number += 1
            version = f"v{self._version_number:03d}"
            candidate = self.constraint_manager.create_candidate_schema(
                frames, identity, specs, gold_version=version
            )
            candidate.update({
                "gold_run_id": identity.gold_run_id,
                "lifecycle": "ACTIVE",
                "created_at": utc_now(),
            })
            self._candidates[version] = candidate
            return {
                "candidate_schema": candidate["candidate_schema"],
                "staging_identity": candidate["staging_identity"],
                "gold_version": version,
                "candidate": candidate,
            }

    def publish(self, prepared: Mapping[str, Any], identity: GoldExecutionIdentity) -> dict[str, Any]:
        version = prepared.get("gold_version")
        with self._lock:
            candidate = self._candidates.get(version)
            if candidate is None or candidate is not prepared.get("candidate"):
                raise GoldPublicationError("candidate is not owned by this publisher")
            if candidate.get("gold_run_id") != identity.gold_run_id:
                raise GoldPublicationError("candidate identity does not match Gold run")
            report = self.constraint_manager.verify(candidate, GOLD_TABLE_SPECS)
            if not report["constraints_verified"]:
                raise GoldPublicationError("candidate constraints are not verified")
            previous = self._current_version
            published = dict(candidate)
            published.update({
                "lifecycle": "PUBLISHED",
                "published_at": utc_now(),
                "previous_version": previous,
            })
            # Pointer and version become visible together under the same lock.
            self._versions[version] = published
            self._current_version = version
            candidate["lifecycle"] = "PUBLISHED"
            return {
                "gold_version": version,
                "candidate_schema": candidate["candidate_schema"],
                "previous_version": previous,
                "kpi_passed": True,
                "published": True,
            }

    def mark_failed(self, prepared: Mapping[str, Any]) -> None:
        version = prepared.get("gold_version")
        with self._lock:
            candidate = self._candidates.get(version)
            if candidate is not None and candidate.get("lifecycle") != "PUBLISHED":
                candidate["lifecycle"] = "FAILED"
                candidate["failed_at"] = utc_now()

    def cleanup(self, now: datetime | None = None) -> tuple[str, ...]:
        current = now or utc_now()
        removed: list[str] = []
        with self._lock:
            previous_versions = sorted(self._versions)[-2:]
            for version, candidate in list(self._candidates.items()):
                if candidate.get("lifecycle") != "FAILED":
                    continue
                failed_at = candidate.get("failed_at")
                if failed_at and failed_at <= current - self.retention:
                    del self._candidates[version]
                    removed.append(version)
            # Keep the current and at least one previous published version.
            for version in sorted(self._versions):
                if version in previous_versions or version == self._current_version:
                    continue
                del self._versions[version]
                removed.append(version)
        return tuple(sorted(set(removed)))

    def execute_with_reconciliation(
        self,
        operation: Callable[[], Any],
        *,
        reconciliation: Any,
        staging_name: str,
        batch_id: str,
        content_hash: str,
        policy: Any,
        sleeper: Callable[[float], None],
    ) -> tuple[Any, int, str]:
        """Retry only transient failures and reconcile unknown commits first."""
        from src.shared.ingestion.retry_policy import execute_with_retry

        def guarded_operation():
            try:
                return operation()
            except UnknownCommitError:
                try:
                    decision = reconciliation.resolve(staging_name, batch_id, content_hash)
                except TypeError:
                    decision = reconciliation.resolve(
                        batch_id=batch_id,
                        content_hash=content_hash,
                        candidate_schema=staging_name,
                    )
                if decision == "SKIP":
                    return {"reconciled": True, "batch_id": batch_id}
                if decision == "RETRY":
                    raise TimeoutError("unknown commit reconciled as retry")
                raise

        result, attempts, status = execute_with_retry(
            guarded_operation, policy, sleeper
        )
        return result, attempts, status.value


def validate_dimension_frame(frame: pd.DataFrame, spec: GoldTableSpec) -> None:
    """Fail closed when a full-read dimension violates its target contract."""
    if spec.build_strategy != "full_read":
        return
    missing = sorted(set(spec.required_columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{spec.target_table} is missing required columns: {missing}")
    if set(frame.columns) != set(spec.required_columns):
        raise ValueError(
            f"{spec.target_table} output columns do not match the Gold contract"
        )
    if frame[spec.primary_key].isna().any():
        raise ValueError(f"{spec.target_table} key contains NULL values")
    if not frame[spec.primary_key].is_unique:
        raise ValueError(f"{spec.target_table} key is not unique")
    for column, expected_type in spec.expected_types.items():
        series = frame[column]
        values = series.dropna()
        if expected_type == "integer":
            numeric = pd.to_numeric(values, errors="coerce")
            valid = numeric.notna().all() and (numeric % 1 == 0).all()
        elif expected_type == "number":
            valid = pd.to_numeric(values, errors="coerce").notna().all()
        elif expected_type == "boolean":
            valid = values.map(lambda value: isinstance(value, bool)).all()
        elif expected_type == "string":
            valid = values.map(lambda value: isinstance(value, str)).all()
        elif expected_type == "date":
            valid = pd.api.types.is_datetime64_any_dtype(series) or values.map(
                lambda value: hasattr(value, "year") and hasattr(value, "month")
            ).all()
        else:
            valid = False
        if not valid:
            raise ValueError(
                f"{spec.target_table}.{column} does not match expected type {expected_type}"
            )


GOLD_TABLE_SPECS = (
    GoldTableSpec(
        "sales_order_header_clean", "dim_date", "date_id",
        ("date_id", "full_date", "year_number", "quarter_number", "month_number", "month_name", "day_number", "is_weekend"),
        expected_types={
            "date_id": "integer", "full_date": "date", "year_number": "integer",
            "quarter_number": "integer", "month_number": "integer", "month_name": "string",
            "day_number": "integer", "is_weekend": "boolean",
        },
    ),
    GoldTableSpec(
        "customer_clean", "dim_customer", "customer_id",
        ("customer_id", "customer_name", "person_id", "store_id", "territory_id", "account_number"),
        expected_types={
            "customer_id": "integer", "customer_name": "string", "person_id": "integer",
            "store_id": "integer", "territory_id": "integer", "account_number": "string",
        },
    ),
    GoldTableSpec(
        "product_clean", "dim_product", "product_id",
        ("product_id", "product_name", "product_number", "product_line", "product_class", "product_style", "list_price", "standard_cost", "is_discontinued"),
        expected_types={
            "product_id": "integer", "product_name": "string", "product_number": "string",
            "product_line": "string", "product_class": "string", "product_style": "string",
            "list_price": "number", "standard_cost": "number", "is_discontinued": "boolean",
        },
    ),
    GoldTableSpec(
        "sales_territory_clean", "dim_territory", "territory_id",
        ("territory_id", "territory_name", "country_region_code", "territory_group"),
        expected_types={
            "territory_id": "integer", "territory_name": "string",
            "country_region_code": "string", "territory_group": "string",
        },
    ),
    GoldTableSpec(
        "sales_person_clean", "dim_salesperson", "salesperson_id",
        ("salesperson_id", "business_entity_id", "territory_id", "sales_quota", "bonus", "commission_pct", "salesperson_name"),
        expected_types={
            "salesperson_id": "integer", "business_entity_id": "integer", "territory_id": "integer",
            "sales_quota": "number", "bonus": "number", "commission_pct": "number",
            "salesperson_name": "string",
        },
    ),
    GoldTableSpec(
        "sales_order_detail_clean", "fact_sales", "sales_order_detail_id",
        ("sales_order_detail_id", "sales_order_id", "order_date_id", "customer_id", "product_id", "territory_id", "salesperson_id", "order_qty", "unit_price", "discount_amount", "line_total", "net_sales"),
        foreign_keys={
            "order_date_id": ("dim_date", "date_id"),
            "customer_id": ("dim_customer", "customer_id"),
            "product_id": ("dim_product", "product_id"),
            "territory_id": ("dim_territory", "territory_id"),
            "salesperson_id": ("dim_salesperson", "salesperson_id"),
        },
        build_strategy="stable_key_batch",
    ),
)


class SilverSnapshotError(ValueError):
    """Raised when Gold is given an incomplete or inconsistent Silver snapshot."""


class SilverSnapshotGate:
    """Validate Silver publication metadata without applying business rules."""

    required_targets = frozenset(SILVER_TARGETS)

    def validate(
        self,
        silver_result: Mapping[str, Any],
        source_snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(silver_result, Mapping):
            raise SilverSnapshotError("Silver result must be a mapping")
        snapshot_id = source_snapshot_id or silver_result.get("source_snapshot_id") or silver_result.get("snapshot_id")
        table_results = silver_result.get("silver", silver_result)
        if not isinstance(table_results, Mapping):
            raise SilverSnapshotError("Silver table results must be a mapping")
        missing = sorted(self.required_targets - set(table_results))
        if missing:
            raise SilverSnapshotError(f"Silver snapshot is missing targets: {missing}")
        if not snapshot_id:
            raise SilverSnapshotError("Silver source_snapshot_id is required")
        for target in self.required_targets:
            result = table_results[target]
            if not isinstance(result, Mapping):
                raise SilverSnapshotError(f"Silver result for {target} is invalid")
            if result.get("status") not in {"SUCCESS", "SUCCESS_WITH_REJECTIONS", IngestionStatus.SUCCESS.value}:
                raise SilverSnapshotError(f"Silver target {target} is not successful")
            if result.get("published") is not True:
                raise SilverSnapshotError(f"Silver target {target} is not published")
            identity = result.get("source_snapshot_id", result.get("snapshot_id", result.get("run_id")))
            if identity != snapshot_id:
                raise SilverSnapshotError(
                    f"Silver target {target} has inconsistent source snapshot: {identity!r}"
                )
        return {"status": "SUCCESS", "source_snapshot_id": str(snapshot_id), "target_count": len(table_results)}


@dataclass(frozen=True)
class GoldExecutionIdentity(ExecutionIdentity):
    """Gold identity while retaining the shared execution identity fields."""

    pipeline_snapshot_id: str = ""
    source_snapshot_id: str = ""

    @classmethod
    def create(cls, pipeline_snapshot_id: str, source_snapshot_id: str) -> "GoldExecutionIdentity":
        base = ExecutionIdentity.create()
        return cls(base.run_id, base.load_id, base.batch_id, pipeline_snapshot_id, source_snapshot_id)

    @property
    def gold_run_id(self) -> str:
        return self.run_id

    @property
    def gold_load_id(self) -> str:
        return self.load_id


@dataclass
class GoldTableResult(IngestionResult):
    identity: GoldExecutionIdentity
    gold_version: str | None = None
    candidate_schema: str | None = None
    staging_identity: str | None = None
    published: bool = False
    validation_passed: bool = False
    constraints_verified: bool = False
    kpi_passed: bool = False
    previous_version: str | None = None

    @property
    def pipeline_snapshot_id(self) -> str:
        return self.identity.pipeline_snapshot_id

    @property
    def source_snapshot_id(self) -> str:
        return self.identity.source_snapshot_id

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "pipeline_snapshot_id": self.pipeline_snapshot_id,
                "source_snapshot_id": self.source_snapshot_id,
                "gold_run_id": self.identity.gold_run_id,
                "gold_load_id": self.identity.gold_load_id,
                "gold_version": self.gold_version,
                "candidate_schema": self.candidate_schema,
                "staging_identity": self.staging_identity,
                "published": self.published,
                "validation_passed": self.validation_passed,
                "constraints_verified": self.constraints_verified,
                "kpi_passed": self.kpi_passed,
                "previous_version": self.previous_version,
            }
        )
        return result


@dataclass
class GoldRunResult:
    identity: GoldExecutionIdentity
    status: IngestionStatus
    table_results: dict[str, GoldTableResult]
    gold_version: str | None = None
    candidate_schema: str | None = None
    published: bool = False
    validation_passed: bool = False
    constraints_verified: bool = False
    kpi_passed: bool = False
    previous_version: str | None = None
    current_pointer: dict[str, Any] | None = None
    error_type: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "pipeline_snapshot_id": self.identity.pipeline_snapshot_id,
            "source_snapshot_id": self.identity.source_snapshot_id,
            "gold_run_id": self.identity.gold_run_id,
            "gold_load_id": self.identity.gold_load_id,
            "gold_version": self.gold_version,
            "candidate_schema": self.candidate_schema,
            "published": self.published,
            "validation_passed": self.validation_passed,
            "constraints_verified": self.constraints_verified,
            "kpi_passed": self.kpi_passed,
            "previous_version": self.previous_version,
            "current_pointer": self.current_pointer,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "tables": {name: result.to_dict() for name, result in self.table_results.items()},
        }


class SalesGoldJob:
    """Coordinate Gold build dependencies; no dependency is created implicitly."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        reader: Callable[..., pd.DataFrame],
        builders: Mapping[str, Callable[..., pd.DataFrame]],
        validator: Callable[..., Mapping[str, Any]] | None = None,
        constraint_manager: Any,
        publisher: Any,
        audit: Any | None = None,
        snapshot_gate: SilverSnapshotGate | None = None,
        fact_batch_reader: Callable[..., Any] | None = None,
        fact_batch_writer: Callable[..., Any] | None = None,
        checkpoint_manager: Any | None = None,
    ):
        self.settings = settings or get_settings()
        self.reader = reader
        self.builders = dict(builders)
        self.validator = validator or GoldIntegrityValidator()
        self.constraint_manager = constraint_manager
        self.publisher = publisher
        self.audit = audit
        self.snapshot_gate = snapshot_gate or SilverSnapshotGate()
        self.fact_batch_reader = fact_batch_reader
        self.fact_batch_writer = fact_batch_writer
        self.checkpoint_manager = checkpoint_manager

    def run(
        self,
        *,
        pipeline_snapshot_id: str,
        silver_result: Mapping[str, Any],
    ) -> dict[str, Any]:
        gate = self.snapshot_gate.validate(silver_result)
        source_snapshot_id = gate["source_snapshot_id"]
        identity = GoldExecutionIdentity.create(pipeline_snapshot_id, source_snapshot_id)
        table_results: dict[str, GoldTableResult] = {}
        started_at = utc_now()
        prepared = None
        run_started = hasattr(self.publisher, "start_run")
        if run_started:
            self.publisher.start_run(identity.gold_run_id)
        try:
            frames, fact_batches = self._build_frames(source_snapshot_id, identity)
            report = dict(self.validator(frames, GOLD_TABLE_SPECS, identity))
            if not report.get("validation_passed", False):
                raise ValueError(report.get("error_message", "Gold validation failed"))
            prepared = self.publisher.prepare(frames, identity, GOLD_TABLE_SPECS)
            constraints = self.constraint_manager.verify(prepared, GOLD_TABLE_SPECS)
            if not constraints.get("constraints_verified", False):
                raise ValueError(constraints.get("error_message", "Gold constraints failed"))
            publication = self._publish_candidate(
                prepared, identity, report, constraints, fact_batches
            )
            for spec in GOLD_TABLE_SPECS:
                table_results[spec.target_table] = self._table_result(
                    spec, identity, started_at, len(frames[spec.target_table]),
                    status=IngestionStatus.SUCCESS,
                    candidate=prepared, report=report, constraints=constraints,
                    publication=publication,
                )
            result = GoldRunResult(
                identity, IngestionStatus.SUCCESS, table_results,
                gold_version=publication.get("gold_version"),
                candidate_schema=publication.get("candidate_schema"),
                published=True, validation_passed=True,
                constraints_verified=True,
                kpi_passed=report.get("kpi_passed", publication.get("kpi_passed", False)),
                previous_version=publication.get("previous_version"),
                current_pointer=publication.get("current_pointer"),
            )
        except Exception as exc:
            if prepared is not None and hasattr(self.publisher, "mark_failed"):
                self.publisher.mark_failed(prepared)
            result = GoldRunResult(
                identity, IngestionStatus.FAILED, table_results,
                error_type=type(exc).__name__, error_message=str(exc),
            )
        finally:
            if run_started:
                self.publisher.finish_run(identity.gold_run_id)
        if self.audit is not None and hasattr(self.audit, "record"):
            self.audit.record(result)
        return result.to_dict()

    def _build_frames(
        self,
        source_snapshot_id: str,
        identity: GoldExecutionIdentity,
    ) -> tuple[dict[str, pd.DataFrame], tuple[FactBatch, ...]]:
        frames: dict[str, pd.DataFrame] = {}
        fact_batches: list[FactBatch] = []
        for spec in GOLD_TABLE_SPECS:
            if spec.target_table == "fact_sales":
                if self.fact_batch_reader is None:
                    raise ValueError(
                        "fact_batch_reader is required; fact_sales cannot use a full-table read"
                    )
                for batch in self.fact_batch_reader(
                    source_snapshot_id=source_snapshot_id,
                    batch_size=getattr(self.settings, "batch_size", 10000),
                ):
                    if not isinstance(batch, FactBatch):
                        raise TypeError("fact_batch_reader must yield FactBatch values")
                    validate_fact_frame(batch.dataframe)
                    if self.fact_batch_writer is not None:
                        self.fact_batch_writer(batch, identity)
                    if self.checkpoint_manager is not None:
                        self.checkpoint_manager.mark_committed(batch.batch_id)
                        self.checkpoint_manager.advance(batch.batch_id, batch.upper_bound)
                    fact_batches.append(batch)
                if not fact_batches:
                    raise ValueError("fact_batch_reader returned no batches")
                frames[spec.target_table] = pd.concat(
                    [batch.dataframe for batch in fact_batches], ignore_index=True
                )
                continue
            frame = self.reader(spec.source_table, source_snapshot_id=source_snapshot_id)
            built = self.builders[spec.target_table](frame)
            validate_dimension_frame(built, spec)
            frames[spec.target_table] = built
        return frames, tuple(fact_batches)

    def _publish_candidate(self, prepared, identity, report, constraints, fact_batches):
        counts = {
            "fact_rows": sum(len(batch.dataframe) for batch in fact_batches),
            "dimension_rows": sum(
                len(frame)
                for name, frame in prepared.get("frames", {}).items()
                if name != "fact_sales"
            ),
            "rows_rejected": report.get("rows_rejected", 0),
        }
        counts["rows_written"] = counts["dimension_rows"] + counts["fact_rows"]
        counts["rows_read"] = counts["rows_written"]
        kwargs = {
            "gold_run_id": identity.gold_run_id,
            "source_snapshot_id": identity.source_snapshot_id,
            "validation_report": report,
            "constraint_report": constraints,
            "kpi_report": report.get("kpi_report", {}),
            "counts": counts,
        }
        try:
            return self.publisher.publish(prepared, **kwargs)
        except TypeError as exc:
            if "unexpected keyword" not in str(exc):
                raise
            return self.publisher.publish(prepared, identity)

    @staticmethod
    def _table_result(spec, identity, started_at, rows_written, *, status, candidate, report, constraints, publication):
        return GoldTableResult(
            identity=identity,
            stage="gold",
            source_table=spec.source_table,
            target_table=spec.target_table,
            status=status,
            rows_read=rows_written,
            rows_written=rows_written,
            started_at=started_at,
            finished_at=utc_now(),
            gold_version=publication.get("gold_version"),
            candidate_schema=candidate.get("candidate_schema"),
            staging_identity=candidate.get("staging_identity"),
            published=True,
            validation_passed=report.get("validation_passed", False),
            constraints_verified=constraints.get("constraints_verified", False),
            kpi_passed=report.get("kpi_passed", publication.get("kpi_passed", False)),
            previous_version=publication.get("previous_version"),
        )
