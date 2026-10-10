from datetime import datetime, timezone

import pandas as pd

from src.shared.ingestion.ingestion_models import (
    ExecutionIdentity,
    RejectedRecord,
    TableSpec,
)


class BronzeValidator:
    """Validate count parity and Bronze quality checks for sales extraction."""

    def validate(
        self, source_count: int, target_count: int, source_table: str, bronze_table: str
    ) -> bool:
        if source_count == target_count:
            return True
        raise ValueError(
            f"Validation failed for {source_table} -> {bronze_table}: "
            f"source={source_count}, target={target_count}"
        )

    def partition_rows(
        self,
        dataframe: pd.DataFrame,
        spec: TableSpec,
        identity: ExecutionIdentity,
    ) -> tuple[pd.DataFrame, tuple[RejectedRecord, ...]]:
        """Separate row-level primary-key failures from schema failures."""
        missing_columns = [
            column
            for column in spec.required_columns
            if column not in dataframe.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required columns for {spec.source_name}: {missing_columns}"
            )

        rejected_mask = dataframe[spec.primary_key].isna()
        rejected = []
        rejected_at = datetime.now(timezone.utc)
        for row_index, row in dataframe.loc[rejected_mask].iterrows():
            rejected.append(
                RejectedRecord(
                    run_id=identity.run_id,
                    load_id=identity.load_id,
                    batch_id=identity.batch_id,
                    source_table=spec.source_name,
                    record_key=str(row_index),
                    source_hash=self._optional_string(row.get("_record_hash")),
                    reason=f"NULL primary key: {spec.primary_key}",
                    rejected_at=rejected_at,
                )
            )

        return dataframe.loc[~rejected_mask].copy(), tuple(rejected)

    @staticmethod
    def _optional_string(value):
        return None if pd.isna(value) else str(value)

    def validate_staging(
        self,
        dataframe: pd.DataFrame,
        spec: TableSpec,
        source_count: int,
        rejected_count: int = 0,
        rejected_threshold=None,
    ) -> dict:
        """Validate a complete staging DataFrame before it can be published."""
        missing_columns = [
            column
            for column in spec.required_columns
            if column not in dataframe.columns
        ]
        required_lineage = [
            "_source_system",
            "_source_table",
            "_load_date",
            "_record_hash",
        ]
        missing_lineage = [
            column for column in required_lineage if column not in dataframe.columns
        ]
        source_values_ok = (
            "_source_table" in dataframe.columns
            and dataframe["_source_table"].eq(spec.source_name).all()
        )
        primary_key_nulls = (
            int(dataframe[spec.primary_key].isna().sum())
            if spec.primary_key in dataframe.columns
            else 0
        )
        duplicate_primary_keys = (
            int(dataframe[spec.primary_key].duplicated().sum())
            if spec.primary_key in dataframe.columns
            else 0
        )
        report = self.validate_table(
            source_count=source_count,
            target_count=len(dataframe),
            source_table=spec.source_name,
            bronze_table=spec.target_name,
            lineage_columns=list(dataframe.columns),
            critical_columns={spec.primary_key: 0},
            null_counts={spec.primary_key: primary_key_nulls},
            rejected_count=rejected_count,
            rejected_threshold=rejected_threshold,
        )
        report.update(
            {
                "missing_columns": missing_columns,
                "missing_lineage_columns": missing_lineage,
                "schema_ok": not missing_columns,
                "lineage_values_ok": source_values_ok,
                "duplicate_primary_keys": duplicate_primary_keys,
                "duplicate_primary_keys_ok": duplicate_primary_keys == 0,
            }
        )
        if missing_columns:
            report["issues"].append(
                f"Missing required columns in {spec.target_name}: {missing_columns}"
            )
        if missing_lineage:
            report["issues"].append(
                f"Missing lineage columns in {spec.target_name}: {missing_lineage}"
            )
        if not source_values_ok:
            report["issues"].append(
                f"Lineage source table mismatch in {spec.target_name}: "
                f"expected={spec.source_name}"
            )
        if duplicate_primary_keys:
            report["issues"].append(
                f"Duplicate primary keys in {spec.target_name}: "
                f"count={duplicate_primary_keys}"
            )
        report["validation_passed"] = (
            report["validation_passed"]
            and report["schema_ok"]
            and not missing_lineage
            and report["lineage_values_ok"]
            and report["duplicate_primary_keys_ok"]
        )
        return report

    def validate_table(
        self,
        source_count: int,
        target_count: int,
        source_table: str,
        bronze_table: str,
        lineage_columns=None,
        critical_columns=None,
        null_counts=None,
        rejected_count: int = 0,
        rejected_threshold=None,
    ) -> dict:
        """Run Bronze validation checks and return a report dictionary."""
        required_lineage = (
            lineage_columns
            if lineage_columns is not None
            else ["_source_system", "_source_table", "_load_date", "_record_hash"]
        )
        required_critical = critical_columns if critical_columns is not None else {}
        null_summary = null_counts if null_counts is not None else {}
        if rejected_count < 0:
            raise ValueError("rejected_count cannot be negative")
        if rejected_threshold is not None and rejected_threshold < 0:
            raise ValueError("rejected_threshold cannot be negative")

        issues = []
        count_match = source_count == target_count
        if not count_match:
            issues.append(
                f"Row count mismatch for {source_table} -> {bronze_table}: "
                f"source={source_count}, target={target_count}"
            )

        lineage_columns_ok = all(
            column in required_lineage
            for column in [
                "_source_system",
                "_source_table",
                "_load_date",
                "_record_hash",
            ]
        )
        if not lineage_columns_ok:
            issues.append(
                f"Missing lineage columns in {bronze_table}: expected metadata fields"
            )

        critical_columns_ok = True
        for column, allowed_nulls in required_critical.items():
            null_count = null_summary.get(column, 0)
            if null_count > allowed_nulls:
                critical_columns_ok = False
                issues.append(
                    f"Critical column {column} exceeds allowed NULL tolerance in {bronze_table}: "
                    f"null_count={null_count}, allowed={allowed_nulls}"
                )

        rejected_threshold_ok = (
            rejected_threshold is None or rejected_count <= rejected_threshold
        )
        if not rejected_threshold_ok:
            issues.append(
                f"Rejected row threshold exceeded for {source_table}: "
                f"rejected_count={rejected_count}, threshold={rejected_threshold}"
            )

        result = {
            "source_table": source_table,
            "bronze_table": bronze_table,
            "source_count": source_count,
            "target_count": target_count,
            "count_match": count_match,
            "lineage_columns_ok": lineage_columns_ok,
            "critical_columns_ok": critical_columns_ok,
            "rejected_count": rejected_count,
            "rejected_threshold": rejected_threshold,
            "rejected_threshold_ok": rejected_threshold_ok,
            "validation_passed": (
                count_match
                and lineage_columns_ok
                and critical_columns_ok
                and rejected_threshold_ok
            ),
            "issues": issues,
        }
        return result
