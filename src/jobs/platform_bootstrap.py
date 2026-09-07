from __future__ import annotations

from typing import Any, Callable

from src.core.settings import Settings, get_settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.ingestion.postgres_ingestion_schema import (
    PLATFORM_SCHEMA_VERSION,
    ensure_ingestion_schema,
)


SUPPORTED_SCHEMA_VERSIONS = {PLATFORM_SCHEMA_VERSION}
BOOTSTRAP_OBJECT_INVENTORY = (
    "bronze",
    "silver",
    "gold",
    "bronze_staging",
    "silver_staging",
    "bronze.pipeline_run_audit",
    "bronze.platform_schema_version",
)


class IncompatibleSchemaVersionError(RuntimeError):
    """Raised when the warehouse metadata version is not supported."""


class PlatformBootstrapJob:
    """Create platform metadata and verify the warehouse is ready for stages."""

    schema_version = PLATFORM_SCHEMA_VERSION

    def __init__(
        self,
        settings: Settings | None = None,
        schema_ensurer: Callable[[Settings], None] = ensure_ingestion_schema,
        connector_factory: Callable[..., Any] = PostgreSQLConnector,
    ):
        self.settings = settings or get_settings()
        self.schema_ensurer = schema_ensurer
        self.connector_factory = connector_factory

    def run(self):
        """Run idempotent DDL followed by non-mutating readiness checks."""
        try:
            migration = self.schema_ensurer(self.settings) or {}
            with self.connector_factory(settings=self.settings) as connection:
                object_rows = connection.fetch_results(
                    """
                    SELECT required.schema_name AS object_name, required.object_type, EXISTS (
                        SELECT 1
                        FROM information_schema.schemata
                        WHERE schema_name = required.schema_name
                    ) AS schema_exists
                    FROM (VALUES
                        ('bronze', 'schema'),
                        ('silver', 'schema'),
                        ('gold', 'schema'),
                        ('bronze_staging', 'schema'),
                        ('silver_staging', 'schema')
                    ) AS required(schema_name, object_type)
                    """
                )
                metadata_rows = connection.fetch_results(
                    """
                    SELECT object_name, object_type, EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'bronze'
                          AND table_name = required.table_name
                    ) AS object_exists
                    FROM (VALUES
                        ('pipeline_run_audit', 'table'),
                        ('platform_schema_version', 'table')
                    ) AS required(table_name, object_type)
                    """
                )
                version_rows = connection.fetch_results(
                    """
                    SELECT version
                    FROM bronze.platform_schema_version
                    WHERE component = 'platform'
                    """
                )
            checks = [
                {
                    "name": row[0],
                    "category": "schema",
                    "status": "ok" if row[2] else "failed",
                    "reason": "object exists" if row[2] else "object is missing",
                }
                for row in object_rows
            ]
            checks.extend(
                {
                    "name": row[0],
                    "category": "metadata" if row[0] != "platform_schema_version" else "version",
                    "status": "ok" if row[2] else "failed",
                    "reason": "object exists" if row[2] else "object is missing",
                }
                for row in metadata_rows
            )
            current_version = version_rows[0][0] if version_rows else None
            version_compatible = current_version in SUPPORTED_SCHEMA_VERSIONS
            checks.append(
                {
                    "name": "platform_schema_version",
                    "category": "version",
                    "status": "ok" if version_compatible else "failed",
                    "reason": (
                        f"compatible version {current_version}"
                        if version_compatible
                        else f"supported versions {sorted(SUPPORTED_SCHEMA_VERSIONS)}, found {current_version}"
                    ),
                }
            )
            readiness = {
                "phase": "post_bootstrap",
                "status": "ready" if all(item["status"] == "ok" for item in checks) else "failed",
                "checks": checks,
            }
            if readiness["status"] != "ready":
                return {
                    "status": "failed",
                    "ready": False,
                    "schema_version": current_version,
                    "migration": migration,
                    "readiness": readiness,
                    "error_type": (
                        "IncompatibleSchemaVersion"
                        if not version_compatible
                        else "BootstrapReadinessError"
                    ),
                    "message": "post-bootstrap readiness checks failed",
                }
            return {
                "status": "ok",
                "schema_version": self.schema_version,
                "migration": migration,
                "ready": True,
                "readiness": readiness,
                "message": "platform bootstrap completed",
            }
        except Exception as exc:
            return self._failed(f"bootstrap failed: {type(exc).__name__}: {exc}")

    @staticmethod
    def _failed(message: str) -> dict[str, object]:
        return {
            "status": "failed",
            "ready": False,
            "readiness": {
                "phase": "post_bootstrap",
                "status": "failed",
                "checks": [],
                "error_type": "BootstrapError",
                "reason": message,
            },
            "message": message,
        }
