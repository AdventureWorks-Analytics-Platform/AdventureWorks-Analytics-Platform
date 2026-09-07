from typing import Any, Dict, List

from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.connectors.sql_server_connector import SQLServerConnector
from src.core.settings import Settings, get_settings


class ConnectionHealthService:
    """Service for checking whether configured connections are healthy."""

    def __init__(
        self,
        settings: Settings | None = None,
        sql_connector_factory=SQLServerConnector,
        postgres_connector_factory=PostgreSQLConnector,
    ):
        self.settings = settings or get_settings()
        self.sql_connector_factory = sql_connector_factory
        self.postgres_connector_factory = postgres_connector_factory

    def _check_connector(self, name: str, connector: Any) -> Dict[str, Any]:
        try:
            connected = connector.connect()
            if connected:
                return {
                    "name": name,
                    "category": "connectivity",
                    "status": "ok",
                    "reason": "connection successful",
                }
            return {
                "name": name,
                "category": "connectivity",
                "status": "failed",
                "reason": "connection failed",
            }
        except Exception as exc:  # pragma: no cover - defensive branch
            return {
                "name": name,
                "category": "connectivity",
                "status": "failed",
                "reason": f"connection error: {type(exc).__name__}",
            }
        finally:
            connector.disconnect()

    def check_all(self) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = [
            self._check_connector(
                "sql_server", self.sql_connector_factory(settings=self.settings)
            ),
            self._check_connector(
                "postgres", self.postgres_connector_factory(settings=self.settings)
            ),
        ]

        overall_status = "ok" if all(item["status"] == "ok" for item in results) else "degraded"
        return {
            "phase": "pre_bootstrap",
            "status": overall_status,
            "checks": results,
            "connections": results,
        }

    def validate_settings(self) -> Dict[str, Any]:
        """Return a sanitized configuration check before any connectivity call."""
        try:
            self.settings.safe_summary()
            return {
                "phase": "pre_bootstrap",
                "status": "ok",
                "checks": [
                    {
                        "name": "settings",
                        "category": "configuration",
                        "status": "ok",
                        "reason": "settings validated",
                    }
                ],
            }
        except Exception as exc:
            return {
                "phase": "pre_bootstrap",
                "status": "failed",
                "checks": [
                    {
                        "name": "settings",
                        "category": "configuration",
                        "status": "failed",
                        "reason": f"settings validation error: {type(exc).__name__}",
                    }
                ],
            }
