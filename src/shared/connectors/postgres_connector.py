import logging
from typing import Optional

import psycopg2

from src.core.settings import Settings, get_settings
from src.shared.connectors.base_connector import BaseConnector
from src.shared.observability.structured_logging import emit_event

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL connection handler for the analytics warehouse."""

    def __init__(self, settings: Optional[Settings] = None):
        super().__init__()
        resolved_settings = settings or get_settings()
        self.host = resolved_settings.postgres_host
        self.port = resolved_settings.postgres_port
        self.database = resolved_settings.postgres_database
        self.username = resolved_settings.postgres_username
        self.password = resolved_settings.postgres_password.get_secret_value()

    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
            )
            emit_event(
                logger,
                logging.INFO,
                "adapter.connected",
                adapter="postgresql",
                operation="connect",
                status="SUCCESS",
            )
            return True
        except Exception as exc:  # pragma: no cover - logging branch
            emit_event(
                logger,
                logging.ERROR,
                "adapter.failed",
                adapter="postgresql",
                operation="connect",
                status="FAILED",
                error_type=type(exc).__name__,
            )
            self.connection = None
            return False

    def disconnect(self):
        if self.connection is not None:
            if not getattr(self.connection, "closed", False):
                try:
                    self.connection.rollback()
                except Exception:  # pragma: no cover - defensive cleanup
                    emit_event(
                        logger,
                        logging.WARNING,
                        "adapter.failed",
                        adapter="postgresql",
                        operation="rollback_on_disconnect",
                        status="FAILED",
                        error_type="RollbackError",
                    )
            self.connection.close()
            self.connection = None
            emit_event(
                logger,
                logging.INFO,
                "adapter.disconnected",
                adapter="postgresql",
                operation="disconnect",
                status="SUCCESS",
            )

    def execute_query(self, query: str, params: Optional[tuple] = None):
        if not self.connection:
            raise RuntimeError("Not connected to PostgreSQL")
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
        return cursor

    def fetch_results(self, query: str, params: Optional[tuple] = None):
        cursor = self.execute_query(query, params)
        try:
            return cursor.fetchall()
        finally:
            cursor.close()
