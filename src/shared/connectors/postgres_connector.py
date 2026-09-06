import logging
from typing import Optional

import psycopg2

from src.core.settings import Settings, get_settings
from src.shared.connectors.base_connector import BaseConnector

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
            logger.info("Connected to PostgreSQL: %s/%s", self.host, self.database)
            return True
        except Exception as exc:  # pragma: no cover - logging branch
            logger.error(
                "Failed to connect to PostgreSQL at %s:%s/%s (%s)",
                self.host,
                self.port,
                self.database,
                type(exc).__name__,
            )
            self.connection = None
            return False

    def disconnect(self):
        if self.connection is not None:
            if not getattr(self.connection, "closed", False):
                try:
                    self.connection.rollback()
                except Exception:  # pragma: no cover - defensive cleanup
                    logger.warning("PostgreSQL rollback during disconnect failed")
            self.connection.close()
            self.connection = None
            logger.info("Disconnected from PostgreSQL")

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
