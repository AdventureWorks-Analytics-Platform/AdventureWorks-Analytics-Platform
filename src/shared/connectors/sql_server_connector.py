import logging

import pyodbc

from src.core.settings import Settings, get_settings
from src.shared.connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)


class SQLServerConnector(BaseConnector):
    """SQL Server connection handler for AdventureWorks2012."""

    def __init__(
        self,
        use_windows_auth: bool | None = None,
        settings: Settings | None = None,
    ):
        super().__init__()
        resolved_settings = settings or get_settings()
        self.settings = resolved_settings
        self.host = resolved_settings.sql_server_host
        self.port = resolved_settings.sql_server_port
        self.database = resolved_settings.sql_server_database
        self.username = resolved_settings.sql_server_username
        self.password = (
            resolved_settings.sql_server_password.get_secret_value()
            if resolved_settings.sql_server_password
            else None
        )
        self.driver = resolved_settings.sql_server_driver
        self.use_windows_auth = (
            resolved_settings.sql_server_auth_mode == "windows"
            if use_windows_auth is None
            else use_windows_auth
        )

    def connect(self) -> bool:
        try:
            if self.use_windows_auth:
                connection_string = (
                    f"Driver={{{self.driver}}};"
                    f"Server={self.host};"
                    f"Database={self.database};"
                    f"Trusted_Connection=yes;"
                )
            else:
                connection_string = (
                    f"Driver={{{self.driver}}};"
                    f"Server={self.host},{self.port};"
                    f"Database={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                )

            self.connection = pyodbc.connect(
                connection_string,
                timeout=self.settings.bronze_query_timeout_seconds,
            )
            logger.info("Connected to SQL Server: %s/%s", self.host, self.database)
            return True
        except Exception as exc:  # pragma: no cover - logging branch
            logger.error(
                "Failed to connect to SQL Server at %s:%s/%s (%s)",
                self.host,
                self.port,
                self.database,
                type(exc).__name__,
            )
            self.connection = None
            return False

    def disconnect(self):
        if self.connection is not None:
            try:
                self.connection.rollback()
            except Exception:  # pragma: no cover - defensive cleanup
                logger.warning("SQL Server rollback during disconnect failed")
            finally:
                self.connection.close()
            self.connection = None
            logger.info("Disconnected from SQL Server")

    def execute_query(self, query: str):
        if not self.connection:
            raise RuntimeError("Not connected to SQL Server")
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
