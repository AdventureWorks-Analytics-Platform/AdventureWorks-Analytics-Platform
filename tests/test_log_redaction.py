import logging

from src.core.settings import Settings
from src.shared.connectors.postgres_connector import PostgreSQLConnector
from src.shared.security.log_redaction import redact_log_message


def test_redaction_removes_secrets_and_credential_values():
    message = "password=warehouse-secret payload=customer-full-payload"

    redacted = redact_log_message(message, ("warehouse-secret", "customer-full-payload"))

    assert "warehouse-secret" not in redacted
    assert "customer-full-payload" not in redacted
    assert "password=[REDACTED]" in redacted


def test_postgres_connection_error_does_not_log_password(monkeypatch, caplog):
    settings = Settings(
        sql_server_host="sql-host",
        postgres_username="warehouse-user",
        postgres_password="warehouse-secret",
        _env_file=None,
    )

    def fail_connect(**kwargs):
        raise RuntimeError(
            "password=warehouse-secret payload=customer-full-payload"
        )

    monkeypatch.setattr(
        "src.shared.connectors.postgres_connector.psycopg2.connect", fail_connect
    )
    with caplog.at_level(logging.ERROR):
        assert PostgreSQLConnector(settings).connect() is False

    assert "warehouse-secret" not in caplog.text
    assert "customer-full-payload" not in caplog.text
    assert "RuntimeError" in caplog.text
