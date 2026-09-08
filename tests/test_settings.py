import pytest
from pydantic import ValidationError

from src.core.settings import Settings


def make_settings(**overrides):
    values = {
        "sql_server_host": "sql-host",
        "postgres_username": "warehouse-user",
        "postgres_password": "warehouse-secret",
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)


def test_settings_parse_types_and_keep_password_out_of_safe_summary():
    settings = make_settings(batch_size="5000", debug="true")

    assert settings.batch_size == 5000
    assert settings.bronze_rejected_threshold == 0
    assert settings.safe_summary()["bronze_rejected_threshold"] == 0
    assert settings.debug is True
    assert "warehouse-secret" not in repr(settings)
    assert "warehouse-secret" not in str(settings.safe_summary())


def test_settings_environment_override(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "2500")
    monkeypatch.setenv("BRONZE_REJECTED_THRESHOLD", "7")
    monkeypatch.setenv("POSTGRES_PASSWORD", "environment-secret")

    settings = Settings(
        sql_server_host="sql-host",
        postgres_username="warehouse-user",
        _env_file=None,
    )

    assert settings.batch_size == 2500
    assert settings.bronze_rejected_threshold == 7
    assert settings.postgres_password.get_secret_value() == "environment-secret"


def test_negative_bronze_rejected_threshold_is_rejected():
    with pytest.raises(ValidationError, match="bronze_rejected_threshold"):
        make_settings(bronze_rejected_threshold=-1)


def test_sql_auth_requires_credentials():
    with pytest.raises(ValidationError, match="username and password"):
        make_settings(sql_server_auth_mode="sql")


def test_windows_auth_does_not_require_credentials():
    settings = make_settings(sql_server_auth_mode="windows")

    assert settings.sql_server_username is None
    assert settings.sql_server_password is None


def test_non_development_environment_rejects_default_postgres_password():
    with pytest.raises(ValidationError, match="explicitly configured"):
        Settings(
            sql_server_host="sql-host",
            postgres_username="warehouse-user",
            environment="production",
            _env_file=None,
        )


def test_retry_max_delay_cannot_be_less_than_initial_delay():
    with pytest.raises(ValidationError, match="retry_max_delay_seconds"):
        make_settings(
            retry_initial_delay_seconds=10,
            retry_max_delay_seconds=5,
        )


@pytest.mark.parametrize("attempts", [1, 2, 3])
def test_retry_max_attempts_accepts_one_to_three_total_attempts(attempts):
    settings = make_settings(retry_max_attempts=attempts)

    assert settings.retry_max_attempts == attempts


@pytest.mark.parametrize("attempts", [0, 4, 10])
def test_retry_max_attempts_rejects_values_outside_one_to_three(attempts):
    with pytest.raises(ValidationError, match="retry_max_attempts"):
        make_settings(retry_max_attempts=attempts)


def test_connectors_use_injected_settings():
    from src.shared.connectors.postgres_connector import PostgreSQLConnector
    from src.shared.connectors.sql_server_connector import SQLServerConnector

    settings = make_settings(
        sql_server_host="injected-sql",
        postgres_host="injected-postgres",
        postgres_port=5544,
    )

    postgres = PostgreSQLConnector(settings=settings)
    sql_server = SQLServerConnector(settings=settings)

    assert (postgres.host, postgres.port) == ("injected-postgres", 5544)
    assert sql_server.host == "injected-sql"