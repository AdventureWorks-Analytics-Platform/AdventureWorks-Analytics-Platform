from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: Literal["development", "test", "staging", "production"] = "development"
    debug: bool = False
    log_level: str = "INFO"

    sql_server_host: str = "localhost"
    sql_server_port: int = 1433
    sql_server_database: str = "AdventureWorks2012"
    sql_server_driver: str = "ODBC Driver 17 for SQL Server"
    sql_server_auth_mode: Literal["windows", "sql"] = "windows"
    sql_server_username: str | None = None
    sql_server_password: SecretStr | None = None

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "adventureworks_warehouse"
    postgres_username: str = "postgres"
    postgres_password: SecretStr = SecretStr("postgres")

    batch_size: int = Field(default=10000, gt=0)
    bronze_rejected_threshold: int = Field(default=0, ge=0)
    silver_rejected_threshold: int = Field(default=0, ge=0)
    silver_transform_version: str = "silver-v1"
    retry_max_attempts: int = Field(default=3, ge=1, le=3)
    retry_initial_delay_seconds: float = Field(default=1.0, gt=0)
    retry_max_delay_seconds: float = Field(default=30.0, gt=0)
    bronze_query_timeout_seconds: int = Field(default=300, gt=0)
    bronze_batch_timeout_seconds: int = Field(default=300, gt=0)
    bronze_table_timeout_seconds: int = Field(default=3600, gt=0)
    bronze_stale_run_timeout_seconds: int = Field(default=7200, gt=0)

    @model_validator(mode="after")
    def validate_configuration(self):
        if self.sql_server_username == "":
            self.sql_server_username = None
        if (
            self.sql_server_password is not None
            and self.sql_server_password.get_secret_value() == ""
        ):
            self.sql_server_password = None

        if self.retry_max_delay_seconds < self.retry_initial_delay_seconds:
            raise ValueError(
                "retry_max_delay_seconds must be greater than or equal to "
                "retry_initial_delay_seconds"
            )

        if self.sql_server_auth_mode == "sql":
            if not self.sql_server_username or not self.sql_server_password:
                raise ValueError(
                    "SQL Server username and password are required when "
                    "sql_server_auth_mode=sql"
                )

        if self.environment not in {"development", "test"}:
            if self.postgres_password.get_secret_value() == "postgres":
                raise ValueError(
                    "POSTGRES_PASSWORD must be explicitly configured outside "
                    "development and test environments"
                )

        return self

    def safe_summary(self) -> dict[str, str | int | bool]:
        return {
            "environment": self.environment,
            "debug": self.debug,
            "log_level": self.log_level,
            "sql_server_host": self.sql_server_host,
            "sql_server_port": self.sql_server_port,
            "sql_server_database": self.sql_server_database,
            "sql_server_auth_mode": self.sql_server_auth_mode,
            "postgres_host": self.postgres_host,
            "postgres_port": self.postgres_port,
            "postgres_database": self.postgres_database,
            "batch_size": self.batch_size,
            "bronze_rejected_threshold": self.bronze_rejected_threshold,
            "silver_rejected_threshold": self.silver_rejected_threshold,
            "silver_transform_version": self.silver_transform_version,
            "retry_max_attempts": self.retry_max_attempts,
            "bronze_query_timeout_seconds": self.bronze_query_timeout_seconds,
            "bronze_batch_timeout_seconds": self.bronze_batch_timeout_seconds,
            "bronze_table_timeout_seconds": self.bronze_table_timeout_seconds,
            "bronze_stale_run_timeout_seconds": self.bronze_stale_run_timeout_seconds,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()