"""Application configuration loaded from environment variables."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_ignore_empty=True, extra="ignore")

    database_url: str | None = Field(default=None, validation_alias="DATABASE_URL")
    postgres_connection_string: str | None = Field(
        default=None, validation_alias="POSTGRES_CONNECTION_STRING"
    )
    events_provider_api_key: SecretStr = Field(
        validation_alias="EVENTS_PROVIDER_API_KEY"
    )
    events_provider_base_url: str = Field(
        default="http://student-system-events-provider-web.student-system-events-provider.svc:8000",
        validation_alias="EVENTS_PROVIDER_BASE_URL",
    )
    sync_interval_seconds: int = Field(
        default=86400, ge=1, validation_alias="SYNC_INTERVAL_SECONDS"
    )
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    @property
    def async_database_url(self) -> str:
        value = self.database_url or self.postgres_connection_string
        if value is None:
            raise ValueError("DATABASE_URL or POSTGRES_CONNECTION_STRING is required")
        if value.startswith("postgres://"):
            return "postgresql+asyncpg://" + value[len("postgres://") :]
        if value.startswith("postgresql://"):
            return "postgresql+asyncpg://" + value[len("postgresql://") :]
        return value
