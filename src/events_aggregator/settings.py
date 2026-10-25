"""Application configuration loaded from environment variables."""

from pydantic import AliasChoices, Field, SecretStr
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
    outbox_interval_seconds: float = Field(default=5, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1, le=1000)
    outbox_retry_base_seconds: float = Field(default=5, gt=0)
    outbox_retry_max_seconds: float = Field(default=300, gt=0)
    capashino_base_url: str = (
        "http://student-system-capashino-web.student-system-capashino.svc:8000"
    )
    capashino_api_key: SecretStr | None = None
    capashino_timeout_seconds: float = Field(default=10, gt=0)
    idempotency_ttl_seconds: int = Field(default=86400, ge=1)
    sentry_dsn: SecretStr | None = Field(
        default=None, validation_alias=AliasChoices("SENTRY_DSN", "GLITCHTIP_DSN")
    )
    app_environment: str = "production"
    app_release: str | None = None
    enable_error_test_endpoint: bool = False

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
