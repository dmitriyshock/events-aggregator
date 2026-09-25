"""Configuration compatibility with local and LMS environment variables."""

import pytest
from pydantic import ValidationError

from events_aggregator.settings import Settings


@pytest.fixture(autouse=True)
def clear_settings_environment(monkeypatch):
    for name in (
        "DATABASE_URL",
        "POSTGRES_CONNECTION_STRING",
        "EVENTS_PROVIDER_API_KEY",
        "EVENTS_PROVIDER_BASE_URL",
        "SYNC_INTERVAL_SECONDS",
        "LOG_LEVEL",
    ):
        monkeypatch.delenv(name, raising=False)


def test_database_url_takes_priority_and_normalizes_postgres(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://primary/events")
    monkeypatch.setenv("POSTGRES_CONNECTION_STRING", "postgresql://fallback/events")
    monkeypatch.setenv("EVENTS_PROVIDER_API_KEY", "test-secret")

    settings = Settings()

    assert settings.async_database_url == "postgresql+asyncpg://primary/events"
    assert "test-secret" not in repr(settings)


def test_postgres_connection_string_fallback(monkeypatch):
    monkeypatch.setenv("POSTGRES_CONNECTION_STRING", "postgresql://fallback/events")
    monkeypatch.setenv("EVENTS_PROVIDER_API_KEY", "test-secret")

    assert Settings().async_database_url == "postgresql+asyncpg://fallback/events"


def test_provider_key_is_required(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://localhost/events")

    with pytest.raises(ValidationError, match="EVENTS_PROVIDER_API_KEY"):
        Settings()


def test_sync_interval_must_be_positive(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://localhost/events")
    monkeypatch.setenv("EVENTS_PROVIDER_API_KEY", "test-secret")
    monkeypatch.setenv("SYNC_INTERVAL_SECONDS", "0")

    with pytest.raises(ValidationError, match="SYNC_INTERVAL_SECONDS"):
        Settings()
