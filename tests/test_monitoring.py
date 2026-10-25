"""Verify automatic exception capture without contacting a GlitchTip server."""

from types import SimpleNamespace

import httpx
import pytest
import sentry_sdk
from fastapi import FastAPI
from sentry_sdk.transport import Transport

from events_aggregator import monitoring
from events_aggregator.main import app
from events_aggregator.settings import Settings


class RecordingTransport(Transport):
    def __init__(self):
        super().__init__()
        self.envelopes = []

    def capture_envelope(self, envelope):
        self.envelopes.append(envelope)


@pytest.mark.asyncio
async def test_unhandled_fastapi_exception_is_captured_and_credentials_removed(
    monkeypatch,
):
    monkeypatch.setenv("EVENTS_PROVIDER_API_KEY", "test-secret")
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.invalid/1")
    settings = Settings()
    transport = RecordingTransport()
    real_init = sentry_sdk.init
    monkeypatch.setattr(
        monitoring.sentry_sdk,
        "init",
        lambda **kwargs: real_init(transport=transport, **kwargs),
    )
    application = FastAPI()
    monitoring.configure_monitoring(settings)

    @application.post("/fail")
    async def fail():
        raise RuntimeError("Expected monitoring verification")

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application, raise_app_exceptions=False),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/fail",
                headers={"X-API-Key": "test-secret", "Cookie": "private-cookie"},
                json={"email": "private@example.com"},
            )
        assert response.status_code == 500
        sentry_sdk.flush(timeout=2)
        captured = [
            item.payload.json
            for envelope in transport.envelopes
            for item in envelope.items
            if item.type == "event"
        ]
        assert any(
            "Expected monitoring verification" in str(event.get("exception"))
            for event in captured
        )
        serialized = str(captured)
        assert "test-secret" not in serialized
        assert "private-cookie" not in serialized
        assert "private@example.com" not in serialized
    finally:
        sentry_sdk.get_client().close()
        real_init(dsn="")


@pytest.mark.asyncio
async def test_debug_endpoint_requires_enable_flag_and_key():
    from pydantic import SecretStr

    app.state.settings = SimpleNamespace(
        enable_error_test_endpoint=False, events_provider_api_key=SecretStr("test-key")
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        assert (
            await client.get("/api/debug/error", headers={"X-API-Key": "test-key"})
        ).status_code == 404
        app.state.settings.enable_error_test_endpoint = True
        assert (await client.get("/api/debug/error")).status_code == 404
        assert (
            await client.get("/api/debug/error", headers={"X-API-Key": "wrong-key"})
        ).status_code == 404
        assert (
            await client.get("/api/debug/error", headers={"X-API-Key": "test-key"})
        ).status_code == 500
