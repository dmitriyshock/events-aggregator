"""GlitchTip uses the Sentry SDK's automatic FastAPI exception integration."""

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration

from .settings import Settings


def _without_credentials(event: dict, _hint: dict) -> dict:
    request = event.get("request", {})
    headers = request.get("headers", {})
    if isinstance(headers, dict):
        request["headers"] = {
            key: value
            for key, value in headers.items()
            if key.lower() not in {"authorization", "x-api-key", "cookie"}
        }
    request.pop("cookies", None)
    request.pop("data", None)
    return event


def configure_monitoring(settings: Settings) -> None:
    if settings.sentry_dsn is None:
        return
    sentry_sdk.init(
        dsn=settings.sentry_dsn.get_secret_value(),
        integrations=[FastApiIntegration(), StarletteIntegration()],
        environment=settings.app_environment,
        release=settings.app_release,
        traces_sample_rate=0,
        auto_session_tracking=False,
        include_local_variables=False,
        max_request_body_size="never",
        before_send=_without_credentials,
    )
