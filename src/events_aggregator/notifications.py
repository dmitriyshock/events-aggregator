"""Capashino transport with stable keys supplied by the durable outbox."""

import httpx


class NotificationError(Exception):
    """A failed or unconfirmed delivery; never includes credentials or bodies."""


class CapashinoClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        *,
        timeout: float = 10,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        base = httpx.URL(base_url)
        if base.scheme not in ("http", "https") or not base.host or base.userinfo:
            raise ValueError("CAPASHINO_BASE_URL must be an absolute HTTP URL")
        if not api_key:
            raise ValueError("CAPASHINO_API_KEY is required")
        self.url = base_url.rstrip("/") + "/api/notifications"
        self._api_key = api_key
        self._client = http_client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = http_client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def send(self, payload: dict) -> None:
        try:
            response = await self._client.post(
                self.url,
                headers={"X-API-Key": self._api_key},
                json={
                    "message": payload["message"],
                    "reference_id": payload["ticket_id"],
                    "idempotency_key": payload["idempotency_key"],
                },
                follow_redirects=False,
            )
        except httpx.RequestError as exc:
            raise NotificationError("Capashino request failed") from exc
        if response.status_code != 201:
            raise NotificationError(f"Capashino returned HTTP {response.status_code}")
        try:
            result = response.json()
        except ValueError as exc:
            raise NotificationError("Capashino returned invalid JSON") from exc
        if (
            not isinstance(result, dict)
            or not result.get("id")
            or str(result.get("reference_id")) != str(payload["ticket_id"])
            or (
                "idempotency_key" in result
                and result["idempotency_key"] != payload["idempotency_key"]
            )
        ):
            raise NotificationError("Capashino returned an unconfirmed notification")
