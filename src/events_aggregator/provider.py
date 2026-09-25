"""Async client for the Events Provider API and its cursor pagination."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from urllib.parse import quote

import httpx


class ProviderError(Exception):
    """A failed provider request or an invalid provider response.

    ``status_code`` is the HTTP status returned by the provider, when available.
    The exception text deliberately excludes response bodies and credentials.
    """

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class EventsProviderClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        base = httpx.URL(base_url)
        if base.scheme not in ("http", "https") or not base.host:
            raise ValueError("base_url must be an absolute HTTP URL")
        self._base_url = base_url.rstrip("/")
        self._events_url = f"{self._base_url}/api/events/"
        self._api_key = api_key
        self._http_client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_http_client = http_client is None

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    async def __aenter__(self) -> EventsProviderClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def _event_url(self, event_id: str, operation: str) -> str:
        if not event_id:
            raise ValueError("event_id is required")
        return f"{self._events_url}{quote(str(event_id), safe='')}/{operation}/"

    def _page_url(self, next_url: str) -> str:
        # A provider cursor can be an absolute URL, an absolute path, or a
        # query-relative URL. Never forward the API key to another origin.
        target = httpx.URL(self._events_url).join(next_url)
        base = httpx.URL(self._events_url)
        if (target.scheme, target.host, target.port) != (
            base.scheme,
            base.host,
            base.port,
        ) or target.userinfo:
            raise ProviderError("Provider returned an unsafe pagination URL")
        if not target.path.startswith(base.path):
            raise ProviderError("Provider returned an invalid pagination URL")
        return str(target)

    async def _request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        try:
            response = await self._http_client.request(
                method,
                url,
                headers={"x-api-key": self._api_key},
                **kwargs,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ProviderError(
                f"Events Provider returned HTTP {exc.response.status_code}",
                status_code=exc.response.status_code,
            ) from exc
        except httpx.RequestError as exc:
            raise ProviderError("Events Provider request failed") from exc
        return response

    @staticmethod
    def _json_object(response: httpx.Response) -> dict:
        try:
            payload = response.json()
        except ValueError as exc:
            raise ProviderError("Events Provider returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ProviderError("Events Provider returned an invalid response")
        return payload

    async def events_page(self, changed_at: date, next_url: str | None = None) -> dict:
        if next_url is None:
            response = await self._request(
                "GET", self._events_url, params={"changed_at": changed_at.isoformat()}
            )
        else:
            response = await self._request("GET", self._page_url(next_url))
        return self._json_object(response)

    async def seats(self, event_id: str) -> list[str]:
        response = await self._request("GET", self._event_url(event_id, "seats"))
        data = self._json_object(response)
        seats = data.get("seats")
        if not isinstance(seats, list) or not all(
            isinstance(seat, str) for seat in seats
        ):
            raise ProviderError("Events Provider returned invalid seats")
        return seats

    async def register(
        self,
        event_id: str,
        first_name: str,
        last_name: str,
        email: str,
        seat: str,
    ) -> str:
        response = await self._request(
            "POST",
            self._event_url(event_id, "register"),
            json={
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "seat": seat,
            },
        )
        ticket_id = self._json_object(response).get("ticket_id")
        if not isinstance(ticket_id, str) or not ticket_id:
            raise ProviderError("Events Provider returned an invalid ticket ID")
        return ticket_id

    async def unregister(self, event_id: str, ticket_id: str) -> bool:
        response = await self._request(
            "DELETE",
            self._event_url(event_id, "unregister"),
            json={"ticket_id": ticket_id},
        )
        # A successful DELETE can legitimately return no body (204).
        if not response.content:
            return True
        data = self._json_object(response)
        return data.get("success") is True


class EventsPaginator:
    """Iterate events from every page of a provider cursor result."""

    def __init__(self, client: EventsProviderClient, changed_at: date) -> None:
        self.client = client
        self.changed_at = changed_at

    async def __aiter__(self) -> AsyncIterator[dict]:
        next_url: str | None = None
        seen_urls: set[str] = set()
        while True:
            page = await self.client.events_page(self.changed_at, next_url)
            results = page.get("results")
            if not isinstance(results, list) or not all(
                isinstance(item, dict) for item in results
            ):
                raise ProviderError("Events Provider returned invalid events")
            for event in results:
                yield event
            next_value = page.get("next")
            if next_value is None or next_value == "":
                return
            if not isinstance(next_value, str) or next_value in seen_urls:
                raise ProviderError(
                    "Events Provider returned an invalid pagination cursor"
                )
            seen_urls.add(next_value)
            next_url = next_value
