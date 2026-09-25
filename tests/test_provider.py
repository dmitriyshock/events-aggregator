import asyncio
from datetime import date
from unittest.mock import AsyncMock

import httpx
import pytest

from events_aggregator.provider import EventsProviderClient, ProviderError

BASE_URL = "https://provider.example/"


def response(method: str, url: str, status: int = 200, json=None) -> httpx.Response:
    request = httpx.Request(method, url)
    if json is None:
        return httpx.Response(status, request=request)
    return httpx.Response(status, request=request, json=json)


def make_client(result: httpx.Response):
    http_client = AsyncMock(spec=httpx.AsyncClient)
    http_client.request.return_value = result
    return EventsProviderClient("secret", BASE_URL, http_client), http_client


def test_events_page_sends_date_and_preserves_event_endpoint_slash():
    client, http_client = make_client(
        response(
            "GET",
            "https://provider.example/api/events/",
            json={"results": [], "next": None},
        )
    )
    page = asyncio.run(client.events_page(date(2026, 9, 25)))
    assert page == {"results": [], "next": None}
    http_client.request.assert_awaited_once_with(
        "GET",
        "https://provider.example/api/events/",
        headers={"x-api-key": "secret"},
        params={"changed_at": "2026-09-25"},
    )


@pytest.mark.parametrize(
    "next_url",
    [
        "https://foreign.example/api/events/?cursor=2",
        "//foreign.example/api/events/?cursor=2",
    ],
)
def test_pagination_rejects_foreign_origin_before_request(next_url):
    client, http_client = make_client(
        response("GET", "https://provider.example/api/events/")
    )
    with pytest.raises(ProviderError, match="unsafe pagination URL"):
        asyncio.run(client.events_page(date(2026, 9, 25), next_url))
    http_client.request.assert_not_awaited()


def test_pagination_accepts_same_origin_cursor():
    client, http_client = make_client(
        response(
            "GET",
            "https://provider.example/api/events/?cursor=two",
            json={"results": []},
        )
    )
    asyncio.run(client.events_page(date(2026, 9, 25), "/api/events/?cursor=two"))
    http_client.request.assert_awaited_once_with(
        "GET",
        "https://provider.example/api/events/?cursor=two",
        headers={"x-api-key": "secret"},
    )


def test_seats_and_registration_contract():
    http_client = AsyncMock(spec=httpx.AsyncClient)
    http_client.request.side_effect = [
        response(
            "GET",
            "https://provider.example/api/events/event%2F1/seats/",
            json={"seats": ["A1"]},
        ),
        response(
            "POST",
            "https://provider.example/api/events/event%2F1/register/",
            json={"ticket_id": "T1"},
        ),
        response(
            "DELETE",
            "https://provider.example/api/events/event%2F1/unregister/",
            status=204,
        ),
    ]
    client = EventsProviderClient("secret", BASE_URL, http_client)

    async def exercise():
        assert await client.seats("event/1") == ["A1"]
        assert (
            await client.register("event/1", "Ada", "Lovelace", "ada@example.com", "A1")
            == "T1"
        )
        assert await client.unregister("event/1", "T1") is True

    asyncio.run(exercise())
    assert http_client.request.await_args_list[0].args == (
        "GET",
        "https://provider.example/api/events/event%2F1/seats/",
    )
    assert http_client.request.await_args_list[1].kwargs["json"] == {
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.com",
        "seat": "A1",
    }
    assert http_client.request.await_args_list[2].args[0] == "DELETE"
    assert http_client.request.await_args_list[2].kwargs["json"] == {"ticket_id": "T1"}


def test_http_error_exposes_status_without_response_body():
    client, _ = make_client(
        response(
            "POST",
            "https://provider.example/api/events/1/register/",
            409,
            {"detail": "private"},
        )
    )
    with pytest.raises(ProviderError) as error:
        asyncio.run(client.register("1", "Ada", "Lovelace", "ada@example.com", "A1"))
    assert error.value.status_code == 409
    assert "private" not in str(error.value)


def test_transport_error_becomes_provider_error():
    http_client = AsyncMock(spec=httpx.AsyncClient)
    http_client.request.side_effect = httpx.ConnectError("connection refused")
    client = EventsProviderClient("secret", BASE_URL, http_client)
    with pytest.raises(ProviderError) as error:
        asyncio.run(client.seats("1"))
    assert error.value.status_code is None


def test_invalid_provider_payload_is_reported():
    client, _ = make_client(
        response(
            "GET", "https://provider.example/api/events/1/seats/", json={"seats": "A1"}
        )
    )
    with pytest.raises(ProviderError, match="invalid seats"):
        asyncio.run(client.seats("1"))
