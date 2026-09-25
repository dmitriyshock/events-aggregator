"""HTTP contract tests without external services."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from events_aggregator.main import app


@pytest.mark.asyncio
async def test_events_list_filters_and_builds_page_links():
    events = AsyncMock()
    events.list.return_value = (3, [{"id": "event-1"}, {"id": "event-2"}])
    app.state.events = events
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/events", params={"date_from": "2026-10-01", "page_size": 2}
        )
    assert response.status_code == 200
    assert response.json() == {
        "count": 3,
        "next": "http://test/api/events?date_from=2026-10-01&page_size=2&page=2",
        "previous": None,
        "results": [{"id": "event-1"}, {"id": "event-2"}],
    }
    events.list.assert_awaited_once_with(
        date_from=date(2026, 10, 1), page=1, page_size=2
    )


@pytest.mark.asyncio
async def test_ticket_creation_validates_email_before_calling_service():
    ticket_service = SimpleNamespace(register=AsyncMock(return_value="ticket-1"))
    app.state.ticket_service = ticket_service
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        bad = await client.post(
            "/api/tickets",
            json={
                "event_id": "event-1",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "email": "not-an-email",
                "seat": "A1",
            },
        )
        good = await client.post(
            "/api/tickets",
            json={
                "event_id": "event-1",
                "first_name": " Ada ",
                "last_name": "Lovelace",
                "email": "ada@example.com",
                "seat": "A1",
            },
        )
    assert bad.status_code == 400
    assert good.status_code == 201
    assert good.json() == {"ticket_id": "ticket-1"}
    ticket_service.register.assert_awaited_once_with(
        "event-1", "Ada", "Lovelace", "ada@example.com", "A1"
    )
