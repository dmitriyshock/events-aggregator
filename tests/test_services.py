"""Tests for the registration rules at the provider boundary."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from events_aggregator.services import DomainError, SeatsCache, TicketService


def event(*, status="published", deadline_hours=1):
    return {
        "id": "event-1",
        "status": status,
        "registration_deadline": (
            datetime.now(UTC) + timedelta(hours=deadline_hours)
        ).isoformat(),
        "event_time": (datetime.now(UTC) + timedelta(days=1)).isoformat(),
        "place": {"seats_pattern": "A1-10,B1-20"},
    }


def service(event_data):
    events = AsyncMock()
    events.get.return_value = event_data
    tickets = AsyncMock()
    provider = AsyncMock()
    provider.seats.return_value = ["A1", "A2"]
    provider.register.return_value = "ticket-1"
    provider.unregister.return_value = True
    return TicketService(events, tickets, provider, SeatsCache()), tickets, provider


@pytest.mark.asyncio
async def test_registration_saves_provider_ticket_and_invalidates_seats_cache():
    svc, tickets, provider = service(event())
    assert await svc.available_seats("event-1") == ["A1", "A2"]
    assert (
        await svc.register("event-1", "Ada", "Lovelace", "a@b.test", "A1") == "ticket-1"
    )
    tickets.create.assert_awaited_once_with(
        "event-1", "ticket-1", "Ada", "Lovelace", "a@b.test", "A1"
    )
    # One read for cached endpoint and a fresh read for registration.
    assert provider.seats.await_count == 2
    await svc.available_seats("event-1")
    assert provider.seats.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_data", "seat", "status"),
    [
        (None, "A1", 404),
        (event(status="new"), "A1", 409),
        (event(deadline_hours=-1), "A1", 409),
        (event(), "C1", 422),
    ],
)
async def test_registration_rejects_invalid_event_or_seat(event_data, seat, status):
    svc, tickets, provider = service(event_data)
    with pytest.raises(DomainError) as caught:
        await svc.register("event-1", "Ada", "Lovelace", "a@b.test", seat)
    assert caught.value.status_code == status
    provider.register.assert_not_awaited()
    tickets.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_seat_rejected_before_provider_registration():
    svc, _, provider = service(event())
    provider.seats.return_value = []
    with pytest.raises(DomainError) as caught:
        await svc.register("event-1", "Ada", "Lovelace", "a@b.test", "A1")
    assert caught.value.status_code == 409
    provider.register.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_uses_stored_event_id_and_deletes_local_ticket():
    svc, tickets, provider = service(event())
    tickets.get.return_value = {"event_id": "event-1"}
    assert await svc.unregister("ticket-1") is True
    provider.unregister.assert_awaited_once_with("event-1", "ticket-1")
    tickets.delete.assert_awaited_once_with("ticket-1")
