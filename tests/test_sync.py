from contextlib import asynccontextmanager
from datetime import UTC, date, datetime

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from events_aggregator.provider import ProviderError
from events_aggregator.storage import (
    EventRepository,
    SyncRepository,
    TicketRepository,
    create_schema,
)
from events_aggregator.sync import SyncService


def event(event_id: str, changed_at: str) -> dict:
    return {"id": event_id, "changed_at": changed_at}


class Events:
    def __init__(self):
        self.batches: list[list[dict]] = []

    async def upsert_many(self, batch: list[dict]) -> None:
        self.batches.append(list(batch))


class State:
    def __init__(self, checkpoint=None, acquired=True):
        self.checkpoint = checkpoint
        self.acquired = acquired
        self.writes = []
        self.status = None

    @asynccontextmanager
    async def sync_lock(self):
        yield self.acquired

    async def get_state(self):
        return self.checkpoint

    async def mark_started(self):
        self.status = "running"

    async def mark_succeeded(self, value):
        self.writes.append(value)
        self.checkpoint = value
        self.status = "success"

    async def mark_failed(self, error):
        self.status = "failed"


class Client:
    def __init__(self, pages):
        self.pages = pages
        self.requested_dates = []

    async def events_page(self, changed_at, next_url=None):
        self.requested_dates.append(changed_at)
        value = self.pages[next_url]
        if isinstance(value, Exception):
            raise value
        return value


@pytest.mark.asyncio
async def test_sync_all_pages_then_advance_checkpoint_with_date_overlap():
    prior = datetime(2026, 9, 24, 18, 30, tzinfo=UTC)
    events, state = Events(), State(prior)
    client = Client(
        {
            None: {
                "results": [event("a", "2026-09-24T18:30:00Z")],
                "next": "?page=2",
            },
            "?page=2": {
                "results": [event("b", "2026-09-25T08:00:00Z")],
                "next": None,
            },
        }
    )
    result = await SyncService(events, state, client, batch_size=1).run()
    assert client.requested_dates == [date(2026, 9, 24)] * 2
    assert [batch[0]["id"] for batch in events.batches] == ["a", "b"]
    assert state.writes == [datetime(2026, 9, 25, 8, tzinfo=UTC)]
    assert state.status == "success"
    assert result["synced_events"] == 2


@pytest.mark.asyncio
async def test_failed_later_page_does_not_advance_checkpoint():
    prior = datetime(2026, 9, 24, tzinfo=UTC)
    events, state = Events(), State(prior)
    client = Client(
        {
            None: {
                "results": [event("a", "2026-09-25T08:00:00Z")],
                "next": "?page=2",
            },
            "?page=2": ProviderError("provider unavailable"),
        }
    )
    with pytest.raises(ProviderError):
        await SyncService(events, state, client, batch_size=1).run()
    assert len(events.batches) == 1  # Safe to replay the same event on retry.
    assert state.writes == []
    assert state.checkpoint == prior
    assert state.status == "failed"


@pytest.mark.asyncio
async def test_no_checkpoint_on_empty_initial_sync_and_skip_if_locked():
    events, state = Events(), State()
    client = Client({None: {"results": [], "next": None}})
    result = await SyncService(events, state, client).run()
    assert result["synced_events"] == 0
    assert client.requested_dates == [date(2000, 1, 1)]
    assert state.writes == [None]

    locked = State(acquired=False)
    result = await SyncService(events, locked, client).run()
    assert result["skipped"] is True
    assert client.requested_dates == [date(2000, 1, 1)]


@pytest.mark.asyncio
async def test_repositories_persist_events_tickets_and_sync_metadata():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        await create_schema(engine)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        events = EventRepository(sessions)
        tickets = TicketRepository(sessions)
        state = SyncRepository(sessions)
        provider_event = {
            "id": "event-1",
            "name": "Concert",
            "place": {
                "id": "place-1",
                "name": "Hall",
                "city": "Moscow",
                "address": "Street 1",
                "seats_pattern": "A1-20",
            },
            "event_time": "2026-10-01T18:00:00Z",
            "registration_deadline": "2026-10-01T17:00:00Z",
            "status": "published",
            "number_of_visitors": 3,
            "changed_at": "2026-09-25T08:00:00Z",
        }
        # Repeated IDs within a batch must not violate the primary key.
        await events.upsert_many([provider_event, provider_event])
        count, listed = await events.list(None, page=1, page_size=10)
        assert count == 1
        assert listed[0]["place"] == {
            "id": "place-1",
            "name": "Hall",
            "city": "Moscow",
            "address": "Street 1",
        }
        assert (await events.get("event-1"))["place"]["seats_pattern"] == "A1-20"
        await tickets.create("event-1", "ticket-1", "Ana", "Doe", "a@b.test", "A15")
        assert (await tickets.get("ticket-1"))["seat"] == "A15"
        assert await tickets.delete("ticket-1") is True
        assert await tickets.get("ticket-1") is None

        await state.mark_started()
        await state.mark_failed(ProviderError("unavailable"))
        assert (await state.get_metadata())["sync_status"] == "failed"
        assert await state.get_state() is None
        await state.mark_started()
        await state.mark_succeeded(datetime(2026, 9, 25, 8, tzinfo=UTC))
        metadata = await state.get_metadata()
        assert metadata["sync_status"] == "success"
        assert metadata["last_changed_at"] == datetime(2026, 9, 25, 8, tzinfo=UTC)
        assert metadata["last_sync_time"] is not None
    finally:
        await engine.dispose()
