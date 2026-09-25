"""PostgreSQL integration checks used by CI when a database is available."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from events_aggregator.storage import (
    EventRepository,
    SyncRepository,
    TicketRepository,
    create_schema,
)


@pytest.mark.asyncio
async def test_postgres_repositories_and_advisory_lock():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not configured")
    engine = create_async_engine(url)
    try:
        await create_schema(engine)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        events = EventRepository(sessions)
        tickets = TicketRepository(sessions)
        state = SyncRepository(sessions)
        event_id = str(uuid4())
        ticket_id = str(uuid4())
        now = datetime.now(UTC)
        await events.upsert_many(
            [
                {
                    "id": event_id,
                    "name": "Postgres test event",
                    "place": {
                        "id": str(uuid4()),
                        "name": "Hall",
                        "city": "Moscow",
                        "address": "Street 1",
                        "seats_pattern": "A1-5",
                    },
                    "event_time": (now + timedelta(days=1)).isoformat(),
                    "registration_deadline": (now + timedelta(hours=23)).isoformat(),
                    "status": "published",
                    "number_of_visitors": 0,
                    "changed_at": now.isoformat(),
                }
            ]
        )
        assert (await events.get(event_id))["place"]["seats_pattern"] == "A1-5"
        assert (await events.list(now.date(), 1, 20))[0] >= 1
        await tickets.create(event_id, ticket_id, "Ada", "Lovelace", "a@b.test", "A1")
        assert (await tickets.get(ticket_id))["event_id"] == event_id
        assert await tickets.delete(ticket_id) is True
        async with state.sync_lock() as first:
            assert first is True
            async with SyncRepository(sessions).sync_lock() as second:
                assert second is False
        async with state.sync_lock() as available_again:
            assert available_again is True
    finally:
        await engine.dispose()
