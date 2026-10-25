"""PostgreSQL integration checks used by CI when a database is available."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from events_aggregator.idempotency import IdempotencyRepository
from events_aggregator.outbox import OutboxWorker
from events_aggregator.services import DomainError, SeatsCache, TicketService
from events_aggregator.storage import (
    EventRepository,
    IdempotencyRecord,
    OutboxEvent,
    SyncRepository,
    Ticket,
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


@pytest.mark.asyncio
async def test_postgres_parallel_keyed_requests_across_service_instances():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not configured")
    engine = create_async_engine(url, pool_size=3, max_overflow=0, pool_timeout=3)
    schema = "keys_test_" + uuid4().hex
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = engine.execution_options(schema_translate_map={None: schema})
    await create_schema(engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    event_id = str(uuid4())
    await EventRepository(sessions).upsert_many(
        [
            {
                "id": event_id,
                "name": "Concurrent test",
                "status": "published",
                "event_time": now + timedelta(days=1),
                "registration_deadline": now + timedelta(hours=1),
                "changed_at": now,
                "place": {"seats_pattern": "A1-10"},
            }
        ]
    )
    provider = AsyncMock()
    provider.seats.return_value = ["A1", "A2"]

    async def register(*_args):
        await asyncio.sleep(0.05)
        return str(uuid4())

    provider.register.side_effect = register

    def service():
        return TicketService(
            EventRepository(sessions),
            TicketRepository(sessions),
            provider,
            SeatsCache(),
            IdempotencyRepository(sessions),
        )

    args = (event_id, "Ada", "Lovelace", "ada@example.com", "A1")
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                *[
                    service().register(*args, idempotency_key="parallel-key")
                    for _ in range(12)
                ]
            ),
            timeout=15,
        )
        assert len(set(results)) == 1
        provider.register.assert_awaited_once()
        with pytest.raises(DomainError) as conflict:
            await service().register(*args[:-1], "A2", idempotency_key="parallel-key")
        assert conflict.value.status_code == 409
        async with sessions() as session:
            for model in (Ticket, OutboxEvent, IdempotencyRecord):
                assert (
                    await session.scalar(select(func.count()).select_from(model)) == 1
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_outbox_workers_skip_a_locked_record():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not configured")
    engine = create_async_engine(url)
    schema = "outbox_test_" + uuid4().hex
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = engine.execution_options(schema_translate_map={None: schema})
    await create_schema(engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    row_id = str(uuid4())
    async with sessions.begin() as session:
        session.add(OutboxEvent(id=row_id, event_type="ticket.purchased", payload={}))
    started, release = asyncio.Event(), asyncio.Event()

    async def send(_payload):
        started.set()
        await release.wait()

    sender = AsyncMock()
    sender.send.side_effect = send
    first = asyncio.create_task(OutboxWorker(sessions, sender).process_one())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        # Another worker must return rather than sending the locked notification.
        other = AsyncMock()
        assert not await asyncio.wait_for(
            OutboxWorker(sessions, other).process_one(), timeout=5
        )
        other.send.assert_not_awaited()
        release.set()
        assert await asyncio.wait_for(first, timeout=5)
        async with sessions() as session:
            row = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.id == row_id)
            )
            assert row.status == "sent"
        sender.send.assert_awaited_once()
    finally:
        release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await engine.dispose()
