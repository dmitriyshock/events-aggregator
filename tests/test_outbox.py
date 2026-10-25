"""Outbox atomicity, retry after restart, and interrupted delivery."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from events_aggregator.outbox import OutboxWorker
from events_aggregator.storage import (
    EventRepository,
    OutboxEvent,
    Ticket,
    TicketRepository,
    create_schema,
)


@pytest.fixture
async def outbox_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}")
    await create_schema(engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    await EventRepository(sessions).upsert_many(
        [
            {
                "id": "event-1",
                "name": "Python conference",
                "place": {"seats_pattern": "A1-10"},
                "event_time": now + timedelta(days=1),
                "registration_deadline": now + timedelta(hours=1),
                "status": "published",
                "changed_at": now,
            }
        ]
    )
    try:
        yield sessions
    finally:
        await engine.dispose()


async def add_ticket(sessions):
    await TicketRepository(sessions).create(
        "event-1", "ticket-1", "Ada", "Lovelace", "ada@example.com", "A1"
    )


@pytest.mark.asyncio
async def test_ticket_and_notification_are_committed_together(outbox_db):
    await add_ticket(outbox_db)
    async with outbox_db() as session:
        row = await session.scalar(select(OutboxEvent))
        assert await session.get(Ticket, "ticket-1") is not None
        assert row.status == "pending"
        assert row.payload["ticket_id"] == "ticket-1"
        assert "Python conference" in row.payload["message"]
        assert row.payload["idempotency_key"] == row.id


@pytest.mark.asyncio
async def test_outbox_failure_rolls_back_ticket(outbox_db):
    def fail_insert(*_args):
        raise RuntimeError("Simulated outbox insert failure")

    event.listen(OutboxEvent, "before_insert", fail_insert)
    try:
        with pytest.raises(RuntimeError, match="outbox insert"):
            await add_ticket(outbox_db)
    finally:
        event.remove(OutboxEvent, "before_insert", fail_insert)
    async with outbox_db() as session:
        assert await session.scalar(select(func.count()).select_from(Ticket)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


@pytest.mark.asyncio
async def test_retry_after_worker_restart_preserves_notification_key(outbox_db):
    await add_ticket(outbox_db)
    sender = AsyncMock()
    sender.send.side_effect = [RuntimeError("Unavailable"), None]
    assert await OutboxWorker(outbox_db, sender).process_one()
    async with outbox_db() as session:
        row = await session.scalar(select(OutboxEvent))
        assert row.status == "pending"
        assert row.attempts == 1
        assert row.last_error == "RuntimeError"
    # A future retry must wait, rather than exhausting the batch immediately.
    assert not await OutboxWorker(outbox_db, sender).process_one()
    async with outbox_db.begin() as session:
        await session.execute(
            update(OutboxEvent).values(next_attempt_at=datetime.now(UTC))
        )
    assert await OutboxWorker(outbox_db, sender).process_one()
    async with outbox_db() as session:
        row = await session.scalar(select(OutboxEvent))
        assert row.status == "sent"
        assert row.attempts == 2
        assert row.sent_at is not None
        assert row.last_error is None
    assert sender.send.await_args_list[0] == sender.send.await_args_list[1]
    assert not await OutboxWorker(outbox_db, sender).process_one()


@pytest.mark.asyncio
async def test_cancelled_delivery_remains_pending(outbox_db):
    await add_ticket(outbox_db)
    sender = AsyncMock()
    sender.send.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await OutboxWorker(outbox_db, sender).process_one()
    async with outbox_db() as session:
        row = await session.scalar(select(OutboxEvent))
        assert row.status == "pending"
        assert row.attempts == 0
