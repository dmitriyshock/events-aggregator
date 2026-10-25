"""Key replay, conflicts, failed registration and concurrent requests."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from events_aggregator.idempotency import IdempotencyRepository
from events_aggregator.main import app
from events_aggregator.provider import ProviderError
from events_aggregator.services import DomainError, SeatsCache, TicketService
from events_aggregator.storage import (
    Event,
    EventRepository,
    IdempotencyRecord,
    OutboxEvent,
    Ticket,
    TicketRepository,
    create_schema,
)

REQUEST = dict(
    event_id="event-1",
    first_name="Ada",
    last_name="Lovelace",
    email="ada@example.com",
    seat="A1",
)


def make_service(sessions, provider):
    return TicketService(
        EventRepository(sessions),
        TicketRepository(sessions),
        provider,
        SeatsCache(),
        IdempotencyRepository(sessions),
    )


@pytest.fixture
async def registration(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'keys.db'}")
    await create_schema(engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    await EventRepository(sessions).upsert_many(
        [
            {
                "id": "event-1",
                "name": "Python",
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
    provider.register.side_effect = lambda *_args: str(uuid4())
    try:
        yield sessions, provider, make_service(sessions, provider)
    finally:
        await engine.dispose()


async def counts(sessions):
    async with sessions() as session:
        return [
            await session.scalar(select(func.count()).select_from(model))
            for model in (Ticket, OutboxEvent, IdempotencyRecord)
        ]


@pytest.mark.asyncio
async def test_replay_survives_restart_and_event_closure(registration):
    sessions, provider, svc = registration
    first = await svc.register(**REQUEST, idempotency_key="retry-key")
    async with sessions.begin() as session:
        await session.execute(update(Event).values(status="cancelled"))
    restarted = make_service(sessions, provider)
    assert await restarted.register(**REQUEST, idempotency_key="retry-key") == first
    provider.register.assert_awaited_once()
    provider.seats.assert_awaited_once()
    assert await counts(sessions) == [1, 1, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        dict(event_id="other-event"),
        dict(seat="A2"),
        dict(email="other@example.com"),
        dict(first_name="Grace"),
        dict(last_name="Hopper"),
    ],
)
async def test_same_key_with_changed_data_conflicts(registration, changed):
    sessions, provider, svc = registration
    first = await svc.register(**REQUEST, idempotency_key="retry-key")
    with pytest.raises(DomainError) as caught:
        await svc.register(**{**REQUEST, **changed}, idempotency_key="retry-key")
    assert caught.value.status_code == 409
    assert await svc.register(**REQUEST, idempotency_key="retry-key") == first
    provider.register.assert_awaited_once()
    assert await counts(sessions) == [1, 1, 1]


@pytest.mark.asyncio
async def test_provider_failure_does_not_consume_key(registration):
    sessions, provider, svc = registration
    provider.register.side_effect = [ProviderError("Unavailable", 503), "ticket-1"]
    with pytest.raises(ProviderError):
        await svc.register(**REQUEST, idempotency_key="retry-key")
    assert await counts(sessions) == [0, 0, 0]
    assert await svc.register(**REQUEST, idempotency_key="retry-key") == "ticket-1"
    assert await counts(sessions) == [1, 1, 1]


@pytest.mark.asyncio
async def test_parallel_requests_create_one_ticket(registration):
    sessions, provider, svc = registration
    results = await asyncio.gather(
        *[
            make_service(sessions, provider).register(
                **REQUEST, idempotency_key="parallel-key"
            )
            for _ in range(12)
        ]
    )
    assert len(set(results)) == 1
    provider.register.assert_awaited_once()
    assert await counts(sessions) == [1, 1, 1]


@pytest.mark.asyncio
async def test_no_key_retains_independent_registration(registration):
    sessions, provider, svc = registration
    first = await svc.register(**REQUEST)
    second = await svc.register(**{**REQUEST, "seat": "A2"})
    assert first != second
    assert provider.register.await_count == 2
    assert await counts(sessions) == [2, 2, 0]


@pytest.mark.asyncio
async def test_expired_key_can_be_reused(registration):
    sessions, provider, svc = registration
    first = await svc.register(**REQUEST, idempotency_key="expiring-key")
    async with sessions.begin() as session:
        await session.execute(
            update(IdempotencyRecord).values(
                expires_at=datetime.now(UTC) - timedelta(seconds=1)
            )
        )
    second = await svc.register(
        **{**REQUEST, "seat": "A2"}, idempotency_key="expiring-key"
    )
    assert first != second
    assert await counts(sessions) == [2, 2, 1]


@pytest.mark.asyncio
async def test_key_persistence_failure_rolls_back_ticket_and_outbox(registration):
    sessions, provider, svc = registration

    def fail(*_args):
        raise RuntimeError("Simulated key write failure")

    event.listen(IdempotencyRecord, "before_insert", fail)
    try:
        with pytest.raises(RuntimeError, match="key write"):
            await svc.register(**REQUEST, idempotency_key="retry-key")
    finally:
        event.remove(IdempotencyRecord, "before_insert", fail)
    assert await counts(sessions) == [0, 0, 0]
    provider.unregister.assert_awaited_once()
    await svc.register(**REQUEST, idempotency_key="retry-key")
    assert await counts(sessions) == [1, 1, 1]


@pytest.mark.asyncio
async def test_api_replay_and_conflict(registration):
    _, provider, svc = registration
    app.state.ticket_service = svc
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        request = {**REQUEST, "idempotency_key": "api-key"}
        first = await client.post("/api/tickets", json=request)
        replay = await client.post("/api/tickets", json=request)
        conflict = await client.post("/api/tickets", json={**request, "seat": "A2"})
    assert first.status_code == replay.status_code == 201
    assert first.json() == replay.json()
    assert conflict.status_code == 409
    provider.register.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["", "   ", 123, "k" * 256])
async def test_api_rejects_invalid_keys(registration, key):
    _, provider, svc = registration
    app.state.ticket_service = svc
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/tickets", json={**REQUEST, "idempotency_key": key}
        )
    assert response.status_code == 400
    provider.register.assert_not_awaited()
