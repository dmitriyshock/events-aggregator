"""Capashino HTTP contract and replay after an interrupted acknowledgement."""

import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from events_aggregator.notifications import CapashinoClient, NotificationError
from events_aggregator.outbox import OutboxWorker
from events_aggregator.storage import OutboxEvent, create_schema

PAYLOAD = {
    "message": "Вы успешно зарегистрированы на мероприятие — Python",
    "ticket_id": "ticket-1",
    "idempotency_key": "outbox-1",
}


@pytest.mark.asyncio
async def test_capashino_headers_and_required_payload():
    def handler(request):
        assert str(request.url) == "https://capashino.test/api/notifications"
        assert request.headers["X-API-Key"] == "private-test-token"
        assert json.loads(request.content) == {
            "message": PAYLOAD["message"],
            "reference_id": "ticket-1",
            "idempotency_key": "outbox-1",
        }
        return httpx.Response(
            201, json={"id": "notification-1", "reference_id": "ticket-1"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await CapashinoClient(
            "private-test-token", "https://capashino.test", http_client=http
        ).send(PAYLOAD)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 301, 400, 401, 409, 422, 503])
async def test_unconfirmed_responses_are_not_successful(status):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(status, json={"secret": "never log me"})
        )
    ) as http:
        client = CapashinoClient("test-key", "https://capashino.test", http_client=http)
        with pytest.raises(NotificationError) as caught:
            await client.send(PAYLOAD)
        assert "never log me" not in str(caught.value)
        assert str(status) in str(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body", [None, {}, {"id": "notification-1", "reference_id": "other-ticket"}]
)
async def test_invalid_confirmation_is_retried(body):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(201, json=body))
    ) as http:
        client = CapashinoClient("test-key", "https://capashino.test", http_client=http)
        with pytest.raises(NotificationError):
            await client.send(PAYLOAD)


@pytest.mark.asyncio
async def test_server_accepts_then_process_crashes_without_duplicate(tmp_path):
    notifications = {}

    def handler(request):
        body = json.loads(request.content)
        key = body["idempotency_key"]
        notifications.setdefault(key, {"id": "notification-1", **body})
        return httpx.Response(201, json=notifications[key])

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'replay.db'}")
    await create_schema(engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions.begin() as session:
        session.add(
            OutboxEvent(id="outbox-1", event_type="ticket.purchased", payload=PAYLOAD)
        )
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = CapashinoClient(
                "test-key", "https://capashino.test", http_client=http
            )

            async def crash_after_send(payload):
                await client.send(payload)
                raise asyncio.CancelledError

            crashing = AsyncMock()
            crashing.send.side_effect = crash_after_send
            with pytest.raises(asyncio.CancelledError):
                await OutboxWorker(sessions, crashing).process_one()
            assert await OutboxWorker(sessions, client).process_one()
        assert len(notifications) == 1
        async with sessions() as session:
            row = await session.scalar(select(OutboxEvent))
            assert row.status == "sent"
    finally:
        await engine.dispose()
