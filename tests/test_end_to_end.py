"""Exercise the API, synchronization, and repositories together."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from events_aggregator import main


class FakeProvider:
    def __init__(self, *_args, **_kwargs):
        now = datetime.now(UTC)
        self.event = {
            "id": "event-1",
            "name": "Concert",
            "place": {
                "id": "place-1",
                "name": "Hall",
                "city": "Moscow",
                "address": "Street 1",
                "seats_pattern": "A1-10",
            },
            "event_time": (now + timedelta(days=30)).isoformat(),
            "registration_deadline": (now + timedelta(days=29)).isoformat(),
            "status": "published",
            "number_of_visitors": 0,
            "changed_at": now.isoformat(),
        }
        self.available = ["A1", "A2"]

    async def events_page(self, _changed_at, next_url=None):
        assert next_url is None
        return {"results": [self.event], "next": None}

    async def seats(self, _event_id):
        return list(self.available)

    async def register(self, _event_id, _first_name, _last_name, _email, seat):
        self.available.remove(seat)
        return "ticket-1"

    async def unregister(self, _event_id, _ticket_id):
        self.available.append("A1")
        return True

    async def aclose(self):
        pass


@pytest.mark.asyncio
async def test_sync_list_register_cancel_roundtrip(tmp_path, monkeypatch):
    database_path = tmp_path / "events.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{database_path}")
    monkeypatch.setenv("EVENTS_PROVIDER_API_KEY", "test-key")
    monkeypatch.setattr(main, "EventsProviderClient", FakeProvider)
    transport = httpx.ASGITransport(app=main.app)
    async with main.lifespan(main.app):
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            assert (await client.get("/api/health")).status_code == 200
            assert (await client.post("/api/sync/trigger")).status_code == 200
            listing = (await client.get("/api/events")).json()
            assert listing["count"] == 1
            assert listing["results"][0]["name"] == "Concert"
            seats = (await client.get("/api/events/event-1/seats")).json()
            assert seats["available_seats"] == ["A1", "A2"]
            created = await client.post(
                "/api/tickets",
                json={
                    "event_id": "event-1",
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "email": "ada@example.com",
                    "seat": "A1",
                },
            )
            assert created.status_code == 201
            assert created.json() == {"ticket_id": "ticket-1"}
            assert (await client.get("/api/events/event-1/seats")).json()[
                "available_seats"
            ] == ["A2"]
            cancelled = await client.delete("/api/tickets/ticket-1")
            assert cancelled.status_code == 200
            assert cancelled.json() == {"success": True}
