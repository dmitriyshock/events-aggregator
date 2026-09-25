"""Business rules independent of HTTP and database implementations."""

import asyncio
import re
import time
from datetime import UTC, datetime
from typing import Protocol


class DomainError(Exception):
    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class EventStore(Protocol):
    async def get(self, event_id: str) -> dict | None: ...


class TicketStore(Protocol):
    async def create(
        self,
        event_id: str,
        ticket_id: str,
        first_name: str,
        last_name: str,
        email: str,
        seat: str,
    ) -> None: ...

    async def get(self, ticket_id: str) -> dict | None: ...

    async def delete(self, ticket_id: str) -> None: ...


class Provider(Protocol):
    async def seats(self, event_id: str) -> list[str]: ...

    async def register(
        self,
        event_id: str,
        first_name: str,
        last_name: str,
        email: str,
        seat: str,
    ) -> str: ...

    async def unregister(self, event_id: str, ticket_id: str) -> bool: ...


def _parse_datetime(value: str | datetime) -> datetime:
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if result.tzinfo is None:
        raise ValueError("Event time must include a timezone")
    return result


def _seat_in_pattern(seat: str, pattern: str) -> bool:
    match = re.fullmatch(r"([A-Z])(\d+)", seat)
    if match is None:
        return False
    section, number = match.group(1), int(match.group(2))
    for part in pattern.split(","):
        range_match = re.fullmatch(r"([A-Z])(\d+)-(\d+)", part.strip())
        if range_match and section == range_match.group(1):
            if int(range_match.group(2)) <= number <= int(range_match.group(3)):
                return True
    return False


class SeatsCache:
    def __init__(self, ttl_seconds: int = 30) -> None:
        self.ttl_seconds = ttl_seconds
        self._values: dict[str, tuple[float, list[str]]] = {}
        self._lock = asyncio.Lock()

    async def get(self, event_id: str, provider: Provider) -> list[str]:
        now = time.monotonic()
        item = self._values.get(event_id)
        if item is not None and item[0] > now:
            return list(item[1])
        async with self._lock:
            item = self._values.get(event_id)
            if item is not None and item[0] > time.monotonic():
                return list(item[1])
            seats = await provider.seats(event_id)
            self._values[event_id] = (time.monotonic() + self.ttl_seconds, seats)
            return list(seats)

    def invalidate(self, event_id: str) -> None:
        self._values.pop(event_id, None)


class TicketService:
    def __init__(
        self,
        events: EventStore,
        tickets: TicketStore,
        provider: Provider,
        seats_cache: SeatsCache,
    ) -> None:
        self.events = events
        self.tickets = tickets
        self.provider = provider
        self.seats_cache = seats_cache

    async def available_seats(self, event_id: str) -> list[str]:
        event = await self.events.get(event_id)
        if event is None:
            raise DomainError("Event not found", 404)
        if event["status"] != "published":
            raise DomainError("Event is not published", 409)
        return await self.seats_cache.get(event_id, self.provider)

    async def register(
        self,
        event_id: str,
        first_name: str,
        last_name: str,
        email: str,
        seat: str,
    ) -> str:
        event = await self.events.get(event_id)
        if event is None:
            raise DomainError("Event not found", 404)
        if event["status"] != "published":
            raise DomainError("Event is not published", 409)
        deadline = _parse_datetime(event["registration_deadline"])
        if datetime.now(UTC) >= deadline:
            raise DomainError("Registration deadline has passed", 409)
        pattern = event["place"].get("seats_pattern", "")
        if not _seat_in_pattern(seat, pattern):
            raise DomainError("Seat does not exist at this place", 422)
        # Use a fresh provider read: a cached list can be stale during registration.
        if seat not in await self.provider.seats(event_id):
            raise DomainError("Seat is unavailable", 409)
        ticket_id = await self.provider.register(
            event_id, first_name, last_name, email, seat
        )
        try:
            await self.tickets.create(
                event_id, ticket_id, first_name, last_name, email, seat
            )
        except Exception:
            # Keep provider and local state aligned if local persistence fails.
            await self.provider.unregister(event_id, ticket_id)
            raise
        self.seats_cache.invalidate(event_id)
        return ticket_id

    async def unregister(self, ticket_id: str) -> bool:
        ticket = await self.tickets.get(ticket_id)
        if ticket is None:
            raise DomainError("Ticket not found", 404)
        event_id = ticket["event_id"]
        event = await self.events.get(event_id)
        if event is None:
            raise DomainError("Event not found", 404)
        if datetime.now(UTC) >= _parse_datetime(event["event_time"]):
            raise DomainError("Event has already started", 409)
        success = await self.provider.unregister(event_id, ticket_id)
        if success:
            await self.tickets.delete(ticket_id)
            self.seats_cache.invalidate(event_id)
        return success
