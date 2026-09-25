"""Database models and repositories for the local events catalogue."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, time
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .statuses import EventStatus, SyncStatus


class Base(DeclarativeBase):
    pass


class Event(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    place_id: Mapped[str | None] = mapped_column(String(36))
    place_name: Mapped[str | None] = mapped_column(Text)
    place_city: Mapped[str | None] = mapped_column(Text)
    place_address: Mapped[str | None] = mapped_column(Text)
    place_seats_pattern: Mapped[Any | None] = mapped_column(JSON)
    place_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    place_changed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    registration_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    status: Mapped[EventStatus | str] = mapped_column(String(64), nullable=False)
    number_of_visitors: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status_changed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Ticket(Base):
    __tablename__ = "tickets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    first_name: Mapped[str] = mapped_column(Text, nullable=False)
    last_name: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    seat: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


class SyncState(Base):
    __tablename__ = "sync_state"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_changed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_sync_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    sync_status: Mapped[SyncStatus] = mapped_column(String(32), nullable=False)
    sync_error: Mapped[str | None] = mapped_column(Text)


async def create_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)


def timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min, tzinfo=UTC)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"Invalid timestamp: {value!r}")
    return (
        parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    )


def _event_status(value: str) -> EventStatus | str:
    try:
        return EventStatus(value)
    except ValueError:
        # The provider may add a status before this service knows its meaning.
        return value


def _event_values(data: dict[str, Any]) -> dict[str, Any]:
    place = data.get("place") or {}
    if not isinstance(place, dict):
        raise ValueError("event place must be an object")
    values = {
        "id": str(data["id"]),
        "name": data["name"],
        "place_id": str(place["id"]) if place.get("id") is not None else None,
        "place_name": place.get("name"),
        "place_city": place.get("city"),
        "place_address": place.get("address"),
        "place_seats_pattern": place.get("seats_pattern"),
        "place_created_at": timestamp(place.get("created_at")),
        "place_changed_at": timestamp(place.get("changed_at")),
        "event_time": timestamp(data["event_time"]),
        "registration_deadline": timestamp(data.get("registration_deadline")),
        "status": _event_status(data["status"]),
        "number_of_visitors": data.get("number_of_visitors", 0),
        "changed_at": timestamp(data["changed_at"]),
        "created_at": timestamp(data.get("created_at")),
        "status_changed_at": timestamp(data.get("status_changed_at")),
    }
    if values["event_time"] is None or values["changed_at"] is None:
        raise ValueError("event_time and changed_at are required")
    return values


def _event_dict(event: Event, *, detail: bool = True) -> dict[str, Any]:
    place: dict[str, Any] = {
        "id": event.place_id,
        "name": event.place_name,
        "city": event.place_city,
        "address": event.place_address,
    }
    if detail:
        place["seats_pattern"] = event.place_seats_pattern
        place["created_at"] = timestamp(event.place_created_at)
        place["changed_at"] = timestamp(event.place_changed_at)
    result = {
        "id": event.id,
        "name": event.name,
        "place": place,
        "event_time": timestamp(event.event_time),
        "registration_deadline": timestamp(event.registration_deadline),
        "status": _event_status(event.status),
        "number_of_visitors": event.number_of_visitors,
    }
    if detail:
        result.update(
            changed_at=timestamp(event.changed_at),
            created_at=timestamp(event.created_at),
            status_changed_at=timestamp(event.status_changed_at),
        )
    return result


class EventRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self.sessions = sessions

    async def list(
        self,
        date_from: date | datetime | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[int, list[dict[str, Any]]]:
        if page < 1 or page_size < 1:
            raise ValueError("page and page_size must be positive")
        start = timestamp(date_from)
        async with self.sessions() as session:
            count_query = select(func.count()).select_from(Event)
            rows_query = select(Event)
            if start is not None:
                count_query = count_query.where(Event.event_time >= start)
                rows_query = rows_query.where(Event.event_time >= start)
            count = await session.scalar(count_query)
            rows = await session.scalars(
                rows_query.order_by(Event.event_time, Event.id)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            return int(count or 0), [_event_dict(row, detail=False) for row in rows]

    async def get(self, event_id: str) -> dict[str, Any] | None:
        async with self.sessions() as session:
            event = await session.get(Event, str(event_id))
            return _event_dict(event) if event else None

    async def upsert_many(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        # Collapse duplicate IDs in a page before SQLAlchemy flushes new rows.
        latest_by_id: dict[str, dict[str, Any]] = {}
        for raw in events:
            values = _event_values(raw)
            current = latest_by_id.get(values["id"])
            if current is None or current["changed_at"] <= values["changed_at"]:
                latest_by_id[values["id"]] = values
        # A batch is atomic, while repeated provider pages may safely replay an ID.
        async with self.sessions.begin() as session:
            for values in latest_by_id.values():
                event = await session.get(Event, values["id"])
                if event is None:
                    session.add(Event(**values))
                elif timestamp(event.changed_at) <= values["changed_at"]:
                    for key, value in values.items():
                        setattr(event, key, value)


def _ticket_dict(ticket: Ticket) -> dict[str, Any]:
    return {
        "id": ticket.id,
        "event_id": ticket.event_id,
        "first_name": ticket.first_name,
        "last_name": ticket.last_name,
        "email": ticket.email,
        "seat": ticket.seat,
        "created_at": ticket.created_at,
    }


class TicketRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self.sessions = sessions

    async def create(
        self,
        event_id: str,
        ticket_id: str,
        first_name: str,
        last_name: str,
        email: str,
        seat: str,
    ) -> dict[str, Any]:
        ticket = Ticket(
            id=str(ticket_id),
            event_id=str(event_id),
            first_name=first_name,
            last_name=last_name,
            email=email,
            seat=seat,
        )
        async with self.sessions.begin() as session:
            session.add(ticket)
            await session.flush()
            return _ticket_dict(ticket)

    async def get(self, ticket_id: str) -> dict[str, Any] | None:
        async with self.sessions() as session:
            ticket = await session.get(Ticket, str(ticket_id))
            return _ticket_dict(ticket) if ticket else None

    async def delete(self, ticket_id: str) -> bool:
        async with self.sessions.begin() as session:
            ticket = await session.get(Ticket, str(ticket_id))
            if ticket is None:
                return False
            await session.delete(ticket)
            return True


class SyncRepository:
    """Persist a high-water mark for completed provider traversals."""

    KEY = "events"
    LOCK_ID = 0x45564E5453594E43
    _local_lock = asyncio.Lock()

    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self.sessions = sessions

    @asynccontextmanager
    async def sync_lock(self):
        """Yield False if another PostgreSQL worker owns this sync lock."""
        if self.sessions.kw["bind"].dialect.name != "postgresql":
            if self._local_lock.locked():
                yield False
                return
            async with self._local_lock:
                yield True
            return
        async with self.sessions() as session:
            locked = await session.scalar(
                text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": self.LOCK_ID}
            )
            if not locked:
                yield False
                return
            try:
                yield True
            finally:
                await session.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": self.LOCK_ID},
                )

    async def get_state(self) -> datetime | None:
        async with self.sessions() as session:
            row = await session.get(SyncState, self.KEY)
            return timestamp(row.last_changed_at) if row else None

    async def get_metadata(self) -> dict[str, Any] | None:
        async with self.sessions() as session:
            row = await session.get(SyncState, self.KEY)
            if row is None:
                return None
            return {
                "last_changed_at": timestamp(row.last_changed_at),
                "last_sync_time": timestamp(row.last_sync_time),
                "sync_status": SyncStatus(row.sync_status),
                "sync_error": row.sync_error,
            }

    async def mark_started(self) -> None:
        async with self.sessions.begin() as session:
            row = await session.get(SyncState, self.KEY)
            if row is None:
                session.add(SyncState(key=self.KEY, sync_status=SyncStatus.RUNNING))
            else:
                row.sync_status = SyncStatus.RUNNING
                row.sync_error = None

    async def mark_succeeded(self, last_changed_at: datetime | None) -> None:
        value = timestamp(last_changed_at)
        async with self.sessions.begin() as session:
            row = await session.get(SyncState, self.KEY)
            if row is None:
                row = SyncState(key=self.KEY, sync_status=SyncStatus.SUCCESS)
                session.add(row)
            row.last_sync_time = datetime.now(UTC)
            row.sync_status = SyncStatus.SUCCESS
            row.sync_error = None
            if value is not None and (
                row.last_changed_at is None or timestamp(row.last_changed_at) < value
            ):
                row.last_changed_at = value

    async def mark_failed(self, error: Exception) -> None:
        async with self.sessions.begin() as session:
            row = await session.get(SyncState, self.KEY)
            if row is None:
                row = SyncState(key=self.KEY, sync_status=SyncStatus.FAILED)
                session.add(row)
            row.last_sync_time = datetime.now(UTC)
            row.sync_status = SyncStatus.FAILED
            row.sync_error = f"{type(error).__name__}: {error}"[:1000]
