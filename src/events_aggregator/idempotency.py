"""Serialize keyed registrations and commit their result with ticket + outbox."""

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from weakref import WeakValueDictionary

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .services import DomainError, TicketStore
from .storage import IdempotencyRecord, TicketRepository, timestamp


class IdempotencyRepository:
    _local_locks: WeakValueDictionary = WeakValueDictionary()

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        ttl_seconds: int = 86400,
    ) -> None:
        self.sessions = sessions
        self.ttl_seconds = ttl_seconds

    @asynccontextmanager
    async def _lock(self, session: AsyncSession, key: str):
        engine = self.sessions.kw["bind"]
        if engine.dialect.name == "postgresql":
            # A transaction lock protects even the first request, before any row
            # exists. Hash collisions only serialize otherwise unrelated keys.
            lock_id = int.from_bytes(
                hashlib.blake2b(key.encode(), digest_size=8).digest(), signed=True
            )
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_id}
            )
            yield
        else:
            # SQLite is for local tests/development, not multi-process hosting.
            lock_key = (asyncio.get_running_loop(), id(engine), key)
            lock = self._local_locks.setdefault(lock_key, asyncio.Lock())
            async with lock:
                yield

    async def execute(
        self,
        key: str,
        request_data: dict,
        operation: Callable[[TicketStore], Awaitable[str]],
    ) -> str:
        fingerprint = hashlib.sha256(
            json.dumps(request_data, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        async with self.sessions() as session:
            # Keep the local lock until COMMIT, just like the PostgreSQL lock.
            async with self._lock(session, key):
                try:
                    now = datetime.now(UTC)
                    row = await session.get(IdempotencyRecord, key)
                    if row is not None and timestamp(row.expires_at) > now:
                        if row.request_hash != fingerprint:
                            raise DomainError(
                                "Idempotency key was used with different data", 409
                            )
                        ticket_id = row.ticket_id
                    else:
                        tickets = TicketRepository(self.sessions, transaction=session)
                        ticket_id = await operation(tickets)
                        if row is None:
                            row = IdempotencyRecord(key=key)
                            session.add(row)
                        completed_at = datetime.now(UTC)
                        row.request_hash = fingerprint
                        row.ticket_id = ticket_id
                        row.created_at = completed_at
                        row.expires_at = completed_at + timedelta(
                            seconds=self.ttl_seconds
                        )
                    await session.commit()
                    return ticket_id
                except BaseException:
                    await session.rollback()
                    raise
