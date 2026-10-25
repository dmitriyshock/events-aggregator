"""Durable notification delivery; PostgreSQL row locks coordinate workers."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .storage import OutboxEvent

logger = logging.getLogger(__name__)


class NotificationSender(Protocol):
    async def send(self, payload: dict) -> None: ...


class PendingNotificationSender:
    async def send(self, payload: dict) -> None:
        raise RuntimeError("Notification transport is not configured")


class OutboxWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        sender: NotificationSender,
        *,
        interval: float = 5,
        batch_size: int = 50,
        retry_base: float = 5,
        retry_max: float = 300,
    ) -> None:
        self.sessions = sessions
        self.sender = sender
        self.interval = interval
        self.batch_size = batch_size
        self.retry_base = retry_base
        self.retry_max = retry_max

    async def process_one(self) -> bool:
        async with self.sessions.begin() as session:
            row = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.status == "pending",
                    OutboxEvent.next_attempt_at <= datetime.now(UTC),
                )
                .order_by(OutboxEvent.next_attempt_at, OutboxEvent.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if row is None:
                return False
            row.attempts += 1
            try:
                await self.sender.send(row.payload)
            except Exception as exc:
                # Do not persist response bodies, request headers or credentials.
                row.last_error = type(exc).__name__
                delay = min(
                    self.retry_max,
                    self.retry_base * 2 ** min(row.attempts - 1, 16),
                )
                row.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
                logger.warning(
                    "Outbox delivery failed: id=%s attempt=%s error=%s",
                    row.id,
                    row.attempts,
                    row.last_error,
                )
            else:
                row.status = "sent"
                row.sent_at = datetime.now(UTC)
                row.last_error = None
            return True

    async def run(self) -> None:
        while True:
            try:
                for _ in range(self.batch_size):
                    if not await self.process_one():
                        break
            except Exception:
                logger.exception("Outbox iteration failed")
            await asyncio.sleep(self.interval)
