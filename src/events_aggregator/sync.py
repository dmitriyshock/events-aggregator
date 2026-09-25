"""Incremental provider to local catalogue synchronization."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import Any

from .provider import EventsPaginator, EventsProviderClient
from .storage import EventRepository, SyncRepository, timestamp

logger = logging.getLogger(__name__)


class SyncService:
    def __init__(
        self,
        event_repo: EventRepository,
        sync_repo: SyncRepository,
        client: EventsProviderClient,
        *,
        initial_date: date = date(2000, 1, 1),
        batch_size: int = 100,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.event_repo = event_repo
        self.sync_repo = sync_repo
        self.client = client
        self.initial_date = initial_date
        self.batch_size = batch_size

    async def run(self) -> dict[str, Any]:
        async with self.sync_repo.sync_lock() as acquired:
            if not acquired:
                return {"synced_events": 0, "last_changed_at": None, "skipped": True}

            await self.sync_repo.mark_started()
            try:
                checkpoint = await self.sync_repo.get_state()
                changed_at_date = checkpoint.date() if checkpoint else self.initial_date
                batch: list[dict[str, Any]] = []
                latest: datetime | None = checkpoint
                count = 0

                async for event in EventsPaginator(self.client, changed_at_date):
                    # Fail before persisting a malformed item or advancing the marker.
                    changed_at = timestamp(event.get("changed_at"))
                    if changed_at is None:
                        raise ValueError("Provider event has no changed_at")
                    latest = max(latest, changed_at) if latest else changed_at
                    batch.append(event)
                    if len(batch) >= self.batch_size:
                        await self.event_repo.upsert_many(batch)
                        count += len(batch)
                        batch = []

                if batch:
                    await self.event_repo.upsert_many(batch)
                    count += len(batch)

                # Provider filtering is date-based, so replay the checkpoint day.
                # The marker and success status commit together after all pages.
                await self.sync_repo.mark_succeeded(latest)
                return {
                    "synced_events": count,
                    "last_changed_at": latest.isoformat() if latest else None,
                    "skipped": False,
                }
            except Exception as error:
                logger.exception("Events synchronization failed")
                await self.sync_repo.mark_failed(error)
                raise


async def run_daily(service: SyncService, stop: asyncio.Event) -> None:
    """Run immediately, then every 24 hours until stopped."""
    while not stop.is_set():
        await service.run()
        try:
            await asyncio.wait_for(stop.wait(), timeout=24 * 60 * 60)
        except TimeoutError:
            pass
