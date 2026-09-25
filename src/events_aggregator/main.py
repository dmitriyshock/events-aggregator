"""HTTP API and application lifecycle."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from events_aggregator.provider import EventsProviderClient, ProviderError
from events_aggregator.services import DomainError, SeatsCache, TicketService
from events_aggregator.storage import (
    EventRepository,
    SyncRepository,
    TicketRepository,
    create_schema,
)
from events_aggregator.sync import SyncService

logger = logging.getLogger(__name__)


class TicketCreate(BaseModel):
    event_id: str
    first_name: str = Field(min_length=1)
    last_name: str = Field(min_length=1)
    email: EmailStr
    seat: str = Field(pattern=r"^[A-Z][1-9][0-9]*$")

    @field_validator("first_name", "last_name")
    @classmethod
    def nonblank_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Name cannot be blank")
        return value


def _database_url() -> str:
    value = os.environ["DATABASE_URL"]
    if value.startswith("postgres://"):
        value = "postgresql+asyncpg://" + value[len("postgres://") :]
    elif value.startswith("postgresql://"):
        value = "postgresql+asyncpg://" + value[len("postgresql://") :]
    return value


async def _daily_sync(app: FastAPI) -> None:
    interval = int(os.getenv("SYNC_INTERVAL_SECONDS", "86400"))
    while True:
        try:
            async with app.state.sync_lock:
                await app.state.sync_service.run()
        except Exception:
            logger.exception("Scheduled event sync failed")
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    engine = create_async_engine(_database_url(), pool_pre_ping=True)
    await create_schema(engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    provider = EventsProviderClient(
        os.environ["EVENTS_PROVIDER_API_KEY"],
        os.getenv(
            "EVENTS_PROVIDER_BASE_URL",
            "http://student-system-events-provider-web.student-system-events-provider.svc:8000",
        ),
    )
    events = EventRepository(sessions)
    tickets = TicketRepository(sessions)
    sync_state = SyncRepository(sessions)
    app.state.engine = engine
    app.state.events = events
    app.state.ticket_service = TicketService(events, tickets, provider, SeatsCache())
    app.state.sync_service = SyncService(events, sync_state, provider)
    app.state.sync_lock = asyncio.Lock()
    worker = asyncio.create_task(_daily_sync(app))
    try:
        yield
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        await provider.aclose()
        await engine.dispose()


app = FastAPI(title="Events Aggregator", lifespan=lifespan)


@app.exception_handler(DomainError)
async def domain_error_handler(_request: Request, exc: DomainError):
    from fastapi.responses import JSONResponse

    return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)


@app.exception_handler(ProviderError)
async def provider_error_handler(_request: Request, exc: ProviderError):
    from fastapi.responses import JSONResponse

    status = 404 if exc.status_code == 404 else 409 if exc.status_code == 400 else 502
    return JSONResponse({"detail": str(exc)}, status_code=status)


@app.get("/api/health")
async def health(request: Request) -> dict:
    try:
        async with request.app.state.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return {"status": "ok"}


@app.post("/api/sync/trigger")
async def trigger_sync(request: Request) -> dict:
    async with request.app.state.sync_lock:
        return await request.app.state.sync_service.run()


@app.get("/api/events")
async def list_events(
    request: Request,
    date_from: date | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict:
    count, results = await request.app.state.events.list(
        date_from=date_from, page=page, page_size=page_size
    )
    base = request.url.remove_query_params("page").include_query_params(
        page_size=page_size
    )
    next_url = (
        str(base.include_query_params(page=page + 1))
        if page * page_size < count
        else None
    )
    previous_url = str(base.include_query_params(page=page - 1)) if page > 1 else None
    return {
        "count": count,
        "next": next_url,
        "previous": previous_url,
        "results": results,
    }


@app.get("/api/events/{event_id}")
async def event_detail(event_id: str, request: Request) -> dict:
    event = await request.app.state.events.get(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.get("/api/events/{event_id}/seats")
async def available_seats(event_id: str, request: Request) -> dict:
    seats = await request.app.state.ticket_service.available_seats(event_id)
    return {"event_id": event_id, "available_seats": seats}


@app.post("/api/tickets", status_code=201)
async def create_ticket(payload: TicketCreate, request: Request) -> dict:
    ticket_id = await request.app.state.ticket_service.register(
        payload.event_id,
        payload.first_name.strip(),
        payload.last_name.strip(),
        str(payload.email),
        payload.seat,
    )
    return {"ticket_id": ticket_id}


@app.delete("/api/tickets/{ticket_id}")
async def delete_ticket(ticket_id: str, request: Request) -> dict:
    return {"success": await request.app.state.ticket_service.unregister(ticket_id)}
