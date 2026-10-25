# Events Aggregator

Asynchronous FastAPI service that synchronizes provider events and manages ticket reservations.

## Setup

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/). Install dependencies and activate the project environment:

```powershell
uv sync
```

Set these environment variables before running the service:

- `DATABASE_URL` or `POSTGRES_CONNECTION_STRING` — PostgreSQL URL, such as `postgresql+asyncpg://user:password@localhost:5432/events`. The LMS `events-aggregator` template provides `POSTGRES_CONNECTION_STRING`.
- `EVENTS_PROVIDER_API_KEY` — API key for the events provider. Keep it secret and never return it from the API.
- `EVENTS_PROVIDER_BASE_URL` — provider API base URL. The default is the internal cluster address; for local development set `http://events-provider.dev-2.python-labs.ru`.
- `SYNC_INTERVAL_SECONDS` — optional automatic sync interval in seconds (positive integer; default `86400`).
- `LOG_LEVEL` — optional logging level (default `INFO`).

Run the API locally:

```powershell
uv run uvicorn events_aggregator.main:app --reload
```

## Part 2: durable notifications

Ticket creation commits the ticket and a `ticket.purchased` outbox record in one
PostgreSQL transaction. The record includes the ticket ID, event name, seat,
notification message and a stable notification idempotency key.

The FastAPI lifespan starts the outbox worker as a background coroutine in the
same process as the API. The Docker command starts both automatically. Shutdown
cancels and awaits both the synchronization and outbox tasks before closing the
database. PostgreSQL `FOR UPDATE SKIP LOCKED` coordinates multiple instances.

Configuration: `OUTBOX_INTERVAL_SECONDS` (5), `OUTBOX_BATCH_SIZE` (50),
`OUTBOX_RETRY_BASE_SECONDS` (5), `OUTBOX_RETRY_MAX_SECONDS` (300).
Failed sends remain pending and retry with capped exponential backoff, without
discarding records.

The worker sends `POST /api/notifications` to Capashino with `X-API-Key`,
`message`, `reference_id=ticket_id` and the persisted notification idempotency
key. Retries reuse that same key, including after process restarts or an
interrupted database commit. Only a confirmed HTTP 201 marks a record sent.
Errors, including HTTP 409, remain pending; a conflict alone does not prove
successful delivery.

- `CAPASHINO_API_KEY`: portal profile API token; set in the service environment.
- `CAPASHINO_BASE_URL`: defaults to the internal cluster service; use
  `https://capashino.dev-2.python-labs.ru` for local access.
- `CAPASHINO_TIMEOUT_SECONDS`: request timeout (10 seconds).

Without the Capashino key the API remains available and notifications stay in
the outbox until configuration is supplied and the service restarted.

## API endpoints

- `GET /api/health` — service health.
- `POST /api/sync/trigger` — trigger event synchronization.
- `GET /api/events` — list events.
- `GET /api/events/{event_id}` — event details.
- `GET /api/events/{event_id}/seats` — event seat availability.
- `POST /api/tickets` — reserve tickets.
- `DELETE /api/tickets/{ticket_id}` — cancel a ticket reservation.

## Checks

```powershell
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Docker

Build and run the container with `PORT` defaulting to `8000`:

```powershell
docker build -t events-aggregator .
docker run --rm -p 8000:8000 -e DATABASE_URL=... -e EVENTS_PROVIDER_API_KEY=... -e EVENTS_PROVIDER_BASE_URL=... events-aggregator
```
