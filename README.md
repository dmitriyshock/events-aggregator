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

## API

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
