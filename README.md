# Events Aggregator

Asynchronous FastAPI service that synchronizes provider events and manages ticket reservations.

## Setup

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/). Install dependencies and activate the project environment:

```powershell
uv sync
```

Set these environment variables before running the service:

For local development, `.env.example` lists all settings with placeholder
credentials. Copy it to `.env` and pass it explicitly with `uv run --env-file .env`.
The production container reads its environment directly from LMS.

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

### Idempotent ticket registration

`POST /api/tickets` accepts an optional `idempotency_key` in its JSON body:

```json
{
  "event_id": "event-uuid",
  "first_name": "Ada",
  "last_name": "Lovelace",
  "email": "ada@example.com",
  "seat": "A1",
  "idempotency_key": "client-generated-unique-key"
}
```

The key must be a nonblank string of 1–255 characters. Repeating the same key
and registration data returns HTTP 201 with the original `ticket_id`, without
calling Events Provider or producing another outbox event. Changed registration
data with an existing key returns HTTP 409. Validation failures return HTTP 400.
Without a key, every request is a new registration, preserving part 1 behavior.

PostgreSQL transaction advisory locks serialize parallel requests for the same
key across API instances. The successful result, ticket and outbox record commit
in the same local transaction. Provider failures leave the key reusable.
`IDEMPOTENCY_TTL_SECONDS` sets retention (86400 seconds by default). Expired keys
are replaced only on a new successful registration; no automatic purge is needed.
Clients must use a new key for a different purchase and avoid reusing expired keys
to retry an old purchase. Request comparison uses validated registration values.

The upstream registration API does not offer a transaction shared with our
database. A hard process kill after upstream registration but before the local
commit still requires reconciliation; normal local persistence failures trigger
an upstream cancellation. The outbox covers durable notification delivery once
the local purchase transaction commits.

### Endpoints

- `GET /api/health` — service health.
- `POST /api/sync/trigger` — trigger event synchronization.
- `GET /api/events` — list events.
- `GET /api/events/{event_id}` — event details.
- `GET /api/events/{event_id}/seats` — event seat availability.
- `POST /api/tickets` — reserve tickets.
- `DELETE /api/tickets/{ticket_id}` — cancel a ticket reservation.

## GlitchTip

Request the `glitchtip` resource on the new LMS service's Resources tab. After
approval, provision the service again if requested by the platform. The platform
adds `SENTRY_DSN` to its environment; `GLITCHTIP_DSN` is also accepted. Redeploy
after configuration changes. Never put the DSN or API tokens in the repository.

The Sentry SDK is initialized during application startup with explicit FastAPI
and Starlette integrations. Unhandled request exceptions are captured
automatically. Request bodies, local variables, cookies and API key headers are
excluded from error events. `APP_ENVIRONMENT` defaults to `production` and
`APP_RELEASE` may identify the deployed version. Performance tracing is disabled.

For verification, temporarily set `ENABLE_ERROR_TEST_ENDPOINT=true` and redeploy.
Call `GET /api/debug/error` with `X-API-Key` set to the service's provider API key.
The deliberate HTTP 500 should appear in the service's GlitchTip project as
`Events Aggregator GlitchTip verification`. Set the flag back to `false` and
redeploy after confirming it in the UI. The endpoint returns 404 when disabled
or when the key does not match.

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
