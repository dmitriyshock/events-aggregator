import asyncio
from datetime import date
from unittest.mock import AsyncMock

import pytest

from events_aggregator.provider import EventsPaginator, ProviderError


def collect(paginator):
    async def run():
        return [event async for event in paginator]

    return asyncio.run(run())


def test_paginator_yields_every_event_and_follows_cursor():
    client = AsyncMock()
    client.events_page.side_effect = [
        {"results": [{"id": "1"}, {"id": "2"}], "next": "/api/events/?cursor=two"},
        {"results": [{"id": "3"}], "next": None},
    ]
    changed_at = date(2026, 9, 25)
    assert collect(EventsPaginator(client, changed_at)) == [
        {"id": "1"},
        {"id": "2"},
        {"id": "3"},
    ]
    assert client.events_page.await_args_list[0].args == (changed_at, None)
    assert client.events_page.await_args_list[1].args == (
        changed_at,
        "/api/events/?cursor=two",
    )


def test_paginator_stops_on_empty_cursor():
    client = AsyncMock()
    client.events_page.return_value = {"results": [], "next": ""}
    assert collect(EventsPaginator(client, date.today())) == []
    client.events_page.assert_awaited_once()


def test_paginator_rejects_repeated_cursor():
    client = AsyncMock()
    client.events_page.side_effect = [
        {"results": [], "next": "?cursor=repeat"},
        {"results": [], "next": "?cursor=repeat"},
    ]
    with pytest.raises(ProviderError, match="pagination cursor"):
        collect(EventsPaginator(client, date.today()))
    assert client.events_page.await_count == 2


def test_paginator_rejects_malformed_results():
    client = AsyncMock()
    client.events_page.return_value = {"results": {"id": "1"}, "next": None}
    with pytest.raises(ProviderError, match="invalid events"):
        collect(EventsPaginator(client, date.today()))
