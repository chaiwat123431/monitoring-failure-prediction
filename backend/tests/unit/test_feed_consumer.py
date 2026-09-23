"""Found by a second /code-review pass on PR #4 (Slice 4), all reproduced against the real stack
before fixing (see PLANNING.md §7): an unknown series_id was silently accepted into the live-feed's
buffers forever, and the outer Kafka-fetch loop had no retry of its own."""

import asyncio
import json

import pytest

from app.ingestion.series_registry import SERIES
from app.live import feed_consumer
from app.live.broadcaster import ConnectionManager


class FakeAppState:
    def __init__(self, model=None):
        self.model = model


class FakeMsg:
    def __init__(self, series_id: str, timestamp: str, value: float):
        self.value = json.dumps(
            {"series_id": series_id, "timestamp": timestamp, "value": value}
        ).encode()


async def test_process_message_skips_an_unknown_series_id():
    buffers: dict = {}

    await feed_consumer._process_message(
        FakeAppState(), ConnectionManager(), buffers, FakeMsg("bogus/unknown", "2020-01-01T00:00:00+00:00", 1.0)
    )

    assert buffers == {}


async def test_process_message_buffers_a_known_series_id():
    buffers: dict = {}
    series_id = SERIES[0].series_id

    await feed_consumer._process_message(
        FakeAppState(), ConnectionManager(), buffers, FakeMsg(series_id, "2020-01-01T00:00:00+00:00", 42.0)
    )

    assert series_id in buffers
    assert list(buffers[series_id])[-1]["value"] == 42.0


async def test_run_live_feed_restarts_the_consume_loop_after_an_unexpected_failure(monkeypatch):
    """AD-22: the outer `async for msg in consumer` loop has no exception handling of its own
    (only per-message processing does). Not reproduced with a real broker restart/outage (aiokafka
    retried internally both times), but fixed for consistency with the retry-don't-die stance this
    file already applies to buffer seeding and topic assignment — this locks that behavior in."""
    calls = []

    async def fake_seed_all_buffers():
        return {}

    async def fake_consume_forever(app_state, connections, buffers):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("simulated Kafka fetch failure")
        raise asyncio.CancelledError()  # stop the loop cleanly once the retry is proven

    monkeypatch.setattr(feed_consumer, "_seed_all_buffers", fake_seed_all_buffers)
    monkeypatch.setattr(feed_consumer, "_consume_forever", fake_consume_forever)

    with pytest.raises(asyncio.CancelledError):
        await feed_consumer.run_live_feed(FakeAppState(), ConnectionManager())

    assert len(calls) == 2
