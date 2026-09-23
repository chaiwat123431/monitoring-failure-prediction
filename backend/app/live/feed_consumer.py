"""Background live-inference task (PLANNING.md AD-22): a second, independent reader of
`raw-metrics` — separate from the ingestion `consumer` service (Slice 2, unchanged), which keeps
durably upserting rows into TimescaleDB exactly as before. This task only pushes already-durable
data to whoever is watching right now.

No consumer group, no committed offsets: the rolling per-series buffers below are rebuilt from
TimescaleDB on every start, so resuming from a saved Kafka offset would only replay stale rows and
broadcast them as though they were live.
"""

import asyncio
import json
from collections import deque
from datetime import timedelta

import pandas as pd
import psycopg
from aiokafka import AIOKafkaConsumer, TopicPartition

from app.config import settings
from app.ingestion.consumer import parse_message
from app.ingestion.series_registry import SERIES
from app.live.broadcaster import ConnectionManager
from app.ml.features import FEATURE_NAMES, WINDOW_MINUTES, compute_features

TOPIC = "raw-metrics"

# Retained history per series is a multiple of the feature window, not exactly WINDOW_MINUTES.
# compute_features' leading-edge guard (AD-15) measures a row's distance from the *start of the
# frame it's given* — if the buffer held exactly WINDOW_MINUTES, the newest row would sit right at
# that boundary and ordinary sampling jitter could drop it. The margin only changes which rows
# compute_features considers "too close to the start of this slice"; the actual trailing-window
# math (rolling mean/std/rate-of-change) only ever looks at the last WINDOW_MINUTES regardless of
# how much extra history precedes it.
BUFFER_MINUTES = WINDOW_MINUTES * 2


async def _seed_buffer(conn: psycopg.AsyncConnection, series_id: str) -> deque:
    """Anchored to this series' own latest ingested timestamp, never wall-clock now() — the NAB
    data's event time (AD-12) is from 2014; real time is not."""
    async with conn.cursor() as cur:
        await cur.execute("SELECT max(time) FROM raw_metrics WHERE series_id = %s", (series_id,))
        (latest,) = await cur.fetchone()
        if latest is None:
            return deque()
        since = latest - timedelta(minutes=BUFFER_MINUTES)
        await cur.execute(
            "SELECT time, value FROM raw_metrics WHERE series_id = %s AND time >= %s ORDER BY time",
            (series_id, since),
        )
        rows = await cur.fetchall()
    return deque({"time": pd.Timestamp(t), "value": v} for t, v in rows)


def _score_input_row(buffer: deque, series_id: str) -> pd.DataFrame | None:
    """Runs the *same* app.ml.features.compute_features used offline (AD-21) on this series'
    buffer, returning its last row (the just-arrived point) or None if it doesn't survive
    compute_features' own drop rules (e.g. a gap directly preceding it, AD-15)."""
    df = pd.DataFrame(
        {
            "series_id": series_id,
            "time": [r["time"] for r in buffer],
            "value": [r["value"] for r in buffer],
        }
    )
    out = compute_features(df)
    if out.empty:
        return None
    return out.iloc[[-1]]


async def _assign_at_tip(consumer: AIOKafkaConsumer) -> None:
    """Manual (groupless) assignment to every partition of TOPIC, positioned at the current end —
    this task only ever sees messages produced from here on (AD-22).

    Retries until the topic actually exists: `backend` only depends on redpanda/timescaledb being
    healthy (docker-compose.yml), not on the producer having run yet, so on a fresh stack this task
    starts before the topic is created. A one-shot `partitions_for_topic()` would silently assign
    to zero partitions in that case and never notice once the topic showed up later — found by
    running this exact sequence against the real stack (empty DB, backend started before the
    producer), not by reading the code.
    """
    delay = 0.5
    while True:
        await consumer.topics()  # forces a metadata fetch so partitions_for_topic below isn't stale
        partitions = consumer.partitions_for_topic(TOPIC)
        if partitions:
            break
        print(f"live-feed: topic {TOPIC!r} not available yet, retrying in {delay}s")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 10)

    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]
    consumer.assign(topic_partitions)
    await consumer.seek_to_end()


async def run_live_feed(app_state, connections: ConnectionManager) -> None:
    """`app_state` is FastAPI's `app.state` (Starlette `State`) — read fresh each iteration
    (`app_state.model`, not a value captured once at task start) so this task always sees whatever
    main.py's lifespan currently has loaded, per AD-21."""
    buffers: dict[str, deque] = {}
    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn:
        for series in SERIES:
            buffers[series.series_id] = await _seed_buffer(conn, series.series_id)

    consumer = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await consumer.start()
    try:
        await _assign_at_tip(consumer)

        async for msg in consumer:
            try:
                payload = parse_message(msg.value)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                # Same stance as the ingestion consumer (AD-13): a message that can never be
                # scored is skipped, not allowed to crash a task nothing else depends on.
                continue

            series_id = payload["series_id"]
            buffer = buffers.setdefault(series_id, deque())
            buffer.append({"time": pd.Timestamp(payload["timestamp"]), "value": payload["value"]})
            cutoff = buffer[-1]["time"] - timedelta(minutes=BUFFER_MINUTES)
            while buffer and buffer[0]["time"] < cutoff:
                buffer.popleft()

            model = app_state.model
            is_anomaly = None
            if model is not None:
                last_row = _score_input_row(buffer, series_id)
                if last_row is not None:
                    x = last_row[FEATURE_NAMES].to_numpy()
                    # model.predict is a synchronous, CPU-bound scikit-learn call — measured at
                    # ~2ms/row against the real artifact (PLANNING.md AD-21). Run off the event
                    # loop so it can't stall other WebSocket connections or /health in the meantime.
                    y_pred = await asyncio.to_thread(model.predict, x)
                    is_anomaly = bool(y_pred[0] == -1)

            await connections.broadcast(
                series_id,
                {
                    "series_id": series_id,
                    "timestamp": payload["timestamp"],
                    "value": payload["value"],
                    "is_anomaly": is_anomaly,
                    "model_loaded": model is not None,
                },
            )
    finally:
        await consumer.stop()
