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
from app.ingestion.series_registry import SERIES, SERIES_IDS
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


async def _seed_all_buffers() -> dict[str, deque]:
    """Retries with backoff instead of dying on the first failure — mirrors AD-13's
    "TimescaleDB unavailable, retry, don't crash" stance and AD-22's own topic-not-ready retry.

    Found by testing the actual startup ordering, not by reading the code: `backend` only depends
    on redpanda/timescaledb being *healthy* (docker-compose.yml), not on `migrate` having *finished*
    creating raw_metrics/nab_anomaly_windows. A one-shot query here raised `UndefinedTable` on a
    fresh stack where backend and migrate start concurrently, which killed this task permanently —
    silently: no log, `/health` stayed 200, and the WebSocket kept accepting connections that never
    received a single live update for the rest of the container's life, even long after migrate
    finished and real data started flowing (confirmed directly: reproduced with migrate skipped
    entirely, then ran migrate + a producer replay afterward and confirmed the already-started
    backend's live feed stayed dead).
    """
    delay = 0.5
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(settings.database_url) as conn:
                return {
                    series.series_id: await _seed_buffer(conn, series.series_id) for series in SERIES
                }
        except psycopg.Error as exc:
            print(f"live-feed: DB not ready for buffer seeding yet ({exc}), retrying in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)


def _score_input_row(buffer: deque, series_id: str) -> pd.DataFrame | None:
    """Runs the *same* app.ml.features.compute_features used offline (AD-21) on this series'
    buffer, returning its last row (the just-arrived point) or None if it doesn't survive
    compute_features' own drop rules (e.g. a gap directly preceding it, AD-15).

    Synchronous, CPU-bound pandas work — measured at ~1.1ms on this project's real buffer size
    (~25 rows at NAB's 5-10min cadence over BUFFER_MINUTES). The same order of magnitude as
    model.predict()'s own measured ~2.2ms (AD-21), so the caller runs this via asyncio.to_thread()
    too, for the same reason: non-negligible work has no business blocking the event loop.
    """
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


async def _process_message(app_state, connections: ConnectionManager, buffers: dict, msg) -> None:
    try:
        payload = parse_message(msg.value)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        # Same stance as the ingestion consumer (AD-13): a message that can never be scored is
        # skipped, not allowed to crash a task nothing else depends on.
        return

    series_id = payload["series_id"]
    if series_id not in SERIES_IDS:
        # Unlike the REST/WebSocket endpoints (which validate series_id against the same
        # registry, app/ingestion/series_registry.SERIES_IDS, before doing anything), this loop
        # previously accepted *any* series_id from the topic via buffers.setdefault(...) — a
        # malformed or stray producer message would silently grow `buffers` with a junk entry
        # for the rest of the container's life. Confirmed directly: published a message with a
        # bogus series_id straight to the topic and it was accepted with zero trace anywhere.
        print(f"live-feed: skipping message for unknown series_id {series_id!r}")
        return

    buffer = buffers.setdefault(series_id, deque())
    buffer.append({"time": pd.Timestamp(payload["timestamp"]), "value": payload["value"]})
    cutoff = buffer[-1]["time"] - timedelta(minutes=BUFFER_MINUTES)
    while buffer and buffer[0]["time"] < cutoff:
        buffer.popleft()

    model = app_state.model
    is_anomaly = None
    if model is not None:
        # _score_input_row is synchronous pandas work (~1.1ms measured, see its docstring) and
        # model.predict is a synchronous sklearn call (~2ms, AD-21) — both run off the event loop.
        last_row = await asyncio.to_thread(_score_input_row, buffer, series_id)
        if last_row is not None:
            x = last_row[FEATURE_NAMES].to_numpy()
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


async def _consume_forever(app_state, connections: ConnectionManager, buffers: dict) -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await consumer.start()
    try:
        await _assign_at_tip(consumer)
        async for msg in consumer:
            try:
                await _process_message(app_state, connections, buffers, msg)
            except Exception as exc:
                # One message's worth of scoring/broadcast failing (e.g. a genuine bug, not a
                # dead socket — ConnectionManager.broadcast already handles dead sockets itself)
                # must not take the whole live feed down for every series' every future client.
                print(f"live-feed: failed to process a message, skipping: {exc}")
    finally:
        await consumer.stop()


async def run_live_feed(app_state, connections: ConnectionManager) -> None:
    """`app_state` is FastAPI's `app.state` (Starlette `State`) — read fresh each iteration
    (`app_state.model`, not a value captured once at task start) so this task always sees whatever
    main.py's lifespan currently has loaded, per AD-21."""
    buffers = await _seed_all_buffers()

    # `async for msg in consumer:` itself (as opposed to per-message handling, already guarded in
    # _consume_forever) has no exception handling of its own: if the underlying fetch loop ever
    # raises instead of retrying internally, the whole task would die silently, exactly like the
    # buffer-seeding and topic-assignment races already fixed above. Not reproduced despite trying
    # (a `docker compose restart redpanda` and a 40s full stop both recovered on their own via
    # aiokafka's internal reconnect logic) — fixed anyway, as defense in depth, for consistency
    # with the retry-don't-die stance this same file already applies twice.
    delay = 0.5
    while True:
        try:
            await _consume_forever(app_state, connections, buffers)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"live-feed: consumer loop failed ({exc}), restarting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)
