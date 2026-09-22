"""Consumes raw-metrics and upserts into TimescaleDB (PLANNING.md AD-13).

Idempotent on redelivery: PRIMARY KEY (series_id, time) + ON CONFLICT DO NOTHING. The Kafka
offset is committed only after the DB write for that message has succeeded, so a crash between
write and commit just reprocesses that row into a no-op on restart. If TimescaleDB is down, the
write is retried with backoff and the offset is not advanced — consumption stalls, nothing is
dropped, and it catches up once the DB comes back.

A message that can never succeed — unparseable JSON, a missing field, or a value the DB rejects
outright (not a connectivity error) — is logged and skipped (offset committed) rather than
retried forever or left to crash the process: retrying a message that is itself the problem does
not help, and blocks every message behind it indefinitely.
"""

import asyncio
import json

import psycopg
from aiokafka import AIOKafkaConsumer

from app.config import settings

TOPIC = "raw-metrics"
GROUP_ID = "ingestion-consumer"

UPSERT_SQL = """
INSERT INTO raw_metrics (time, series_id, value)
VALUES (%(timestamp)s, %(series_id)s, %(value)s)
ON CONFLICT (series_id, time) DO NOTHING
"""


class ReconnectingConn:
    """A dropped connection to a dead DB stays dead — retrying execute() on it forever never
    succeeds even once the DB is back. This holds the current connection and replaces it with a
    fresh one whenever a write fails, so retries actually have a chance to reconnect."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn: psycopg.AsyncConnection | None = None

    async def get(self) -> psycopg.AsyncConnection:
        if self._conn is None or self._conn.closed:
            self._conn = await psycopg.AsyncConnection.connect(self._dsn, autocommit=True)
        return self._conn

    def discard(self) -> None:
        self._conn = None

    async def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            await self._conn.close()


def parse_message(raw_bytes: bytes) -> dict:
    """Raises json.JSONDecodeError / KeyError / TypeError / ValueError on anything malformed —
    the caller treats all of those as "skip this message", never as a reason to crash."""
    raw = json.loads(raw_bytes)
    return {
        "series_id": raw["series_id"],
        "timestamp": raw["timestamp"],
        "value": float(raw["value"]),
    }


async def write_with_retry(db: ReconnectingConn, payload: dict) -> None:
    """Retries forever on connectivity failures (OperationalError/OSError) — those recover once
    the DB comes back. Any other psycopg.Error (e.g. a value the column rejects) means the *data*
    is the problem, not the connection: retrying it changes nothing, so it's raised for the
    caller to skip-and-log instead of looping forever on an unwritable message."""
    delay = 0.5
    while True:
        try:
            conn = await db.get()
            await conn.execute(UPSERT_SQL, payload)
            return
        except (psycopg.OperationalError, OSError) as exc:
            print(f"consumer: DB write failed, retrying in {delay}s: {exc}")
            db.discard()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


async def main() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    db = ReconnectingConn(settings.database_url)
    written = 0
    skipped = 0
    try:
        async for msg in consumer:
            where = f"partition={msg.partition} offset={msg.offset}"
            try:
                payload = parse_message(msg.value)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                print(f"consumer: skipping unparseable message ({where}): {exc}")
                await consumer.commit()
                skipped += 1
                continue

            try:
                await write_with_retry(db, payload)
            except psycopg.Error as exc:
                print(f"consumer: skipping message the DB permanently rejected ({where}): {exc}")
                await consumer.commit()
                skipped += 1
                continue

            await consumer.commit()
            written += 1
            if written % 500 == 0:
                print(f"consumer: {written} rows written")
    finally:
        print(f"consumer: shutting down, {written} rows written, {skipped} skipped this run")
        await consumer.stop()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
