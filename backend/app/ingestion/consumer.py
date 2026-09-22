"""Consumes raw-metrics and upserts into TimescaleDB (PLANNING.md AD-13).

Idempotent on redelivery: PRIMARY KEY (series_id, time) + ON CONFLICT DO NOTHING. The Kafka
offset is committed only after the DB write for that message has succeeded, so a crash between
write and commit just reprocesses that row into a no-op on restart. If TimescaleDB is down, the
write is retried with backoff and the offset is not advanced — consumption stalls, nothing is
dropped, and it catches up once the DB comes back.
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


async def write_with_retry(db: ReconnectingConn, payload: dict) -> None:
    delay = 0.5
    while True:
        try:
            conn = await db.get()
            await conn.execute(UPSERT_SQL, payload)
            return
        except (psycopg.Error, OSError) as exc:
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
    try:
        async for msg in consumer:
            payload = json.loads(msg.value)
            await write_with_retry(db, payload)
            await consumer.commit()
            written += 1
            if written % 500 == 0:
                print(f"consumer: {written} rows written")
    finally:
        print(f"consumer: shutting down, {written} rows written this run")
        await consumer.stop()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
