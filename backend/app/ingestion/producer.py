"""Replays the NAB CSVs to Kafka in file order, one row = one message (PLANNING.md AD-12).

Event time (the CSV row's own timestamp) and replay pacing (wall clock) are two separate
variables: `now()` never touches the message payload, it only controls how fast rows are sent.
"""

import asyncio
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from app.config import settings
from app.ingestion.series_registry import SERIES

TOPIC = "raw-metrics"
DATA_DIR = Path(os.environ.get("NAB_DATA_DIR", "/data"))
REPLAY_SPEED = float(os.environ.get("REPLAY_SPEED", "0"))  # 0 = as fast as possible


def parse_ts(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


async def ensure_topic() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=TOPIC, num_partitions=len(SERIES), replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()


async def replay_series(producer: AIOKafkaProducer, series) -> int:
    path = DATA_DIR / series.csv_path
    key = series.series_id.encode()
    prev_ts: datetime | None = None
    count = 0
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            ts = parse_ts(row["timestamp"])
            if REPLAY_SPEED > 0 and prev_ts is not None:
                delay = (ts - prev_ts).total_seconds() / REPLAY_SPEED
                if delay > 0:
                    await asyncio.sleep(delay)
            payload = {
                "series_id": series.series_id,
                "timestamp": ts.isoformat(),
                "value": float(row["value"]),
            }
            await producer.send_and_wait(TOPIC, key=key, value=json.dumps(payload).encode())
            prev_ts = ts
            count += 1
    return count


async def main() -> None:
    await ensure_topic()
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, acks="all")
    await producer.start()
    try:
        for series in SERIES:
            n = await replay_series(producer, series)
            print(f"producer: {series.series_id}: replayed {n} rows")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
