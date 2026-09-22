import asyncio

import psycopg
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import APIRouter, Response

from app.config import settings

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    """Liveness: process is up. No I/O. Must never fail because a dependency is down."""
    return {"status": "ok"}


async def _check_database() -> str:
    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn:
        await conn.execute("SELECT 1")
    return "ok"


async def _check_kafka() -> str:
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    try:
        await producer.start()
    finally:
        await producer.stop()
    return "ok"


async def _run_check(coro) -> str:
    try:
        return await asyncio.wait_for(coro, timeout=settings.ready_check_timeout_seconds)
    except asyncio.TimeoutError:
        return "error: timeout"
    except (psycopg.Error, KafkaError, OSError) as exc:
        return f"error: {exc}"


@router.get("/health/ready")
async def ready(response: Response) -> dict:
    """Readiness: checks TimescaleDB and Redpanda reachability, each with a hard timeout."""
    database_status, kafka_status = await asyncio.gather(
        _run_check(_check_database()),
        _run_check(_check_kafka()),
    )
    body = {"database": database_status, "kafka": kafka_status}
    if database_status != "ok" or kafka_status != "ok":
        response.status_code = 503
    return body
