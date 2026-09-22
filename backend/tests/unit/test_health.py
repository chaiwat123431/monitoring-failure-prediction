import asyncio

import pytest
from fastapi.testclient import TestClient

from app.api import health as health_module
from app.main import app

client = TestClient(app)


def test_health_returns_ok_without_touching_dependencies(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("/health must not perform I/O")

    monkeypatch.setattr(health_module, "_check_database", boom)
    monkeypatch.setattr(health_module, "_check_kafka", boom)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize(
    "db_result,kafka_result,expected_status",
    [
        ("ok", "ok", 200),
        ("error: boom", "ok", 503),
        ("ok", "error: boom", 503),
        ("error: boom", "error: boom", 503),
    ],
)
def test_ready_reflects_dependency_state(monkeypatch, db_result, kafka_result, expected_status):
    async def fake_db():
        return db_result

    async def fake_kafka():
        return kafka_result

    monkeypatch.setattr(health_module, "_check_database", fake_db)
    monkeypatch.setattr(health_module, "_check_kafka", fake_kafka)

    response = client.get("/health/ready")

    assert response.status_code == expected_status
    assert response.json() == {"database": db_result, "kafka": kafka_result}


def test_ready_times_out_bounded(monkeypatch):
    async def hangs_forever():
        await asyncio.sleep(3600)
        return "ok"

    monkeypatch.setattr(health_module, "_check_database", hangs_forever)
    monkeypatch.setattr(health_module.settings, "ready_check_timeout_seconds", 0.05)

    async def fake_kafka():
        return "ok"

    monkeypatch.setattr(health_module, "_check_kafka", fake_kafka)

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["database"] == "error: timeout"
