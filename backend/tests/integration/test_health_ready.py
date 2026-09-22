"""Runs against the real docker-compose stack (TimescaleDB + Redpanda).

Not run by the default `pytest` invocation:
    pytest -m integration
"""

import pytest
from fastapi.testclient import TestClient

from app.main import app

pytestmark = pytest.mark.integration

client = TestClient(app)


def test_ready_is_ok_against_real_dependencies():
    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"database": "ok", "kafka": "ok"}
