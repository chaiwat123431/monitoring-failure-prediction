"""Validation/error paths only — these run before any DB I/O in the handlers, so they need no
real TimescaleDB (the happy path with real rows is integration-tested, tests/integration/test_api.py).
AD-24's missing-model behaviour is also pure state, so it's covered here too.
"""

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_history_rejects_unknown_series_id():
    response = client.get(
        "/api/series/does-not-exist/history",
        params={"start": "2020-01-01T00:00:00Z", "end": "2020-01-01T01:00:00Z"},
    )

    assert response.status_code == 404


def test_history_rejects_start_after_end():
    response = client.get(
        "/api/series/realAWSCloudwatch/ec2_cpu_utilization_825cc2/history",
        params={"start": "2020-01-01T01:00:00Z", "end": "2020-01-01T00:00:00Z"},
    )

    assert response.status_code == 400


def test_list_series_returns_the_registry():
    response = client.get("/api/series")

    assert response.status_code == 200
    ids = {row["series_id"] for row in response.json()}
    assert "realAWSCloudwatch/ec2_cpu_utilization_825cc2" in ids
    assert "realAWSCloudwatch/rds_cpu_utilization_cc0c53" in ids


def test_model_endpoint_is_503_when_no_model_is_loaded():
    app.state.model_metadata = None

    response = client.get("/api/model")

    assert response.status_code == 503
    assert response.json()["status"] == "not_trained"


def test_model_endpoint_returns_metadata_verbatim_when_loaded():
    fake_metadata = {"feature_version": "v1", "metrics": {"combined": {"f1": 0.3}}}
    app.state.model_metadata = fake_metadata

    response = client.get("/api/model")

    assert response.status_code == 200
    assert response.json() == fake_metadata
