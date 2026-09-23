"""Runs against the real docker-compose stack: real TimescaleDB data, the real trained model.

Requires: `docker compose up` running with ingestion + training already completed (see
test_ingestion.py, test_train.py) — `models/isolation_forest.joblib` must exist.
"""

from datetime import timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.ingestion.series_registry import SERIES
from app.main import app

pytestmark = pytest.mark.integration

SERIES_ID = SERIES[0].series_id
WINDOW = SERIES[0].anomaly_windows[0]


@pytest.fixture(scope="module")
def client():
    # Runs the real lifespan (AD-21/AD-24): loads the real model artifact and starts the real
    # live-feed task against the real Kafka/TimescaleDB this module requires anyway.
    with TestClient(app) as c:
        yield c


def test_history_row_values_match_raw_metrics_exactly(client):
    start = WINDOW.start - timedelta(hours=1)
    end = WINDOW.start + timedelta(hours=1)

    response = client.get(
        f"/api/series/{SERIES_ID}/history",
        params={"start": start.isoformat(), "end": end.isoformat()},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["model_loaded"] is True

    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT time, value FROM raw_metrics "
                "WHERE series_id = %s AND time BETWEEN %s AND %s ORDER BY time",
                (SERIES_ID, start, end),
            )
            db_rows = cur.fetchall()

    assert len(body["history"]) == len(db_rows)
    for row, (db_time, db_value) in zip(body["history"], db_rows):
        assert row["time"] == db_time.isoformat()
        assert row["value"] == db_value


def test_history_labeled_windows_include_the_known_anomaly(client):
    start = WINDOW.start - timedelta(hours=1)
    end = WINDOW.end + timedelta(hours=1)

    response = client.get(
        f"/api/series/{SERIES_ID}/history",
        params={"start": start.isoformat(), "end": end.isoformat()},
    )

    windows = response.json()["labeled_windows"]
    assert {"window_start": WINDOW.start.isoformat(), "window_end": WINDOW.end.isoformat()} in windows


def test_history_is_anomaly_matches_the_model_predicting_the_same_rows_directly(client):
    """Direct proof AD-21's claim holds against real data: the API's is_anomaly must be exactly
    what model.predict() says for the same feature rows, computed independently here."""
    import joblib
    import pandas as pd

    from app.ml.features import FEATURE_NAMES, compute_features

    start = WINDOW.start - timedelta(hours=2)
    end = WINDOW.start + timedelta(hours=2)

    response = client.get(
        f"/api/series/{SERIES_ID}/history",
        params={"start": start.isoformat(), "end": end.isoformat()},
    )
    body = response.json()

    artifact = joblib.load(settings.model_path)
    model = artifact["model"]

    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT time, value FROM raw_metrics WHERE series_id = %s ORDER BY time",
                (SERIES_ID,),
            )
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["time", "value"])
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df["series_id"] = SERIES_ID
    features = compute_features(df)
    x = features[FEATURE_NAMES].to_numpy()
    y_pred = model.predict(x)
    expected = {t: bool(p == -1) for t, p in zip(features["time"], y_pred)}

    checked = 0
    for row in body["history"]:
        t = pd.Timestamp(row["time"])
        if t in expected:
            checked += 1
            assert row["is_anomaly"] == expected[t]
    assert checked > 0


def test_model_metadata_matches_the_real_artifact(client):
    import joblib

    response = client.get("/api/model")
    assert response.status_code == 200

    artifact = joblib.load(settings.model_path)
    assert response.json() == artifact["metadata"]
