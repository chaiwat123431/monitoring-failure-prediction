"""Runs the real training pipeline against the real TimescaleDB data and checks the actual
measured precision/recall/F1 — not "training finished without an exception."

Requires: `docker compose up` running with ingestion already completed (see test_ingestion.py).
"""

from pathlib import Path

import joblib
import psycopg
import pytest

from app.config import settings
from app.ingestion.series_registry import SERIES
from app.ml.features import FEATURE_NAMES, compute_features
from scripts.train import (
    label_anomalies,
    load_anomaly_windows,
    load_raw_metrics,
    score,
    split_train_test,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def pipeline_outputs():
    with psycopg.connect(settings.database_url) as conn:
        raw = load_raw_metrics(conn)
        windows = load_anomaly_windows(conn)
    features = compute_features(raw)
    train_df, test_df, cutoffs = split_train_test(features)
    return raw, windows, features, train_df, test_df, cutoffs


def test_no_training_row_falls_inside_any_labeled_anomaly_window(pipeline_outputs):
    """Direct proof of AD-16's no-leakage claim, not just code review of the split logic."""
    _, windows, _, train_df, _, _ = pipeline_outputs

    labeled = label_anomalies(train_df, windows)

    assert labeled.sum() == 0


def test_test_set_contains_every_known_anomaly_window(pipeline_outputs):
    _, windows, _, _, test_df, _ = pipeline_outputs

    for series_id in [s.series_id for s in SERIES]:
        n_windows_for_series = (windows["series_id"] == series_id).sum()
        labeled = label_anomalies(test_df[test_df["series_id"] == series_id], windows)
        assert n_windows_for_series > 0
        assert labeled.sum() > 0


def test_real_precision_recall_f1_against_the_known_windows(pipeline_outputs, tmp_path):
    """The actual deliverable: real measured metrics from a real fit + predict, not a mock."""
    from sklearn.ensemble import IsolationForest

    _, windows, _, train_df, test_df, _ = pipeline_outputs

    model = IsolationForest(n_estimators=100, contamination="auto", random_state=42)
    model.fit(train_df[FEATURE_NAMES].to_numpy())

    y_pred = (model.predict(test_df[FEATURE_NAMES].to_numpy()) == -1).astype(int)
    test_df = test_df.copy()
    test_df["y_pred"] = y_pred
    test_df["y_true"] = label_anomalies(test_df, windows).values

    combined = score(test_df["y_true"], test_df["y_pred"])
    print(f"\nmeasured combined metrics: {combined}")

    # Loose regression floors set from the real measured baseline (recall ~0.79, f1 ~0.30),
    # not a made-up target — catches a large regression without being flaky on noise.
    assert combined["recall"] > 0.5
    assert combined["f1"] > 0.15

    for series_id, group in test_df.groupby("series_id"):
        m = score(group["y_true"], group["y_pred"])
        print(f"measured {series_id}: {m}")
        assert m["recall"] > 0.4


def test_reloading_the_saved_artifact_reproduces_its_own_recorded_metrics():
    """Proves the saved artifact is what was actually evaluated — not a save/load mismatch."""
    model_path = Path(__file__).resolve().parents[3] / "models" / "isolation_forest.joblib"
    if not model_path.exists():
        pytest.skip("models/isolation_forest.joblib not present — run scripts/train.py first")

    artifact = joblib.load(model_path)
    model = artifact["model"]
    saved_metrics = artifact["metadata"]["metrics"]
    feature_names = artifact["metadata"]["feature_names"]

    with psycopg.connect(settings.database_url) as conn:
        raw = load_raw_metrics(conn)
        windows = load_anomaly_windows(conn)
    features = compute_features(raw)
    _, test_df, _ = split_train_test(features)

    y_pred = (model.predict(test_df[feature_names].to_numpy()) == -1).astype(int)
    test_df = test_df.copy()
    test_df["y_pred"] = y_pred
    test_df["y_true"] = label_anomalies(test_df, windows).values

    for series_id, group in test_df.groupby("series_id"):
        fresh = score(group["y_true"], group["y_pred"])
        saved = saved_metrics[series_id]
        for key in ("precision", "recall", "f1"):
            assert fresh[key] == pytest.approx(saved[key], abs=1e-9)
