"""Trains one Isolation Forest on both NAB series (PLANNING.md AD-15..AD-19).

Run against the real stack:
    DATABASE_URL=postgresql://app:app@localhost:5433/monitoring uv run python scripts/train.py

nab_anomaly_windows is touched exactly once here — after model.fit() — purely to score
predictions. IsolationForest.fit() is called with features only; there is no `y` argument in its
API for a label to leak through even by mistake.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
import psycopg
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score, recall_score

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings  # noqa: E402
from app.ingestion.series_registry import SERIES  # noqa: E402
from app.ml.features import FEATURE_NAMES, FEATURE_VERSION, WINDOW_MINUTES, compute_features  # noqa: E402

MODEL_PATH = Path(
    os.environ.get("MODEL_PATH", str(Path(__file__).resolve().parents[2] / "models" / "isolation_forest.joblib"))
)
MODEL_PARAMS = {"n_estimators": 100, "contamination": "auto", "random_state": 42}


def load_raw_metrics(conn: psycopg.Connection) -> pd.DataFrame:
    series_ids = [s.series_id for s in SERIES]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT series_id, time, value FROM raw_metrics "
            "WHERE series_id = ANY(%s) ORDER BY series_id, time",
            (series_ids,),
        )
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["series_id", "time", "value"])
    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df


def load_anomaly_windows(conn: psycopg.Connection) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute("SELECT series_id, window_start, window_end FROM nab_anomaly_windows")
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["series_id", "window_start", "window_end"])
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    return df


def split_train_test(features: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """AD-16: cut per series at that series' own first labeled anomaly window."""
    cutoffs = {s.series_id: s.anomaly_windows[0].start for s in SERIES}
    cutoff_col = features["series_id"].map(cutoffs)
    train_df = features[features["time"] < cutoff_col].reset_index(drop=True)
    test_df = features[features["time"] >= cutoff_col].reset_index(drop=True)
    return train_df, test_df, cutoffs


def label_anomalies(df: pd.DataFrame, windows: pd.DataFrame) -> pd.Series:
    """AD-17: ground truth for evaluation only, computed after the fact — never fed to fit()."""
    y_true = pd.Series(0, index=df.index)
    for _, w in windows.iterrows():
        mask = (
            (df["series_id"] == w["series_id"])
            & (df["time"] >= w["window_start"])
            & (df["time"] <= w["window_end"])
        )
        y_true[mask] = 1
    return y_true


def score(y_true, y_pred) -> dict:
    return {
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0),
    }


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        raw = load_raw_metrics(conn)
        windows = load_anomaly_windows(conn)

    features = compute_features(raw)
    train_df, test_df, cutoffs = split_train_test(features)

    model = IsolationForest(**MODEL_PARAMS)
    model.fit(train_df[FEATURE_NAMES].to_numpy())

    y_pred_raw = model.predict(test_df[FEATURE_NAMES].to_numpy())
    test_df = test_df.copy()
    test_df["y_pred"] = (y_pred_raw == -1).astype(int)
    test_df["y_true"] = label_anomalies(test_df, windows).values

    metrics = {}
    for series_id, group in test_df.groupby("series_id"):
        metrics[series_id] = {
            **score(group["y_true"], group["y_pred"]),
            "train_rows": int((train_df["series_id"] == series_id).sum()),
            "test_rows": len(group),
            "positive_rows": int(group["y_true"].sum()),
        }
    metrics["combined"] = {
        **score(test_df["y_true"], test_df["y_pred"]),
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "positive_rows": int(test_df["y_true"].sum()),
    }

    metadata = {
        "feature_version": FEATURE_VERSION,
        "window_minutes": WINDOW_MINUTES,
        "feature_names": FEATURE_NAMES,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "series_used": [s.series_id for s in SERIES],
        "split_cutoffs": {sid: ts.isoformat() for sid, ts in cutoffs.items()},
        "model_params": MODEL_PARAMS,
        "sklearn_version": sklearn.__version__,
        "metrics": metrics,
    }

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "metadata": metadata}, MODEL_PATH)

    print(f"train: saved {MODEL_PATH}")
    for series_id, m in metrics.items():
        print(
            f"train: {series_id}: precision={m['precision']:.3f} recall={m['recall']:.3f} "
            f"f1={m['f1']:.3f} (train={m['train_rows']} test={m['test_rows']} "
            f"positives={m['positive_rows']})"
        )


if __name__ == "__main__":
    main()
