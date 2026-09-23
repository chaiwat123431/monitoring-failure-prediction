"""REST endpoints: series discovery, historical metrics + scores, and model metadata
(PLANNING.md AD-20/AD-21/AD-24).
"""

import asyncio
from datetime import datetime, timedelta

import pandas as pd
import psycopg
from fastapi import APIRouter, HTTPException, Request, Response

from app.config import settings
from app.ingestion.series_registry import SERIES
from app.ml.features import FEATURE_NAMES, WINDOW_MINUTES, compute_features

router = APIRouter(prefix="/api")

_SERIES_IDS = {s.series_id for s in SERIES}


@router.get("/series")
async def list_series() -> list[dict]:
    return [{"series_id": s.series_id} for s in SERIES]


@router.get("/series/{series_id:path}/history")
async def series_history(series_id: str, start: datetime, end: datetime, request: Request) -> dict:
    """Raw metric history for [start, end] plus each row's is_anomaly (AD-21: computed by the same
    compute_features()/model.predict() as everywhere else, never a re-implementation) and the NAB
    ground-truth windows overlapping the range, kept as a separate array rather than a per-row
    column (AD-11's leak-avoidance discipline applied at the API boundary too).
    """
    if series_id not in _SERIES_IDS:
        raise HTTPException(status_code=404, detail=f"unknown series_id: {series_id}")
    if start >= end:
        raise HTTPException(status_code=400, detail="start must be before end")

    # Fetch WINDOW_MINUTES of lead-in before `start` so every in-range row gets a real trailing
    # window from compute_features, instead of the leading rows of the *query range* (not of the
    # series) being spuriously dropped by compute_features' leading-edge guard (AD-15).
    lookback = start - timedelta(minutes=WINDOW_MINUTES)

    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT time, value FROM raw_metrics "
                "WHERE series_id = %s AND time BETWEEN %s AND %s ORDER BY time",
                (series_id, lookback, end),
            )
            rows = await cur.fetchall()

            await cur.execute(
                "SELECT window_start, window_end FROM nab_anomaly_windows "
                "WHERE series_id = %s AND window_start <= %s AND window_end >= %s "
                "ORDER BY window_start",
                (series_id, end, start),
            )
            windows = await cur.fetchall()

    df = pd.DataFrame(rows, columns=["time", "value"])
    df["series_id"] = series_id

    model = request.app.state.model
    is_anomaly_by_time: dict = {}
    if model is not None and not df.empty:
        features = compute_features(df)
        if not features.empty:
            x = features[FEATURE_NAMES].to_numpy()
            # Batch predict (~8ms for a full series, measured — AD-21), off the event loop.
            y_pred = await asyncio.to_thread(model.predict, x)
            is_anomaly_by_time = {
                t: bool(pred == -1) for t, pred in zip(features["time"], y_pred)
            }

    history = [
        {"time": t.isoformat(), "value": v, "is_anomaly": is_anomaly_by_time.get(t)}
        for t, v in zip(df["time"], df["value"])
        if t >= start
    ]

    return {
        "series_id": series_id,
        "model_loaded": model is not None,
        "history": history,
        "labeled_windows": [
            {"window_start": ws.isoformat(), "window_end": we.isoformat()} for ws, we in windows
        ],
    }


@router.get("/model")
async def model_metadata(request: Request, response: Response) -> dict:
    """AD-24: a missing model is a handled 503, not an exception — same status-code-conveys-state
    style as /health/ready, not FastAPI's default {"detail": ...} error envelope."""
    metadata = request.app.state.model_metadata
    if metadata is None:
        response.status_code = 503
        return {
            "status": "not_trained",
            "detail": "run scripts/train.py to produce models/isolation_forest.joblib",
        }
    return metadata
