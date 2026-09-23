import numpy as np
import pandas as pd
import pytest

from app.ml.features import FEATURE_NAMES, compute_features


def _series(series_id: str, start: str, values: list[float], freq: str = "5min") -> pd.DataFrame:
    times = pd.date_range(start, periods=len(values), freq=freq, tz="UTC")
    return pd.DataFrame({"series_id": series_id, "time": times, "value": values})


def test_rolling_features_match_hand_computed_values():
    values = [float(i) for i in range(20)]  # linear ramp, deterministic mean/std/diff
    df = _series("x", "2020-01-01 00:00", values)

    out = compute_features(df)

    # First surviving row is the 13th sample (index 12, value=12.0) — the first with >=60min
    # of history since the series started at index 0.
    first = out.iloc[0]
    assert first["value"] == 12.0
    window = np.array(values[1:13])  # pandas' time-window is (t-60min, t], excludes t-60min itself
    assert first["rolling_mean_1h"] == pytest.approx(window.mean())
    assert first["rolling_std_1h"] == pytest.approx(window.std(ddof=1))
    assert first["rate_of_change"] == pytest.approx(1.0)  # linear ramp, step 1 every 5min
    assert len(out) == len(values) - 12  # rows before the first full window are dropped


def test_rolling_window_is_time_based_not_row_count_based():
    """A 10-minute gap must not silently stretch or shrink the effective window."""
    times = pd.date_range("2020-01-01 00:00", periods=13, freq="5min", tz="UTC").tolist()
    times[7:] = [t + pd.Timedelta(minutes=5) for t in times[7:]]  # insert a 10-min gap at index 6-7
    values = [10.0] * 13
    df = pd.DataFrame({"series_id": "a", "time": times, "value": values})

    out = compute_features(df)

    # Series spans 65 minutes (00:00 -> 01:05); only rows >=60min after start survive.
    assert list(out["time"]) == [times[11], times[12]]


def test_rolling_state_never_crosses_a_series_boundary():
    series_a = _series("a", "2020-01-01 00:00", [10.0] * 15)
    series_b = _series("b", "2020-06-01 00:00", [1000.0] * 15)
    # Deliberately interleaved / unsorted input.
    df = pd.concat([series_b, series_a], ignore_index=True)

    out = compute_features(df)

    for series_id, group in out.groupby("series_id"):
        # If rolling state leaked across series, series_a's mean would be pulled toward 1000
        # (or series_b's toward 10) near the boundary.
        assert (group["rolling_mean_1h"] == group["value"].iloc[0]).all()


def test_output_has_the_declared_feature_columns():
    df = _series("x", "2020-01-01 00:00", [float(i) for i in range(15)])
    out = compute_features(df)
    for col in FEATURE_NAMES:
        assert col in out.columns
