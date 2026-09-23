"""Feature engineering (PLANNING.md AD-15). The single source of truth for turning raw
(series_id, time, value) rows into model input — imported by the offline training script now and
by Slice 4's live inference later, so the two can never compute features differently.
"""

import pandas as pd

FEATURE_VERSION = "v1"
WINDOW = "60min"
WINDOW_MINUTES = 60
FEATURE_NAMES = ["value", "rolling_mean_1h", "rolling_std_1h", "rate_of_change"]


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """df must have columns: series_id, time (tz-aware), value.

    Returns the same columns plus FEATURE_NAMES, sorted by (series_id, time), with the leading
    rows of each series dropped until a full 60-minute trailing window exists for that series —
    a rolling window never crosses a series_id boundary.
    """
    df = df.sort_values(["series_id", "time"]).reset_index(drop=True)

    parts = []
    for _series_id, group in df.groupby("series_id", sort=False):
        g = group.set_index("time").sort_index()
        rolling = g["value"].rolling(WINDOW)
        g["rolling_mean_1h"] = rolling.mean()
        g["rolling_std_1h"] = rolling.std(ddof=1)
        g["rate_of_change"] = g["value"].diff()

        series_start = g.index.min()
        has_full_window = (g.index - series_start) >= pd.Timedelta(WINDOW)
        g = g.loc[has_full_window]

        parts.append(g.reset_index())

    return pd.concat(parts, ignore_index=True)
