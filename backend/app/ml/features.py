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

    Returns the same columns plus FEATURE_NAMES, sorted by (series_id, time), with two kinds of
    rows dropped — a rolling window never crosses a series_id boundary in either case:
    - the leading rows of each series, until a full 60-minute trailing window exists;
    - any row whose own trailing window still contains only 1 sample despite that — e.g. right
      after a real gap *inside* the series (not just at its start) that's itself >= 60 minutes.
      `rolling_std_1h` is undefined (NaN) for a 1-sample window; a NaN-shaped row isn't a real
      "normal" or "anomalous" feature vector, it's a data-quality artifact from missing
      monitoring data, so it's dropped rather than fed to the model.
    """
    df = df.sort_values(["series_id", "time"]).reset_index(drop=True)

    parts = []
    for _series_id, group in df.groupby("series_id", sort=False):
        # Already time-sorted: the frame-level sort_values above sorted within each group too.
        g = group.set_index("time")
        rolling = g["value"].rolling(WINDOW)
        g["rolling_mean_1h"] = rolling.mean()
        g["rolling_std_1h"] = rolling.std(ddof=1)
        g["rate_of_change"] = g["value"].diff()

        series_start = g.index.min()
        has_full_window = (g.index - series_start) >= pd.Timedelta(WINDOW)
        g = g.loc[has_full_window]

        parts.append(g.reset_index())

    result = pd.concat(parts, ignore_index=True)
    return result.dropna(subset=FEATURE_NAMES).reset_index(drop=True)
