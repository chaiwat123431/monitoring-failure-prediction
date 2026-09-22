"""Row-by-row proof that what's in TimescaleDB matches the NAB source CSVs exactly: same
timestamps, same values, same anomaly labels. Not "the pipeline ran" — the actual data.

Requires: `docker compose up` running with migrate/producer/consumer already completed against
the real NAB CSVs in `data/` (see scripts/fetch_nab_data.sh).
"""

import csv
from datetime import datetime
from pathlib import Path

import psycopg
import pytest

from app.config import settings
from app.ingestion.series_registry import SERIES, parse_nab_timestamp

pytestmark = pytest.mark.integration

DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"


def _read_csv(series) -> list[tuple[datetime, float]]:
    path = DATA_DIR / series.csv_path
    rows = []
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            ts = parse_nab_timestamp(row["timestamp"])
            rows.append((ts, float(row["value"])))
    return rows


@pytest.fixture(scope="module")
def db_conn():
    with psycopg.connect(settings.database_url) as conn:
        yield conn


@pytest.mark.parametrize("series", SERIES, ids=[s.series_id for s in SERIES])
def test_row_count_matches_source_csv(db_conn, series):
    csv_rows = _read_csv(series)
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw_metrics WHERE series_id = %s", (series.series_id,))
        (db_count,) = cur.fetchone()
    assert db_count == len(csv_rows)


@pytest.mark.parametrize("series", SERIES, ids=[s.series_id for s in SERIES])
def test_every_row_matches_source_csv_exactly(db_conn, series):
    csv_rows = _read_csv(series)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT time, value FROM raw_metrics WHERE series_id = %s ORDER BY time",
            (series.series_id,),
        )
        db_rows = cur.fetchall()

    assert len(db_rows) == len(csv_rows)
    mismatches = [
        (csv_ts, csv_val, db_ts, db_val)
        for (csv_ts, csv_val), (db_ts, db_val) in zip(csv_rows, db_rows)
        if csv_ts != db_ts or csv_val != db_val
    ]
    assert mismatches == [], f"{len(mismatches)} row(s) differ, e.g. {mismatches[:5]}"


@pytest.mark.parametrize("series", SERIES, ids=[s.series_id for s in SERIES])
def test_anomaly_windows_match_nab_ground_truth(db_conn, series):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT window_start, window_end FROM nab_anomaly_windows "
            "WHERE series_id = %s ORDER BY window_start",
            (series.series_id,),
        )
        db_windows = cur.fetchall()

    expected = [(w.start, w.end) for w in series.anomaly_windows]
    assert db_windows == expected


@pytest.mark.parametrize("series", SERIES, ids=[s.series_id for s in SERIES])
def test_anomaly_windows_actually_intersect_ingested_data(db_conn, series):
    """Catches a timezone/parsing bug where windows land outside the ingested time range."""
    with db_conn.cursor() as cur:
        for window in series.anomaly_windows:
            cur.execute(
                "SELECT count(*) FROM raw_metrics "
                "WHERE series_id = %s AND time BETWEEN %s AND %s",
                (series.series_id, window.start, window.end),
            )
            (count,) = cur.fetchone()
            assert count > 0, f"window {window} has no matching rows in raw_metrics"
