"""Plain-SQL schema init (PLANNING.md AD-14 — no Alembic yet). Run once before producer/consumer.

Creates raw_metrics (the hypertable ingestion writes to) and nab_anomaly_windows (seeded here,
from SERIES, and never touched by the producer/consumer — AD-11).
"""

import psycopg

from app.config import settings
from app.ingestion.series_registry import SERIES

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_metrics (
    time      TIMESTAMPTZ      NOT NULL,
    series_id TEXT             NOT NULL,
    value     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (series_id, time)
);

SELECT create_hypertable('raw_metrics', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS nab_anomaly_windows (
    id           SERIAL PRIMARY KEY,
    series_id    TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    UNIQUE (series_id, window_start, window_end)
);
"""

SEED_WINDOW_SQL = """
INSERT INTO nab_anomaly_windows (series_id, window_start, window_end)
VALUES (%s, %s, %s)
ON CONFLICT (series_id, window_start, window_end) DO NOTHING
"""


def main() -> None:
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute(SCHEMA_SQL)
        for series in SERIES:
            for window in series.anomaly_windows:
                conn.execute(SEED_WINDOW_SQL, (series.series_id, window.start, window.end))
    print("migrate: schema ready, anomaly windows seeded")


if __name__ == "__main__":
    main()
