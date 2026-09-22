"""The 2 NAB series this project ingests (PLANNING.md AD-10) and their ground-truth anomaly
windows, taken verbatim from NAB's labels/combined_windows.json. Hardcoded rather than parsed
from that file at runtime: there are only 3 windows total across these 2 fixed series (AD-11)."""

from dataclasses import dataclass
from datetime import datetime, timezone


def parse_nab_timestamp(s: str) -> datetime:
    """NAB timestamps carry no timezone; UTC is the standard community convention for this
    corpus (PLANNING.md AD-12). Single source of truth — also used by the producer and the
    ingestion integration test, so the parsing/timezone assumption can't silently drift between
    what's ingested and what's checked against."""
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class AnomalyWindow:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class SeriesSpec:
    series_id: str
    csv_path: str  # relative to the data directory
    anomaly_windows: tuple[AnomalyWindow, ...]


def _window(start: str, end: str) -> AnomalyWindow:
    return AnomalyWindow(parse_nab_timestamp(start), parse_nab_timestamp(end))


SERIES: tuple[SeriesSpec, ...] = (
    SeriesSpec(
        series_id="realAWSCloudwatch/ec2_cpu_utilization_825cc2",
        csv_path="realAWSCloudwatch/ec2_cpu_utilization_825cc2.csv",
        anomaly_windows=(_window("2014-04-15 07:24:00", "2014-04-16 11:54:00"),),
    ),
    SeriesSpec(
        series_id="realAWSCloudwatch/rds_cpu_utilization_cc0c53",
        csv_path="realAWSCloudwatch/rds_cpu_utilization_cc0c53.csv",
        anomaly_windows=(
            _window("2014-02-24 22:50:00", "2014-02-25 15:35:00"),
            _window("2014-02-26 16:30:00", "2014-02-27 09:10:00"),
        ),
    ),
)
