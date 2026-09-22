import json

import pytest

from app.ingestion import producer as producer_module
from app.ingestion.series_registry import SeriesSpec


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, key, value):
        self.sent.append((topic, key, value))


@pytest.fixture
def csv_series(tmp_path, monkeypatch):
    csv_content = "timestamp,value\n2014-01-01 00:00:00,1.5\n2014-01-01 00:05:00,2.5\n"
    (tmp_path / "series.csv").write_text(csv_content)
    monkeypatch.setattr(producer_module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(producer_module, "REPLAY_SPEED", 0.0)
    return SeriesSpec(series_id="test/series", csv_path="series.csv", anomaly_windows=())


async def test_payload_carries_the_csv_timestamp_not_wall_clock(csv_series):
    fake = FakeProducer()

    count = await producer_module.replay_series(fake, csv_series)

    assert count == 2
    topic, key, value = fake.sent[0]
    assert topic == producer_module.TOPIC
    assert key == b"test/series"
    payload = json.loads(value)
    assert payload == {"series_id": "test/series", "timestamp": "2014-01-01T00:00:00+00:00", "value": 1.5}

    _, _, value2 = fake.sent[1]
    payload2 = json.loads(value2)
    assert payload2["timestamp"] == "2014-01-01T00:05:00+00:00"


async def test_rows_are_sent_in_file_order(csv_series):
    fake = FakeProducer()

    await producer_module.replay_series(fake, csv_series)

    timestamps = [json.loads(v)["timestamp"] for _, _, v in fake.sent]
    assert timestamps == sorted(timestamps)
