import psycopg
import pytest

from app.ingestion.consumer import ReconnectingConn, parse_message, write_with_retry

pytestmark = pytest.mark.filterwarnings("ignore")


@pytest.mark.parametrize(
    "raw_bytes",
    [
        b"not-json-garbage",
        b'{"series_id": "a", "timestamp": "t"}',  # missing value
        b'{"series_id": "a", "timestamp": "t", "value": "not-a-number"}',
        b'{"series_id": "a", "timestamp": "t", "value": null}',
    ],
)
def test_parse_message_raises_on_anything_malformed(raw_bytes):
    with pytest.raises((ValueError, KeyError, TypeError)):
        parse_message(raw_bytes)


def test_parse_message_extracts_the_three_fields():
    payload = parse_message(b'{"series_id": "s", "timestamp": "t", "value": "1.5"}')
    assert payload == {"series_id": "s", "timestamp": "t", "value": 1.5}


class FakeConn:
    def __init__(self, fail_times=0, error=None):
        self.fail_times = fail_times
        self.error = error or psycopg.OperationalError("connection refused")
        self.calls = 0
        self.closed = False

    async def execute(self, *args, **kwargs):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.error
        return None


class FakeReconnectingConn:
    """Mimics ReconnectingConn's interface without a real DB, tracking reconnects."""

    def __init__(self, conn: FakeConn):
        self._conn = conn
        self.reconnects = 0

    async def get(self):
        return self._conn

    def discard(self):
        self.reconnects += 1


async def test_write_with_retry_retries_operational_errors_until_success():
    conn = FakeConn(fail_times=2, error=psycopg.OperationalError("connection refused"))
    db = FakeReconnectingConn(conn)

    await write_with_retry(db, {"series_id": "s", "timestamp": "t", "value": 1.0})

    assert conn.calls == 3
    assert db.reconnects == 2


async def test_write_with_retry_does_not_retry_a_permanent_data_error():
    conn = FakeConn(fail_times=99, error=psycopg.DataError("invalid input syntax"))
    db = FakeReconnectingConn(conn)

    with pytest.raises(psycopg.DataError):
        await write_with_retry(db, {"series_id": "s", "timestamp": "t", "value": 1.0})

    assert conn.calls == 1
