import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.live.broadcaster import ConnectionManager
from app.main import app

client = TestClient(app)


def test_unknown_series_id_is_rejected_before_accept():
    """AD-23: validated against the registry — this must not require app.state.connections to
    exist (real lifespan doesn't run in this unit test), since rejection happens before it's read.
    """
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect("/ws/series/does-not-exist/live"):
            pass
    assert exc_info.value.code == 1008


def test_known_series_is_accepted_and_registered_with_the_connection_manager():
    app.state.connections = ConnectionManager()
    series_id = "realAWSCloudwatch/ec2_cpu_utilization_825cc2"

    with client.websocket_connect(f"/ws/series/{series_id}/live"):
        assert any(app.state.connections._connections.get(series_id, set()))

    # Disconnecting must unregister the socket — no leaked references.
    assert not app.state.connections._connections.get(series_id, set())
