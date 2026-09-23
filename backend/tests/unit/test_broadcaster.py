import pytest
from starlette.websockets import WebSocketDisconnect

from app.live.broadcaster import ConnectionManager


class FakeWebSocket:
    def __init__(self, fail: bool = False, error: Exception | None = None):
        self.fail = fail
        self.error = error or RuntimeError("connection broken")
        self.sent: list[dict] = []

    async def send_json(self, message: dict) -> None:
        if self.fail:
            raise self.error
        self.sent.append(message)


async def test_broadcast_reaches_only_sockets_for_that_series():
    connections = ConnectionManager()
    ec2_socket = FakeWebSocket()
    rds_socket = FakeWebSocket()
    connections.connect("ec2", ec2_socket)
    connections.connect("rds", rds_socket)

    await connections.broadcast("ec2", {"value": 1})

    assert ec2_socket.sent == [{"value": 1}]
    assert rds_socket.sent == []


async def test_a_failed_send_disconnects_that_socket_without_affecting_others():
    connections = ConnectionManager()
    dead_socket = FakeWebSocket(fail=True)
    live_socket = FakeWebSocket()
    connections.connect("ec2", dead_socket)
    connections.connect("ec2", live_socket)

    await connections.broadcast("ec2", {"value": 1})

    assert live_socket.sent == [{"value": 1}]
    # The dead socket was removed — a second broadcast doesn't try it again.
    await connections.broadcast("ec2", {"value": 2})
    assert live_socket.sent == [{"value": 1}, {"value": 2}]


def test_disconnect_is_a_no_op_for_a_socket_never_connected():
    connections = ConnectionManager()
    connections.disconnect("ec2", FakeWebSocket())  # must not raise


async def test_websocket_disconnect_is_treated_as_gone():
    connections = ConnectionManager()
    socket = FakeWebSocket(fail=True, error=WebSocketDisconnect(code=1006))
    connections.connect("ec2", socket)

    await connections.broadcast("ec2", {"value": 1})  # must not raise

    assert socket not in connections._connections["ec2"]


async def test_a_non_connection_error_propagates_instead_of_being_swallowed_as_a_disconnect():
    """Found by /code-review: send_json's own json.dumps can raise TypeError on a bad message,
    *before* any socket I/O happens — that's a real bug, not a dead client, and must not be
    silently misread as one."""
    connections = ConnectionManager()
    socket = FakeWebSocket(fail=True, error=TypeError("Object of type X is not JSON serializable"))
    connections.connect("ec2", socket)

    with pytest.raises(TypeError):
        await connections.broadcast("ec2", {"value": 1})

    # Not disconnected — the socket itself was never actually confirmed broken.
    assert socket in connections._connections["ec2"]
