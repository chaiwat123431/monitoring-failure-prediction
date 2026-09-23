from app.live.broadcaster import ConnectionManager


class FakeWebSocket:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.sent: list[dict] = []

    async def send_json(self, message: dict) -> None:
        if self.fail:
            raise RuntimeError("connection broken")
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
