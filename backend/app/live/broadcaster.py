"""Fan-out from the live-feed task to connected WebSocket clients, keyed by series_id
(PLANNING.md AD-22/AD-23) — one set of sockets per series; a client only ever sees the series it
connected for, so nothing server-side needs to filter a shared stream.
"""

from collections import defaultdict

from fastapi import WebSocket, WebSocketDisconnect


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)

    def connect(self, series_id: str, websocket: WebSocket) -> None:
        self._connections[series_id].add(websocket)

    def disconnect(self, series_id: str, websocket: WebSocket) -> None:
        self._connections[series_id].discard(websocket)

    async def broadcast(self, series_id: str, message: dict) -> None:
        # Snapshot before iterating: a send failure disconnects (mutates the set) mid-loop.
        for websocket in list(self._connections.get(series_id, ())):
            try:
                await websocket.send_json(message)
            except (WebSocketDisconnect, RuntimeError):
                # Starlette raises WebSocketDisconnect for a broken connection (converted from
                # OSError) and RuntimeError for sending on an already-closed socket — both mean
                # "this client is gone." Anything else (e.g. a TypeError from send_json's own
                # json.dumps on a bad `message`) is a real bug and must propagate, not be silently
                # misread as a disconnect and swallowed.
                self.disconnect(series_id, websocket)
