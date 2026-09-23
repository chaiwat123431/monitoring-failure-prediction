"""WebSocket live feed (PLANNING.md AD-22/AD-23): one endpoint per series, no automatic history
backfill on connect — a client is expected to call GET /api/series/{id}/history first for the
range it wants to chart, then open this socket for the live tail (AD-23).
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.ingestion.series_registry import SERIES_IDS

router = APIRouter()


@router.websocket("/ws/series/{series_id:path}/live")
async def series_live(websocket: WebSocket, series_id: str) -> None:
    if series_id not in SERIES_IDS:
        await websocket.close(code=1008, reason=f"unknown series_id: {series_id}")
        return

    connections = websocket.app.state.connections
    await websocket.accept()
    connections.connect(series_id, websocket)
    try:
        while True:
            # Send-only channel: receive() here only exists to notice a disconnect promptly
            # instead of leaking a dead connection until the next failed broadcast.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        connections.disconnect(series_id, websocket)
