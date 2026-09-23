import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.health import router as health_router
from app.api.metrics import router as metrics_router
from app.api.ws import router as ws_router
from app.config import settings
from app.live.broadcaster import ConnectionManager
from app.live.feed_consumer import run_live_feed
from app.ml.model_store import load_model


@asynccontextmanager
async def lifespan(app: FastAPI):
    # AD-24: a missing model artifact is a handled state, not a startup crash — train.py may
    # simply not have run yet.
    model, metadata = load_model(settings.model_path)
    if model is None:
        print(f"api: no model found at {settings.model_path}, starting without inference — run scripts/train.py")
    app.state.model = model
    app.state.model_metadata = metadata
    app.state.connections = ConnectionManager()

    live_feed_task = asyncio.create_task(run_live_feed(app.state, app.state.connections))
    try:
        yield
    finally:
        live_feed_task.cancel()
        try:
            await live_feed_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="monitoring-failure-prediction", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(metrics_router)
app.include_router(ws_router)
