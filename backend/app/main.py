from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.health import router as health_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ingestion consumer startup/shutdown will be wired in here (Slice 2+).
    yield


app = FastAPI(title="monitoring-failure-prediction", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
