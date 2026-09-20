"""FastAPI application entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from . import config
from .barriers import router as barriers_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.from_url(
        config.REDIS_URL,
        decode_responses=True,
        max_connections=config.REDIS_MAX_CONNECTIONS,
    )
    try:
        yield
    finally:
        await app.state.redis.aclose()


app = FastAPI(
    title="Barrier Coordination API",
    version="1.0.0",
    description=(
        "Rendezvous barriers for parallel-task coordination. Clients create a "
        "barrier with a participant roster, a quorum threshold and a deadline; "
        "participants check in idempotently; waiters block until the quorum is "
        "frozen or the deadline passes."
    ),
    lifespan=lifespan,
)
app.include_router(barriers_router, prefix="/barriers", tags=["barriers"])


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"detail": f"redis unavailable: {exc.__class__.__name__}"},
    )


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {"service": "barrier-api", "docs": "/docs", "health": "/healthz"}


@app.get("/healthz", tags=["meta"])
async def healthz() -> dict:
    await app.state.redis.ping()
    return {"status": "ok"}
