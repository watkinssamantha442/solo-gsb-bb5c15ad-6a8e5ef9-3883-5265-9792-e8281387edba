"""Barrier endpoints: create, inspect, check in, and blocking wait."""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from . import config
from .lua import CHECKIN_SCRIPT, FINALIZE_SCRIPT
from .models import (
    BarrierCreate,
    BarrierState,
    CheckInRequest,
    CheckInResult,
    WaitResult,
)

router = APIRouter()


def _keys(barrier_id: str) -> dict[str, str]:
    base = f"barrier:{barrier_id}"
    return {
        "meta": f"{base}:meta",
        "participants": f"{base}:participants",
        "checked": f"{base}:checked",
        "result": f"{base}:result",
        "channel": f"{base}:events",
    }


async def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis


def _utc(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


@dataclass
class _State:
    """Internal snapshot of a barrier, including Redis server time."""

    barrier_id: str
    status: str
    threshold: int
    deadline_ts: float
    server_now: float
    participants: list[str]
    checked_in: list[str]
    met_members: list[str] | None
    met_at_ts: float | None

    @property
    def absent(self) -> list[str]:
        return sorted(set(self.participants) - set(self.checked_in))


async def _read_state(r: redis.Redis, barrier_id: str) -> _State | None:
    """Read the full barrier state, lazily closing it if the deadline has passed."""
    keys = _keys(barrier_id)
    async with r.pipeline(transaction=False) as pipe:
        pipe.hgetall(keys["meta"])
        pipe.smembers(keys["participants"])
        pipe.smembers(keys["checked"])
        pipe.get(keys["result"])
        pipe.time()
        meta, participants, checked, result_json, (sec, usec) = await pipe.execute()

    if not meta:
        return None

    server_now = sec + usec / 1_000_000
    status = meta["status"]
    deadline_ts = float(meta["deadline"])

    if status == "open" and server_now >= deadline_ts:
        # Lazily finalize at the deadline (idempotent; also wakes any waiters).
        finalized = await r.register_script(FINALIZE_SCRIPT)(
            keys=[keys["meta"], keys["channel"]], args=[]
        )
        status = "closed" if finalized == "CLOSED" else finalized.lower()

    return _State(
        barrier_id=barrier_id,
        status=status,
        threshold=int(meta["threshold"]),
        deadline_ts=deadline_ts,
        server_now=server_now,
        participants=sorted(participants),
        checked_in=sorted(checked),
        met_members=json.loads(result_json) if result_json else None,
        met_at_ts=float(meta["met_at"]) if meta.get("met_at") else None,
    )


def _state_response(state: _State) -> BarrierState:
    remaining = (
        max(0.0, state.deadline_ts - state.server_now) if state.status == "open" else 0.0
    )
    return BarrierState(
        barrier_id=state.barrier_id,
        status=state.status,  # type: ignore[arg-type]
        threshold=state.threshold,
        deadline=_utc(state.deadline_ts),
        participants=state.participants,
        checked_in=state.checked_in,
        checked_in_count=len(state.checked_in),
        absent=state.absent,
        remaining_seconds=round(remaining, 3),
        met_members=state.met_members if state.status == "met" else None,
        met_at=_utc(state.met_at_ts)
        if state.status == "met" and state.met_at_ts is not None
        else None,
    )


def _wait_response(state: _State) -> WaitResult:
    if state.status == "met":
        return WaitResult(
            barrier_id=state.barrier_id,
            status="met",
            members=state.met_members,
            met_at=_utc(state.met_at_ts) if state.met_at_ts is not None else None,
        )
    # "closed" (deadline reached) or "open" (caller's timeout elapsed first).
    return WaitResult(
        barrier_id=state.barrier_id,
        status=state.status,  # type: ignore[arg-type]
        checked_in=state.checked_in,
        absent=state.absent,
    )


async def _get_state_or_404(r: redis.Redis, barrier_id: str) -> _State:
    state = await _read_state(r, barrier_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"barrier {barrier_id!r} not found")
    return state


@router.post("", response_model=BarrierState, status_code=201)
async def create_barrier(
    body: BarrierCreate, r: redis.Redis = Depends(get_redis)
) -> BarrierState:
    """Create a barrier with a participant roster, a quorum threshold and a deadline."""
    barrier_id = uuid.uuid4().hex
    sec, usec = await r.time()
    now = sec + usec / 1_000_000

    deadline_utc = body.deadline_utc()
    deadline_ts = deadline_utc.timestamp() if deadline_utc else now + float(body.ttl_seconds)
    expire_at_ms = int((deadline_ts + config.BARRIER_RETENTION_SECONDS) * 1000)

    keys = _keys(barrier_id)
    participants = sorted(set(body.participants))
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(
            keys["meta"],
            mapping={
                "status": "open",
                "threshold": str(body.threshold),
                "deadline": f"{deadline_ts:.6f}",
                "created_at": f"{now:.6f}",
                "expire_at": str(expire_at_ms),
            },
        )
        pipe.sadd(keys["participants"], *participants)
        pipe.pexpireat(keys["meta"], expire_at_ms)
        pipe.pexpireat(keys["participants"], expire_at_ms)
        await pipe.execute()

    return _state_response(await _get_state_or_404(r, barrier_id))


@router.get("/{barrier_id}", response_model=BarrierState)
async def get_barrier(
    barrier_id: str, r: redis.Redis = Depends(get_redis)
) -> BarrierState:
    """Return the current state of a barrier."""
    return _state_response(await _get_state_or_404(r, barrier_id))


@router.post("/{barrier_id}/checkin", response_model=CheckInResult)
async def check_in(
    barrier_id: str, body: CheckInRequest, r: redis.Redis = Depends(get_redis)
) -> CheckInResult:
    """Idempotently check a participant in.

    The whole operation is a single atomic Lua script: the first check-in that
    reaches the threshold freezes the quorum snapshot; later check-ins never
    rewrite it. Repeating the same participant is a safe no-op.
    """
    keys = _keys(barrier_id)
    result = await r.register_script(CHECKIN_SCRIPT)(
        keys=[
            keys["meta"],
            keys["participants"],
            keys["checked"],
            keys["result"],
            keys["channel"],
        ],
        args=[body.participant_id],
    )
    code = result[0]

    if code == "NOT_FOUND":
        raise HTTPException(status_code=404, detail=f"barrier {barrier_id!r} not found")
    if code == "NOT_PARTICIPANT":
        raise HTTPException(
            status_code=403,
            detail=f"{body.participant_id!r} is not a participant of this barrier",
        )
    if code in ("CLOSED", "EXPIRED"):
        raise HTTPException(
            status_code=409,
            detail="barrier is closed: the deadline has passed",
        )

    # CHECKED_IN / MET / ALREADY_MET — all successful, none rewrites a frozen result.
    recorded = code in ("CHECKED_IN", "MET") and int(result[1]) == 1
    state = await _get_state_or_404(r, barrier_id)
    return CheckInResult(
        barrier_id=barrier_id,
        participant_id=body.participant_id,
        barrier_status=state.status,  # type: ignore[arg-type]
        recorded=recorded,
        checked_in_count=len(state.checked_in),
        threshold=state.threshold,
    )


@router.get("/{barrier_id}/wait", response_model=WaitResult)
async def wait_for_barrier(
    barrier_id: str,
    timeout: float | None = Query(
        default=None,
        gt=0,
        description="Optional caller-side max wait in seconds. The barrier deadline "
        "always bounds the wait; on caller timeout the current "
        "state is returned with status 'open'.",
    ),
    r: redis.Redis = Depends(get_redis),
) -> WaitResult:
    """Block until the barrier trips or its deadline passes.

    Returns immediately with the frozen quorum snapshot once the threshold is
    met; at the deadline returns the checked-in members and the absent list.
    Waiters are woken via Redis Pub/Sub, so any number of concurrent waiters are
    all released reliably.
    """
    keys = _keys(barrier_id)
    state = await _get_state_or_404(r, barrier_id)

    if state.status == "open":
        pubsub = r.pubsub()
        await pubsub.subscribe(keys["channel"])
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            # Re-read the state *after* subscribing so an event can never be
            # missed between the state check and the subscription.
            while True:
                state = await _get_state_or_404(r, barrier_id)
                if state.status != "open":
                    break

                budget = state.deadline_ts - state.server_now
                if timeout is not None:
                    client_remaining = timeout - (loop.time() - started)
                    if client_remaining <= 0:
                        break  # caller-side timeout: return the still-open state
                    budget = min(budget, client_remaining)
                if budget <= 0:
                    continue  # deadline reached; the next _read_state finalizes

                # Sleep until a check-in/finalize event or the budget elapses.
                await pubsub.get_message(ignore_subscribe_messages=True, timeout=budget)
        finally:
            try:
                await pubsub.unsubscribe(keys["channel"])
            finally:
                close = getattr(pubsub, "aclose", None) or pubsub.close
                await close()

    return _wait_response(state)
