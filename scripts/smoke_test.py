#!/usr/bin/env python3
"""End-to-end smoke test against a running Barrier API.

Usage:
    pip install -r requirements-dev.txt
    python scripts/smoke_test.py            # defaults to http://localhost:8000
    BARRIER_API_URL=http://host:port python scripts/smoke_test.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx

BASE_URL = os.getenv("BARRIER_API_URL", "http://localhost:8000")


def ok(name: str) -> None:
    print(f"  [PASS] {name}")


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        # --- health ---------------------------------------------------------
        r = await client.get("/healthz")
        r.raise_for_status()
        ok("health check")

        # --- validation -----------------------------------------------------
        r = await client.post(
            "/barriers",
            json={"participants": ["a", "b"], "threshold": 3, "ttl_seconds": 30},
        )
        assert r.status_code == 422, r.text
        r = await client.post(
            "/barriers",
            json={
                "participants": ["a"],
                "threshold": 1,
                "ttl_seconds": 30,
                "deadline": "2999-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 422, r.text
        ok("invalid creations rejected (threshold > roster, both deadline modes)")

        # --- concurrent check-ins freeze the quorum atomically ---------------
        r = await client.post(
            "/barriers",
            json={
                "participants": [f"p{i}" for i in range(10)],
                "threshold": 5,
                "ttl_seconds": 60,
            },
        )
        assert r.status_code == 201, r.text
        barrier = r.json()
        bid = barrier["barrier_id"]
        assert barrier["status"] == "open"
        assert barrier["checked_in_count"] == 0

        # Three waiters subscribe before any check-in.
        waiters = [
            asyncio.create_task(client.get(f"/barriers/{bid}/wait")) for _ in range(3)
        ]
        await asyncio.sleep(0.3)

        async def checkin(pid: str) -> httpx.Response:
            return await client.post(
                f"/barriers/{bid}/checkin", json={"participant_id": pid}
            )

        # 10 distinct participants + 2 duplicates, all at once.
        started = time.monotonic()
        responses = await asyncio.gather(
            *(checkin(f"p{i}") for i in range(10)),
            checkin("p0"),
            checkin("p1"),
        )
        assert all(r.status_code == 200 for r in responses), [r.text for r in responses]

        # Every waiter must be woken with the identical frozen snapshot.
        wait_results = [await w for w in waiters]
        elapsed = time.monotonic() - started
        snapshots = {tuple(w.json()["members"]) for w in wait_results}
        assert all(w.json()["status"] == "met" for w in wait_results)
        assert len(snapshots) == 1, f"waiters disagree: {snapshots}"
        frozen = list(snapshots)[0]
        assert len(frozen) == 5, f"frozen quorum must have exactly 5 members: {frozen}"
        assert elapsed < 10, "waiters were not woken promptly"
        ok(f"quorum of 5 frozen atomically; 3 waiters woken in {elapsed:.2f}s")

        # Duplicates were idempotent no-ops.
        r = await client.get(f"/barriers/{bid}")
        state = r.json()
        assert state["status"] == "met"
        assert sorted(state["met_members"]) == sorted(frozen)
        ok("frozen snapshot stable on re-read")

        # Late check-ins must not rewrite the frozen result.
        r = await checkin("p9")
        assert r.status_code == 200 and r.json()["barrier_status"] == "met"
        assert r.json()["recorded"] is False
        r = await client.get(f"/barriers/{bid}")
        assert sorted(r.json()["met_members"]) == sorted(frozen)
        ok("late check-in did not rewrite the frozen result")

        # --- idempotency of a single participant -----------------------------
        r = await client.post(
            "/barriers",
            json={"participants": ["solo"], "threshold": 1, "ttl_seconds": 30},
        )
        bid2 = r.json()["barrier_id"]
        r1 = await client.post(f"/barriers/{bid2}/checkin", json={"participant_id": "solo"})
        r2 = await client.post(f"/barriers/{bid2}/checkin", json={"participant_id": "solo"})
        assert r1.json()["recorded"] is True
        assert r2.json()["recorded"] is False
        assert r2.json()["checked_in_count"] == 1
        ok("duplicate check-in is idempotent")

        # --- non-participant rejected ----------------------------------------
        r = await client.post(
            "/barriers",
            json={"participants": ["a", "b"], "threshold": 2, "ttl_seconds": 30},
        )
        bid3 = r.json()["barrier_id"]
        r = await client.post(f"/barriers/{bid3}/checkin", json={"participant_id": "intruder"})
        assert r.status_code == 403, r.text
        r = await client.get("/barriers/does-not-exist")
        assert r.status_code == 404
        ok("non-participant rejected (403), unknown barrier 404")

        # --- deadline: wait returns checked-in + absent -----------------------
        r = await client.post(
            "/barriers",
            json={"participants": ["a", "b", "c"], "threshold": 3, "ttl_seconds": 2},
        )
        bid4 = r.json()["barrier_id"]
        await client.post(f"/barriers/{bid4}/checkin", json={"participant_id": "a"})

        async def wait_deadline() -> httpx.Response:
            return await client.get(f"/barriers/{bid4}/wait")

        started = time.monotonic()
        w1, w2 = await asyncio.gather(wait_deadline(), wait_deadline())
        elapsed = time.monotonic() - started
        for w in (w1, w2):
            body = w.json()
            assert body["status"] == "closed", body
            assert body["checked_in"] == ["a"], body
            assert sorted(body["absent"]) == ["b", "c"], body
        assert 1.5 <= elapsed < 10, f"wait returned after {elapsed:.2f}s (deadline was 2s)"
        ok(f"deadline reached: 2 waiters got checked_in + absent after {elapsed:.2f}s")

        # Late check-in after the deadline is rejected.
        r = await client.post(f"/barriers/{bid4}/checkin", json={"participant_id": "b"})
        assert r.status_code == 409, r.text
        ok("check-in after deadline rejected (409)")

        # --- caller-side timeout ----------------------------------------------
        r = await client.post(
            "/barriers",
            json={"participants": ["x", "y"], "threshold": 2, "ttl_seconds": 60},
        )
        bid5 = r.json()["barrier_id"]
        r = await client.get(f"/barriers/{bid5}/wait", params={"timeout": 1})
        assert r.json()["status"] == "open", r.text
        ok("caller-side timeout returns current open state")

    print("\nALL SMOKE TESTS PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AssertionError as exc:
        print(f"\nSMOKE TEST FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
