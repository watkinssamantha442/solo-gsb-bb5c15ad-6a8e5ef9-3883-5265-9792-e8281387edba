"""Runtime configuration, overridable via environment variables."""
from __future__ import annotations

import os

# Redis connection string. Inside docker-compose this points at the `redis` service.
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Seconds barrier data is kept in Redis after the deadline passes (then auto-expires).
BARRIER_RETENTION_SECONDS: int = int(os.getenv("BARRIER_RETENTION_SECONDS", "3600"))

# Max simultaneous Redis connections (each blocked waiter holds one pub/sub
# connection for its lifetime, plus one short-lived connection per command).
REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "1000"))
