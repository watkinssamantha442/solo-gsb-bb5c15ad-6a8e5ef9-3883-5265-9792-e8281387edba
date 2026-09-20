"""Pydantic request/response schemas for the Barrier API."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class BarrierCreate(BaseModel):
    """Payload for POST /barriers.

    Exactly one of ``deadline`` (absolute ISO-8601 timestamp) or
    ``ttl_seconds`` (relative, from creation) must be given.
    """

    model_config = ConfigDict(extra="forbid")

    participants: list[str] = Field(
        min_length=1,
        description="Identifiers of the participants allowed to check in.",
        examples=[["worker-1", "worker-2", "worker-3"]],
    )
    threshold: int = Field(
        ge=1,
        description="Number of distinct participants required to trip the barrier.",
    )
    deadline: datetime | None = Field(
        default=None,
        description="Absolute ISO-8601 deadline. Naive datetimes are treated as UTC.",
    )
    ttl_seconds: float | None = Field(
        default=None,
        gt=0,
        description="Relative deadline: seconds from barrier creation.",
    )

    @field_validator("participants")
    @classmethod
    def _participants_are_non_blank(cls, value: list[str]) -> list[str]:
        for participant in value:
            if not isinstance(participant, str) or not participant.strip():
                raise ValueError("participant ids must be non-empty, non-blank strings")
        return value

    @model_validator(mode="after")
    def _validate_threshold_and_deadline(self) -> "BarrierCreate":
        unique = set(self.participants)
        if self.threshold > len(unique):
            raise ValueError(
                f"threshold ({self.threshold}) exceeds the number of unique "
                f"participants ({len(unique)}); the barrier could never be met"
            )
        if (self.deadline is None) == (self.ttl_seconds is None):
            raise ValueError("provide exactly one of 'deadline' or 'ttl_seconds'")
        deadline_utc = self.deadline_utc()
        if deadline_utc is not None and deadline_utc <= datetime.now(timezone.utc):
            raise ValueError("deadline must be in the future")
        return self

    def deadline_utc(self) -> datetime | None:
        """Return the deadline as a timezone-aware UTC datetime (or None)."""
        if self.deadline is None:
            return None
        if self.deadline.tzinfo is None:
            return self.deadline.replace(tzinfo=timezone.utc)
        return self.deadline.astimezone(timezone.utc)


class CheckInRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    participant_id: str = Field(min_length=1, examples=["worker-1"])


class BarrierState(BaseModel):
    """Full state of a barrier."""

    barrier_id: str
    status: Literal["open", "met", "closed"]
    threshold: int
    deadline: datetime
    participants: list[str]
    checked_in: list[str]
    checked_in_count: int
    absent: list[str]
    remaining_seconds: float
    # Frozen quorum snapshot, only present once status == "met".
    met_members: list[str] | None = None
    met_at: datetime | None = None


class CheckInResult(BaseModel):
    barrier_id: str
    participant_id: str
    barrier_status: Literal["open", "met", "closed"]
    recorded: bool = Field(
        description="True if this call newly recorded the participant; "
        "False for duplicate/late no-op check-ins."
    )
    checked_in_count: int
    threshold: int


class WaitResult(BaseModel):
    """Result of the blocking wait endpoint.

    * status "met":    ``members`` holds the frozen quorum snapshot.
    * status "closed": ``checked_in``/``absent`` hold the attendance at the deadline.
    * status "open":   the caller's ``timeout`` elapsed first; current attendance is
                       returned and the caller may wait again.
    """

    barrier_id: str
    status: Literal["open", "met", "closed"]
    members: list[str] | None = None
    met_at: datetime | None = None
    checked_in: list[str] | None = None
    absent: list[str] | None = None
