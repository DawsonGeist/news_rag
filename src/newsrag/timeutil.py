"""UTC time helpers. Every timestamp in the system is timezone-aware UTC.

SQLite stores timestamps as fixed-width text (``DB_FORMAT``) so that string
order equals time order.
"""

from __future__ import annotations

from datetime import UTC, datetime

DB_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def utcnow() -> datetime:
    return datetime.now(UTC)


def ensure_utc(dt: datetime) -> datetime:
    """Convert an aware datetime to UTC. Naive datetimes are rejected, never guessed."""
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f"naive datetime not allowed: {dt!r}")
    return dt.astimezone(UTC)


def to_db(dt: datetime) -> str:
    return ensure_utc(dt).strftime(DB_FORMAT)


def from_db(value: str) -> datetime:
    return datetime.strptime(value, DB_FORMAT).replace(tzinfo=UTC)
