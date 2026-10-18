"""Publication-date parsing. Never guesses a timezone.

- A full timestamp must carry its offset; a naive timestamp is discarded.
- A date with no time ("2026-10-17", a URL path) is pinned to 12:00 UTC. Every US
  publication time on that local date then falls within 12 hours of it, which keeps
  date-only sources inside Gate 1's ±24 h tolerance.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, time, timedelta

DATE_ONLY_PIN = time(12, 0, tzinfo=UTC)
EARLIEST = datetime(1995, 1, 1, tzinfo=UTC)
FUTURE_SLACK = timedelta(days=1)

_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# /2026/10/17/ or /2026/10/17- in a path; month and day may lack a leading zero.
_URL_DATE = re.compile(r"/(20\d{2}|19\d{2})/(\d{1,2})/(\d{1,2})(?:/|-|$)")


def pin_date(d: date) -> datetime:
    return datetime.combine(d, DATE_ONLY_PIN)


def parse_datetime(value: object) -> datetime | None:
    """Parse an ISO 8601 date or timestamp. Returns aware UTC, or None if absent,
    unparseable or naive."""
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if _DATE_ONLY.match(s):
        try:
            return pin_date(date.fromisoformat(s))
        except ValueError:
            return None
    # Python's parser wants "+HH:MM"; some sites write "+HHMM" or a space separator.
    s = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", s.replace(" ", "T", 1))
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        return None
    return dt.astimezone(UTC)


def url_path_date(url: str) -> datetime | None:
    m = _URL_DATE.search(url)
    if not m:
        return None
    try:
        return pin_date(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
    except ValueError:
        return None


def plausible(dt: datetime | None, fetched_at: datetime) -> datetime | None:
    """Reject dates before the web-news era or after the page was fetched."""
    if dt is None or dt < EARLIEST or dt > fetched_at + FUTURE_SLACK:
        return None
    return dt
