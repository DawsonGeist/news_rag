"""The ``sources`` and ``feeds`` tables: candidate import and status changes.

Owner: the feed vetter (component 1). The poller writes only the poll columns of
``feeds`` (etag, intervals, failures) and may pause a feed that keeps failing.

**Manual decisions are final.** A human accept/reject/review is recorded as a
``feed_health`` row with ``vetter_version = "manual"``. A feed whose latest health row
is manual is never re-vetted or overwritten by the vetter, not even with ``--recheck``
or after a ``VETTER_VERSION`` bump: re-reviewing feeds costs a human far more than
re-vetting costs the machine.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

from newsrag import ids
from newsrag.contracts.v1 import FeedCandidate, FeedHealthReport, FeedVerdict
from newsrag.storage.state import transaction
from newsrag.timeutil import to_db

FEED_STATUSES = frozenset({"candidate", "active", "review", "rejected", "paused"})
MANUAL_VERSION = "manual"
# Statuses that are decisions (and lock the feed); candidate and paused are operational.
MANUAL_VERDICTS = {
    "active": FeedVerdict.ACCEPT,
    "rejected": FeedVerdict.REJECT,
    "review": FeedVerdict.REVIEW,
}

LATEST_HEALTH_SQL = (
    "SELECT h.feed_id, json_extract(h.report_json, '$.vetter_version') AS version "
    "FROM feed_health h WHERE h.checked_at = "
    "(SELECT max(checked_at) FROM feed_health x WHERE x.feed_id = h.feed_id)"
)
"""Latest health row per feed: (feed_id, version)."""


def is_manual(conn: sqlite3.Connection, feed_id: str) -> bool:
    row = conn.execute(
        "SELECT json_extract(report_json, '$.vetter_version') FROM feed_health "
        "WHERE feed_id = ? ORDER BY checked_at DESC LIMIT 1",
        (feed_id,),
    ).fetchone()
    return row is not None and row[0] == MANUAL_VERSION


def load_candidates(path: Path) -> list[FeedCandidate]:
    rows = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    return [FeedCandidate.model_validate(r) for r in rows]


@dataclass
class ImportStats:
    sources_new: int = 0
    feeds_new: int = 0
    feeds_updated: int = 0


def import_candidates(
    conn: sqlite3.Connection, candidates: Iterable[FeedCandidate], now: datetime
) -> ImportStats:
    """Upsert candidates. Idempotent; never touches a feed's status or poll state, so
    re-importing a regenerated list keeps every vetting verdict."""
    stats = ImportStats()
    n = to_db(now)
    with transaction(conn):
        for c in candidates:
            cur = conn.execute(
                "INSERT INTO sources (source_id, country, display_name, listed_rank, created_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT (source_id) DO NOTHING",
                (c.source_id, c.country, c.display_name, c.listed_rank, n),
            )
            stats.sources_new += cur.rowcount
            exists = conn.execute("SELECT 1 FROM feeds WHERE feed_id = ?", (c.feed_id,)).fetchone()
            conn.execute(
                "INSERT INTO feeds (feed_id, feed_url, source_id, categories_json, medium, origin, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (feed_id) DO UPDATE SET categories_json = excluded.categories_json, "
                "medium = excluded.medium, updated_at = excluded.updated_at",
                (
                    c.feed_id,
                    c.feed_url,
                    c.source_id,
                    json.dumps(c.categories),
                    c.medium.value,
                    c.origin.value,
                    n,
                    n,
                ),
            )
            if exists:
                stats.feeds_updated += 1
            else:
                stats.feeds_new += 1
    return stats


def resolve_feed(conn: sqlite3.Connection, feed_ref: str) -> str | None:
    """A feed_id from either a feed_id or a feed URL."""
    if len(feed_ref) == 32 and all(ch in "0123456789abcdef" for ch in feed_ref):
        candidate = feed_ref
    else:
        try:
            candidate = ids.feed_id(ids.normalize_url(feed_ref))
        except ids.InvalidURL:
            return None
    row = conn.execute("SELECT feed_id FROM feeds WHERE feed_id = ?", (candidate,)).fetchone()
    return row[0] if row else None


def set_status(
    conn: sqlite3.Connection,
    feed_ids: Iterable[str],
    status: str,
    reason: str | None,
    now: datetime,
) -> int:
    """Change feed status. Re-activating a feed clears its failure count so it is
    polled right away. Not a decision: see ``decide_manually``."""
    with transaction(conn):
        return _set_status(conn, feed_ids, status, reason, now)


def decide_manually(
    conn: sqlite3.Connection,
    feed_ids: Iterable[str],
    status: str,
    reason: str,
    now: datetime,
) -> int:
    """A human verdict: set the status and lock the feed against re-vetting. Only
    ``active``, ``rejected`` and ``review`` are decisions."""
    if status not in MANUAL_VERDICTS:
        raise ValueError(f"not a decision status: {status}")
    feed_ids = list(feed_ids)
    with transaction(conn):
        changed = _set_status(conn, feed_ids, status, f"manual: {reason}", now)
        for fid in feed_ids:
            url = conn.execute("SELECT feed_url FROM feeds WHERE feed_id = ?", (fid,)).fetchone()
            if url is None:
                continue
            report = FeedHealthReport(
                feed_id=fid,
                feed_url=url[0],
                checked_at=now,
                vetter_version=MANUAL_VERSION,
                verdict=MANUAL_VERDICTS[status],
                detail=f"manual: {reason}",
            )
            conn.execute(
                "INSERT INTO feed_health (feed_id, checked_at, verdict, report_json) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (feed_id, checked_at) DO UPDATE SET "
                "verdict = excluded.verdict, report_json = excluded.report_json",
                (fid, to_db(now), report.verdict.value, report.model_dump_json()),
            )
    return changed


def _set_status(
    conn: sqlite3.Connection,
    feed_ids: Iterable[str],
    status: str,
    reason: str | None,
    now: datetime,
) -> int:
    if status not in FEED_STATUSES:
        raise ValueError(f"unknown feed status: {status}")
    n = to_db(now)
    changed = 0
    for fid in feed_ids:
        cur = conn.execute(
            "UPDATE feeds SET status = ?, status_reason = ?, updated_at = ?, "
            "consecutive_failures = CASE WHEN ? = 'active' THEN 0 "
            "ELSE consecutive_failures END, "
            "next_poll_at = CASE WHEN ? = 'active' THEN NULL ELSE next_poll_at END "
            "WHERE feed_id = ?",
            (status, reason, n, status, status, fid),
        )
        changed += cur.rowcount
    return changed
