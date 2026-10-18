"""Daily retention: expire raw blobs past ``retention.raw_days``, sweep orphaned blob
files, and prune the poll log. Run by the supervisor once a day, or by hand with
``newsrag storage retention``.

Stage events are not pruned yet: the design rolls them up into hourly percentiles after
``retention.stage_event_days``, which is deferred until the table's size matters (see
docs/decisions.md). Lag reports read the raw rows.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from newsrag.config import Settings
from newsrag.storage.blobs import expire_blobs, sweep_blobs
from newsrag.storage.state import transaction
from newsrag.timeutil import to_db


@dataclass
class RetentionReport:
    blobs_expired: int
    orphans_swept: int
    polls_pruned: int


def run_retention(conn: sqlite3.Connection, settings: Settings, now: datetime) -> RetentionReport:
    expired = expire_blobs(conn, settings.data_dir, now)
    swept = sweep_blobs(conn, settings.data_dir)
    cutoff = to_db(now - timedelta(days=settings.retention.poll_log_days))
    with transaction(conn):
        pruned = conn.execute("DELETE FROM feed_polls WHERE polled_at < ?", (cutoff,)).rowcount
    return RetentionReport(expired, swept, pruned)
