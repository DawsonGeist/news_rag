"""The ``enrichments`` table (owner: enricher): queueing, claiming and recovery.

Only one enricher runs (``GROUP_LOCKS["enrichment"]``), so like the storage writer it can
treat every ``running`` row it sees at startup as its dead predecessor's.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from newsrag.storage.state import transaction
from newsrag.timeutil import from_db, to_db


@dataclass(frozen=True)
class EnrichClaim:
    doc_id: str
    revision: int
    ingested_at: datetime
    attempt: int  # 1-based, this attempt included
    # Segments already committed by an earlier run (a reconcile re-queued the revision
    # because its vectors went missing): only the vectors are redone.
    segments_committed: bool


def queue_new(
    conn: sqlite3.Connection, pipeline_version: str, after_rowid: int, now: datetime
) -> int:
    """Queue every ``doc_revisions`` row past ``after_rowid`` that has no row for this
    pipeline version yet. Returns the new watermark (the highest rowid seen).

    ``after_rowid=0`` scans everything (startup); later calls only look at new rows.
    rowids only grow: the writer only inserts into ``doc_revisions``, and compaction
    only updates ``file_id``.
    """
    with transaction(conn):
        top = conn.execute("SELECT coalesce(max(rowid), 0) FROM doc_revisions").fetchone()[0]
        if top > after_rowid:
            conn.execute(
                "INSERT OR IGNORE INTO enrichments (doc_id, revision, pipeline_version, state, "
                "ingested_at, queued_at, updated_at) "
                "SELECT doc_id, revision, ?, 'pending', ingested_at, ?, ? FROM doc_revisions "
                "WHERE rowid > ? AND rowid <= ?",
                (pipeline_version, to_db(now), to_db(now), after_rowid, top),
            )
    return int(top)


def claim(
    conn: sqlite3.Connection,
    pipeline_version: str,
    worker: str,
    limit: int,
    lease_s: float,
    now: datetime,
) -> list[EnrichClaim]:
    """Lease up to ``limit`` pending revisions, newest stored first, so fresh articles
    never wait behind a backfill."""
    expires = to_db(now + timedelta(seconds=lease_s))
    with transaction(conn):
        rows = conn.execute(
            "SELECT doc_id, revision, ingested_at, attempts, segments FROM enrichments "
            "WHERE pipeline_version = ? AND state = 'pending' "
            "ORDER BY ingested_at DESC LIMIT ?",
            (pipeline_version, limit),
        ).fetchall()
        conn.executemany(
            "UPDATE enrichments SET state = 'running', attempts = attempts + 1, "
            "lease_owner = ?, lease_expires_at = ?, started_at = ?, updated_at = ? "
            "WHERE doc_id = ? AND revision = ? AND pipeline_version = ?",
            [
                (
                    worker,
                    expires,
                    to_db(now),
                    to_db(now),
                    r["doc_id"],
                    r["revision"],
                    pipeline_version,
                )
                for r in rows
            ],
        )
    return [
        EnrichClaim(
            r["doc_id"],
            r["revision"],
            from_db(r["ingested_at"]),
            r["attempts"] + 1,
            r["segments"] is not None,
        )
        for r in rows
    ]


def reclaim_running(conn: sqlite3.Connection, max_attempts: int, now: datetime) -> tuple[int, int]:
    """Startup: hand every ``running`` row back to ``pending``, or fail it once it has
    used up its attempts. A batch that kills the process every time (a poison
    document, say) therefore stops after ``max_attempts`` restarts.
    Returns (requeued, failed)."""
    with transaction(conn):
        failed = conn.execute(
            "UPDATE enrichments SET state = 'failed', lease_owner = NULL, lease_expires_at = NULL, "
            "last_error = coalesce(last_error, 'enricher died during this revision'), "
            "finished_at = ?, updated_at = ? WHERE state = 'running' AND attempts >= ?",
            (to_db(now), to_db(now), max_attempts),
        ).rowcount
        requeued = conn.execute(
            "UPDATE enrichments SET state = 'pending', lease_owner = NULL, "
            "lease_expires_at = NULL, updated_at = ? WHERE state = 'running'",
            (to_db(now),),
        ).rowcount
    return requeued, failed


def expected_vectors(conn: sqlite3.Connection, pipeline_version: str) -> dict[tuple[str, int], int]:
    """(doc_id, revision) -> points the vector store must hold, for every ``done`` row."""
    rows = conn.execute(
        "SELECT doc_id, revision, vectors FROM enrichments "
        "WHERE pipeline_version = ? AND state = 'done' AND vectors > 0",
        (pipeline_version,),
    )
    return {(r["doc_id"], r["revision"]): r["vectors"] for r in rows}


def expected_total(conn: sqlite3.Connection, pipeline_version: str) -> int:
    row = conn.execute(
        "SELECT coalesce(sum(vectors), 0) FROM enrichments "
        "WHERE pipeline_version = ? AND state = 'done'",
        (pipeline_version,),
    ).fetchone()
    return int(row[0])


def requeue(
    conn: sqlite3.Connection, pipeline_version: str, keys: list[tuple[str, int]], now: datetime
) -> None:
    """Send revisions back to ``pending`` for their vectors to be redone. Their committed
    segments are kept (``segments`` stays set), so the re-run writes vectors only."""
    with transaction(conn):
        conn.executemany(
            "UPDATE enrichments SET state = 'pending', attempts = 0, vectors = NULL, "
            "last_error = 'requeued by reconcile', updated_at = ? "
            "WHERE doc_id = ? AND revision = ? AND pipeline_version = ?",
            [(to_db(now), d, r, pipeline_version) for d, r in keys],
        )
