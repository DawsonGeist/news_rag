"""Startup recovery and the invariant checker (contracts/v1/README.md, "Durability protocol").

Startup recovery is cheap and runs every time a component starts. The full blob
sweep walks millions of files, so it runs with the daily retention job instead
(``blobs.sweep_blobs``).
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq

from newsrag.storage.blobs import BLOB_SUFFIX, blob_rel_path
from newsrag.storage.fsio import file_sha256, fsync_dir, sweep_tmp
from newsrag.storage.state import transaction
from newsrag.storage.tables import DOCUMENT_SCHEMA
from newsrag.timeutil import to_db

# A lease belongs to the component that took it, so each reclaims only its own states.
_RECLAIM = {"fetching": "pending", "extracting": "fetched"}


@dataclass
class RecoveryReport:
    tmp_removed: int = 0
    table_files_removed: int = 0
    leases_reclaimed: int = 0


def reclaim_expired_leases(
    conn: sqlite3.Connection, state: Literal["fetching", "extracting"], now: datetime
) -> int:
    """Return rows whose lease has expired to the previous ready state. Work is
    idempotent (content-addressed blobs, deterministic extraction), so redoing it is safe."""
    with transaction(conn):
        cur = conn.execute(
            "UPDATE frontier SET state = ?, lease_owner = NULL, lease_expires_at = NULL, "
            "next_attempt_at = ?, updated_at = ? WHERE state = ? AND lease_expires_at < ?",
            (_RECLAIM[state], to_db(now), to_db(now), state, to_db(now)),
        )
        return cur.rowcount


def remove_uncommitted_table_files(conn: sqlite3.Connection, data_dir: Path) -> int:
    """Delete files under ``tables/`` without a ``committed`` manifest row: parts from a
    crash between install and commit, and parts superseded by compaction.

    Only the storage writer may call this, while holding the writer lock.
    """
    tables = data_dir / "tables"
    if not tables.exists():
        return 0
    committed = {
        r[0] for r in conn.execute("SELECT rel_path FROM table_files WHERE state = 'committed'")
    }
    removed = 0
    for path in tables.rglob("*.parquet"):
        if path.relative_to(data_dir).as_posix() not in committed:
            path.unlink()
            fsync_dir(path.parent)
            removed += 1
    return removed


def recover_writer(conn: sqlite3.Connection, data_dir: Path, now: datetime) -> RecoveryReport:
    """Startup recovery for the storage writer. Caller holds the writer lock."""
    return RecoveryReport(
        tmp_removed=sweep_tmp(data_dir / "tmp"),
        table_files_removed=remove_uncommitted_table_files(conn, data_dir),
        leases_reclaimed=reclaim_expired_leases(conn, "extracting", now),
    )


def verify_storage(conn: sqlite3.Connection, data_dir: Path, deep: bool = True) -> list[str]:
    """Check the storage invariants (the kill test's oracle). Returns problems found.

    ``deep`` re-hashes every committed file and blob and reads every part.
    """
    problems: list[str] = []
    for r in conn.execute(
        "SELECT file_id, rel_path, sha256, row_count FROM table_files WHERE state = 'committed'"
    ):
        path = data_dir / r["rel_path"]
        if not path.exists():
            problems.append(f"committed file missing: {r['rel_path']}")
            continue
        if deep and file_sha256(path) != r["sha256"]:
            problems.append(f"sha256 mismatch: {r['rel_path']}")
            continue
        if deep:
            table = pq.read_table(path, schema=DOCUMENT_SCHEMA, columns=["doc_id", "revision"])
            in_file = set(
                zip(table["doc_id"].to_pylist(), table["revision"].to_pylist(), strict=True)
            )
            if len(in_file) != r["row_count"]:
                problems.append(f"row_count mismatch: {r['rel_path']}")
            ledger = {
                (x["doc_id"], x["revision"])
                for x in conn.execute(
                    "SELECT doc_id, revision FROM doc_revisions WHERE file_id = ?", (r["file_id"],)
                )
            }
            if not ledger <= in_file:
                problems.append(f"doc_revisions rows not in {r['rel_path']}: {ledger - in_file}")

    for r in conn.execute(
        "SELECT d.doc_id, d.revision FROM doc_revisions d LEFT JOIN table_files t "
        "ON d.file_id = t.file_id WHERE t.state IS NOT 'committed'"
    ):
        problems.append(f"revision {r['doc_id']}:{r['revision']} not in a committed file")

    for r in conn.execute(
        "SELECT doc_id, group_concat(revision) AS revs, count(*) AS n, max(revision) AS mx "
        "FROM doc_revisions GROUP BY doc_id HAVING n != mx"
    ):
        problems.append(f"revisions of {r['doc_id']} not contiguous: {r['revs']}")
    dup_hashes = Counter(
        (r["doc_id"], r["content_hash"])
        for r in conn.execute("SELECT doc_id, content_hash FROM doc_revisions")
    )
    problems += [f"content stored twice: {k}" for k, n in dup_hashes.items() if n > 1]

    for r in conn.execute("SELECT blob_hash FROM blobs WHERE state = 'present'"):
        path = data_dir / blob_rel_path(r["blob_hash"])
        if not path.exists():
            problems.append(f"present blob missing: {r['blob_hash']}")
    expired = {r[0] for r in conn.execute("SELECT blob_hash FROM blobs WHERE state = 'expired'")}
    raw = data_dir / "raw"
    if raw.exists():
        for path in raw.glob(f"*/*/*{BLOB_SUFFIX}"):
            if path.name.removesuffix(BLOB_SUFFIX) in expired:
                problems.append(f"expired blob still on disk: {path.name}")

    for r in conn.execute(
        "SELECT f.url_id FROM frontier f LEFT JOIN doc_revisions d "
        "ON f.doc_id = d.doc_id AND f.revision = d.revision "
        "WHERE f.state = 'stored' AND d.doc_id IS NULL"
    ):
        problems.append(f"frontier row stored but revision missing: {r['url_id']}")
    return problems
