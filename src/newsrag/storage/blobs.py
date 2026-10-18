"""Content-addressed raw blob store: ``raw/ab/cd/<sha256>.html.zst`` (zstd level 9).

Owners: the fetcher installs blobs; the retention job expires them.

Installing a blob (rename into ``raw/``, fsync the directory, upsert the ``blobs``
row) happens inside the caller's write transaction, and retention unlinks a file
only inside a write transaction after re-checking its row. The SQLite write lock
therefore serializes the two, so a blob re-fetched while retention is expiring it
can never end up ``present`` with its file deleted.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import zstandard

from newsrag.ids import sha256_hex
from newsrag.storage.fsio import TmpArea, install, write_fsync
from newsrag.storage.state import transaction
from newsrag.timeutil import to_db

ZSTD_LEVEL = 9
BLOB_SUFFIX = ".html.zst"


class BlobCorrupt(RuntimeError):
    pass


def blob_rel_path(blob_hash: str) -> str:
    """Path relative to the data directory. Two levels of fan-out keep directories small."""
    return f"raw/{blob_hash[:2]}/{blob_hash[2:4]}/{blob_hash}{BLOB_SUFFIX}"


@dataclass(frozen=True)
class StagedBlob:
    blob_hash: str
    tmp_path: Path
    size_raw: int
    size_stored: int
    content_type: str | None


class BlobStore:
    def __init__(self, data_dir: Path, tmp: TmpArea) -> None:
        self.data_dir = data_dir
        self.tmp = tmp

    def path(self, blob_hash: str) -> Path:
        return self.data_dir / blob_rel_path(blob_hash)

    def stage(self, body: bytes, content_type: str | None = None) -> StagedBlob:
        """Compress and fsync ``body`` into this process's tmp area. No lock needed.

        Always staged, even if the blob may already exist: whether it does can only be
        decided under the write lock in ``commit``, and compressing ~50 KB is cheap.
        """
        compressed = zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(body)
        tmp_path = self.tmp.new_path(BLOB_SUFFIX)
        write_fsync(tmp_path, compressed)
        return StagedBlob(sha256_hex(body), tmp_path, len(body), len(compressed), content_type)

    def commit(
        self,
        conn: sqlite3.Connection,
        staged: StagedBlob,
        now: datetime,
        expires_at: datetime | None,
    ) -> None:
        """Make ``staged`` part of the manifest. Must run inside ``transaction(conn)``.

        A blob already ``present`` keeps its file; only its expiry can move later
        (NULL, "keep forever", always wins).
        """
        if not conn.in_transaction:
            raise RuntimeError("BlobStore.commit must run inside a write transaction")
        row = conn.execute(
            "SELECT state FROM blobs WHERE blob_hash = ?", (staged.blob_hash,)
        ).fetchone()
        exp = to_db(expires_at) if expires_at is not None else None
        if row is not None and row["state"] == "present":
            staged.tmp_path.unlink(missing_ok=True)
            conn.execute(
                "UPDATE blobs SET expires_at = CASE WHEN expires_at IS NULL OR ? IS NULL "
                "THEN NULL ELSE max(expires_at, ?) END WHERE blob_hash = ?",
                (exp, exp, staged.blob_hash),
            )
            return
        install(staged.tmp_path, self.path(staged.blob_hash))
        conn.execute(
            "INSERT INTO blobs (blob_hash, rel_path, size_raw, size_stored, content_type, "
            "stored_at, expires_at, state) VALUES (?, ?, ?, ?, ?, ?, ?, 'present') "
            "ON CONFLICT (blob_hash) DO UPDATE SET size_stored = excluded.size_stored, "
            "content_type = excluded.content_type, stored_at = excluded.stored_at, "
            "expires_at = excluded.expires_at, state = 'present'",
            (
                staged.blob_hash,
                blob_rel_path(staged.blob_hash),
                staged.size_raw,
                staged.size_stored,
                staged.content_type,
                to_db(now),
                exp,
            ),
        )

    def read(self, blob_hash: str, verify: bool = True) -> bytes:
        """Decompressed body. Callers check the manifest first: a file not listed as
        ``present`` does not exist, even if it is still on disk."""
        body = zstandard.ZstdDecompressor().decompress(self.path(blob_hash).read_bytes())
        if verify and sha256_hex(body) != blob_hash:
            raise BlobCorrupt(blob_hash)
        return body


def _unlink_if_not_present(conn: sqlite3.Connection, data_dir: Path, blob_hash: str) -> bool:
    """Unlink a blob file unless its row is ``present``, holding the write lock."""
    with transaction(conn):
        row = conn.execute("SELECT state FROM blobs WHERE blob_hash = ?", (blob_hash,)).fetchone()
        if row is not None and row["state"] == "present":
            return False
        path = data_dir / blob_rel_path(blob_hash)
        existed = path.exists()
        path.unlink(missing_ok=True)
        return existed


def expire_blobs(conn: sqlite3.Connection, data_dir: Path, now: datetime, batch: int = 1000) -> int:
    """Retention: mark due blobs ``expired`` (committed first), then unlink their files.

    A crash between the two steps leaves expired rows with files on disk;
    ``sweep_blobs`` removes those.
    """
    total = 0
    while True:
        with transaction(conn):
            hashes = [
                r[0]
                for r in conn.execute(
                    "SELECT blob_hash FROM blobs WHERE state = 'present' AND expires_at < ? "
                    "LIMIT ?",
                    (to_db(now), batch),
                )
            ]
            conn.executemany(
                "UPDATE blobs SET state = 'expired' WHERE blob_hash = ?", [(h,) for h in hashes]
            )
        for h in hashes:
            _unlink_if_not_present(conn, data_dir, h)
        total += len(hashes)
        if len(hashes) < batch:
            return total


def sweep_blobs(conn: sqlite3.Connection, data_dir: Path) -> int:
    """Delete blob files whose row is missing or ``expired``. Returns files removed.

    Walks all of ``raw/`` (millions of files at full scale), so it runs with the
    daily retention job, not at startup. Orphans are harmless in between: the
    manifest says they do not exist, and re-fetching the same body overwrites them.
    """
    raw = data_dir / "raw"
    if not raw.exists():
        return 0
    present = {r[0] for r in conn.execute("SELECT blob_hash FROM blobs WHERE state = 'present'")}
    removed = 0
    for path in raw.glob(f"*/*/*{BLOB_SUFFIX}"):
        blob_hash = path.name.removesuffix(BLOB_SUFFIX)
        # The snapshot may be stale (a blob committed since); the locked re-check decides.
        if blob_hash not in present and _unlink_if_not_present(conn, data_dir, blob_hash):
            removed += 1
    return removed
