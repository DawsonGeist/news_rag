"""Storage writer commit path: revisions, Parquet part, manifest and frontier in one commit.

Only one writer process may run (``WRITER_LOCK``). Revision numbers are resolved
outside the write transaction, which is safe only because nobody else inserts
into ``doc_revisions``.

The extraction loop (lease ``fetched`` rows, run the extractor pool, buffer, flush)
is built on top of ``claim_for_extraction`` and ``StorageWriter.commit`` in P1.3.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from newsrag.contracts.v1 import Document, Stage, StageEvent, StageStatus
from newsrag.obs.events import new_event, record
from newsrag.storage.fsio import TmpArea
from newsrag.storage.state import transaction
from newsrag.storage.tables import install_part, stage_documents_part
from newsrag.timeutil import from_db, to_db, utcnow

WRITER_LOCK = "writer.lock"


@dataclass(frozen=True)
class ExtractedDoc:
    """One extractor result waiting to be committed.

    ``doc.revision`` and ``doc.ingested_at`` are provisional: the writer sets both.
    ``url_id`` is None for re-extraction runs, which touch no frontier row.
    """

    doc: Document
    url_id: str | None
    extract_enqueued_at: datetime | None
    extract_started_at: datetime
    extract_finished_at: datetime
    attempt: int = 1


@dataclass(frozen=True)
class Resolution:
    item: ExtractedDoc
    doc: Document  # with its final revision
    is_new: bool  # False: this content_hash is already stored for this doc_id


def resolve_revisions(conn: sqlite3.Connection, items: Sequence[ExtractedDoc]) -> list[Resolution]:
    """Apply the revision rules (contracts README, "Revisions and duplicates") to a batch,
    against both the ledger and earlier items in the same batch."""
    doc_ids = sorted({it.doc.doc_id for it in items})
    known: dict[str, dict[str, int]] = {d: {} for d in doc_ids}
    for i in range(0, len(doc_ids), 500):
        chunk = doc_ids[i : i + 500]
        rows = conn.execute(
            "SELECT doc_id, revision, content_hash FROM doc_revisions "
            f"WHERE doc_id IN ({','.join('?' * len(chunk))})",
            chunk,
        )
        for r in rows:
            known[r["doc_id"]][r["content_hash"]] = r["revision"]

    out: list[Resolution] = []
    for it in items:
        hashes = known[it.doc.doc_id]
        if it.doc.content_hash in hashes:
            rev = hashes[it.doc.content_hash]
            out.append(Resolution(it, it.doc.model_copy(update={"revision": rev}), False))
            continue
        rev = max(hashes.values(), default=0) + 1
        hashes[it.doc.content_hash] = rev
        out.append(Resolution(it, it.doc.model_copy(update={"revision": rev}), True))
    return out


@dataclass
class CommitResult:
    file_id: str | None = None
    new_revisions: list[tuple[str, int]] = field(default_factory=list)
    unchanged: list[tuple[str, int]] = field(default_factory=list)


class StorageWriter:
    def __init__(
        self,
        conn: sqlite3.Connection,
        data_dir: Path,
        tmp: TmpArea,
        worker: str,
        pipeline_version: str,
    ) -> None:
        self.conn = conn
        self.data_dir = data_dir
        self.tmp = tmp
        self.worker = worker
        self.pipeline_version = pipeline_version

    def commit(self, items: Sequence[ExtractedDoc]) -> CommitResult:
        """Durably store a batch: write the part, then one transaction for the manifest,
        the revision ledger, the frontier rows and the extract/store stage events."""
        result = CommitResult()
        if not items:
            return result
        flush_started = utcnow()
        # ingested_at is the flush time, so it is >= fetched_at and measures the writer's
        # share of ingest lag. It is not part of content_hash, so G1.6 is unaffected.
        resolutions = [
            Resolution(r.item, r.doc.model_copy(update={"ingested_at": flush_started}), r.is_new)
            for r in resolve_revisions(self.conn, items)
        ]
        new_docs = [r.doc for r in resolutions if r.is_new]
        part = stage_documents_part(self.tmp, new_docs) if new_docs else None

        with transaction(self.conn):
            now = utcnow()
            if part is not None:
                install_part(self.conn, self.data_dir, part, now)
                result.file_id = part.file_id
                self.conn.executemany(
                    "INSERT INTO doc_revisions (doc_id, revision, content_hash, raw_blob_hash, "
                    "canonical_url, source_id, content_kind, file_id, published_at, ingested_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            d.doc_id,
                            d.revision,
                            d.content_hash,
                            d.raw_blob_hash,
                            d.canonical_url,
                            d.source_id,
                            d.content_kind.value,
                            part.file_id,
                            to_db(d.published_at) if d.published_at else None,
                            to_db(d.ingested_at),
                        )
                        for d in new_docs
                    ],
                )
            events: list[StageEvent] = []
            for r in resolutions:
                (result.new_revisions if r.is_new else result.unchanged).append(
                    (r.doc.doc_id, r.doc.revision)
                )
                it = r.item
                if it.url_id is not None:
                    self.conn.execute(
                        "UPDATE frontier SET state = 'stored', doc_id = ?, revision = ?, "
                        "lease_owner = NULL, lease_expires_at = NULL, last_error = NULL, "
                        "ready_at = ?, updated_at = ? WHERE url_id = ?",
                        (r.doc.doc_id, r.doc.revision, to_db(now), to_db(now), it.url_id),
                    )
                ids = {"url_id": it.url_id, "doc_id": r.doc.doc_id, "revision": r.doc.revision}
                events.append(
                    new_event(
                        Stage.EXTRACT,
                        StageStatus.OK,
                        worker=self.worker,
                        started_at=it.extract_started_at,
                        finished_at=it.extract_finished_at,
                        enqueued_at=it.extract_enqueued_at,
                        attempt=it.attempt,
                        pipeline_version=self.pipeline_version,
                        **ids,
                    )
                )
                events.append(
                    new_event(
                        Stage.STORE,
                        StageStatus.OK if r.is_new else StageStatus.SKIPPED,
                        worker=self.worker,
                        started_at=flush_started,
                        finished_at=now,
                        enqueued_at=it.extract_finished_at,
                        detail=None if r.is_new else "unchanged content_hash",
                        pipeline_version=self.pipeline_version,
                        **ids,
                    )
                )
            record(self.conn, events)
        return result


@dataclass(frozen=True)
class ExtractionClaim:
    url_id: str
    url: str
    final_url: str | None
    raw_blob_hash: str
    attempts: int
    ready_at: datetime


def claim_for_extraction(
    conn: sqlite3.Connection, worker: str, limit: int, lease_s: int, now: datetime | None = None
) -> list[ExtractionClaim]:
    """Lease up to ``limit`` ``fetched`` rows (state -> ``extracting``)."""
    now = now or utcnow()
    expires = to_db(now + timedelta(seconds=lease_s))
    with transaction(conn):
        rows = conn.execute(
            "SELECT url_id, url, final_url, raw_blob_hash, attempts, ready_at FROM frontier "
            "WHERE state = 'fetched' AND next_attempt_at <= ? "
            "ORDER BY priority DESC, next_attempt_at LIMIT ?",
            (to_db(now), limit),
        ).fetchall()
        conn.executemany(
            "UPDATE frontier SET state = 'extracting', lease_owner = ?, lease_expires_at = ?, "
            "updated_at = ? WHERE url_id = ?",
            [(worker, expires, to_db(now), r["url_id"]) for r in rows],
        )
    return [
        ExtractionClaim(
            r["url_id"],
            r["url"],
            r["final_url"],
            r["raw_blob_hash"],
            r["attempts"],
            from_db(r["ready_at"]),
        )
        for r in rows
    ]
