"""Re-extraction check (Gate 1, G1.6): re-run the extractor over stored raw blobs and
confirm it reproduces each Document's ``content_hash``.

Only Documents whose ``extractor_version`` equals the current one are compared: a
different version is expected to differ. Documents whose raw blob has expired are
counted, not compared. The frontier hints the extractor used (content type, feed
summary and date) come from the frontier row that produced the revision.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from concurrent.futures import Executor
from dataclasses import dataclass, field

from newsrag.contracts.v1 import Document
from newsrag.extract.extractor import ExtractContext, extract, extractor_version
from newsrag.storage.blobs import BlobStore
from newsrag.storage.tables import DOCUMENTS, committed_files, read_documents
from newsrag.timeutil import from_db

BATCH = 256


@dataclass
class ReextractReport:
    compared: int = 0
    matched: int = 0
    other_version: int = 0
    blob_missing: int = 0
    mismatches: list[str] = field(default_factory=list)  # "doc_id:revision"

    @property
    def passes(self) -> bool:
        return not self.mismatches and self.compared > 0


def _context(conn: sqlite3.Connection, doc: Document) -> ExtractContext:
    row = conn.execute(
        "SELECT content_type, feed_summary, feed_published_at FROM frontier "
        "WHERE doc_id = ? AND raw_blob_hash = ? LIMIT 1",
        (doc.doc_id, doc.raw_blob_hash),
    ).fetchone()
    return ExtractContext(
        final_url=doc.final_url,
        source_id=doc.source_id,
        discovered_at=doc.discovered_at,
        fetched_at=doc.fetched_at,
        pipeline_version=doc.pipeline_version,
        content_type=row["content_type"] if row else None,
        feed_id=doc.feed_id,
        feed_title=doc.feed_title,
        # The Document keeps the summary it was extracted with; prefer it.
        feed_summary=doc.feed_summary
        if doc.feed_summary is not None
        else (row["feed_summary"] if row else None),
        feed_published_at=from_db(row["feed_published_at"])
        if row and row["feed_published_at"]
        else None,
    )


def _hash(body: bytes, ctx: ExtractContext) -> str | None:
    out = extract(body, ctx)
    return out.content_hash if isinstance(out, Document) else None


def reextract(
    conn: sqlite3.Connection,
    blobs: BlobStore,
    pool: Executor | None = None,
    sample: int | None = None,
) -> ReextractReport:
    report = ReextractReport()
    current = extractor_version()
    present = {r[0] for r in conn.execute("SELECT blob_hash FROM blobs WHERE state = 'present'")}

    def jobs() -> Iterator[tuple[Document, bytes, ExtractContext]]:
        taken = 0
        for path in committed_files(conn, blobs.data_dir, DOCUMENTS):
            for doc in read_documents([path]):
                if doc.extractor_version != current:
                    report.other_version += 1
                    continue
                if doc.raw_blob_hash not in present:
                    report.blob_missing += 1
                    continue
                if sample is not None and taken >= sample:
                    return
                taken += 1
                yield doc, blobs.read(doc.raw_blob_hash), _context(conn, doc)

    # Batches keep memory bounded: the archive holds millions of blobs.
    batch: list[tuple[Document, bytes, ExtractContext]] = []
    for job in jobs():
        batch.append(job)
        if len(batch) >= BATCH:
            _compare(batch, pool, report)
            batch = []
    _compare(batch, pool, report)
    return report


def _compare(
    batch: list[tuple[Document, bytes, ExtractContext]],
    pool: Executor | None,
    report: ReextractReport,
) -> None:
    bodies = [b for _, b, _ in batch]
    ctxs = [c for _, _, c in batch]
    hashes = pool.map(_hash, bodies, ctxs, chunksize=16) if pool else map(_hash, bodies, ctxs)
    for (doc, _, _), h in zip(batch, hashes, strict=True):
        report.compared += 1
        if h == doc.content_hash:
            report.matched += 1
        else:
            report.mismatches.append(f"{doc.doc_id}:{doc.revision}")
