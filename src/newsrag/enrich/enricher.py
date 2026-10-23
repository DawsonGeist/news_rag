"""The enricher process, ``newsrag run enricher`` (phase2-design.md, "Process model").

Each micro-batch:

1. **queue** new ``doc_revisions`` rows (incremental, by rowid) and **claim** up to
   ``batch_docs`` pending revisions, newest stored first;
2. **read** them through the manifest and **segment** them (CPU, ~5 ms a document);
3. **embed** passages and titles on the GPU in one call;
4. **stage** the segments as one Parquet part (tmp area, fsync);
5. **index**: write the vectors to Qdrant and wait, then drop the documents' older
   revisions from it. Searchable from here on;
6. **commit**, in one transaction: install the part, mark the revisions ``done``
   (with their vector counts) and record segment, embed and index events.

A crash before 6 leaves the revisions ``running``; the next start re-queues them, and
because point IDs are segment IDs the re-run overwrites rather than duplicates
(see ``enrich.vectors``). At startup the enricher compares the vector store with the
``done`` rows and re-queues whatever went missing.

Only English articles (full text or summary) are enriched; everything else is
``skipped`` with a reason, as is any revision older than the document's latest.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from newsrag.config import Settings
from newsrag.contracts.v1 import (
    ContentKind,
    Document,
    Segment,
    SegmentLevel,
    Stage,
    StageEvent,
    StageStatus,
    TextLocator,
)
from newsrag.enrich.embedder import Embedder
from newsrag.enrich.state import (
    EnrichClaim,
    claim,
    expected_total,
    expected_vectors,
    queue_new,
    reclaim_running,
    requeue,
)
from newsrag.enrich.tables import SEGMENT_SCHEMA, SEGMENTS, segments_to_table
from newsrag.enrich.vectors import Point, VectorIndex
from newsrag.obs.events import new_event, record, worker_id
from newsrag.segment.segmenter import segment
from newsrag.storage.compaction import compact_derived
from newsrag.storage.fsio import ProcessLock, TmpArea, sweep_tmp
from newsrag.storage.recovery import remove_uncommitted_table_files
from newsrag.storage.state import transaction
from newsrag.storage.tables import GROUP_LOCKS, install_part, read_revisions, stage_part
from newsrag.timeutil import to_db, utcnow

log = logging.getLogger(__name__)

# seg-1 passages and titles, bge-m3@5617a9f, float16 originals in Qdrant. Changing any of
# them is a new version, which re-enriches the archive into a new collection.
PIPELINE_VERSION = "enrich-1"
COLLECTION = "passages_enrich_1"
ENRICHER_LOCK = GROUP_LOCKS["enrichment"]

ELIGIBLE_KINDS = frozenset({ContentKind.FULL_TEXT, ContentKind.SUMMARY_ONLY})
LANGUAGES = frozenset({"en"})
EMBED_LEVELS = frozenset({SegmentLevel.PASSAGE, SegmentLevel.TITLE})


def ineligible_reason(doc: Document) -> str | None:
    """Why a Document is not enriched, or None. Paywalled pages hold only a teaser (and
    content must be open access); non-articles are section pages and the like."""
    if doc.content_kind not in ELIGIBLE_KINDS:
        return f"content_kind {doc.content_kind.value}"
    if doc.language not in LANGUAGES:
        return f"language {doc.language}"
    return None


def payload(doc: Document, seg: Segment) -> dict[str, Any]:
    """What a search hit carries: enough to filter (source, date, level) and to fetch the
    text from the archive by locator, without storing the text twice."""
    loc = seg.locator
    assert isinstance(loc, TextLocator)
    out: dict[str, Any] = {
        "doc_id": doc.doc_id,
        "revision": doc.revision,
        "level": seg.level.value,
        "position": seg.position,
        "field": loc.field,
        "start": loc.start,
        "end": loc.end,
        "source_id": doc.source_id,
        "content_kind": doc.content_kind.value,
        "ingested": int(doc.ingested_at.timestamp()),
        "pipeline_version": PIPELINE_VERSION,
    }
    if doc.published_at is not None:
        out["published"] = int(doc.published_at.timestamp())
    return out


@dataclass
class _Outcome:
    claim: EnrichClaim
    state: str  # done | skipped | failed | pending (retry)
    segments: int | None = None
    vectors: int = 0
    error: str | None = None
    detail: str | None = None


@dataclass
class BatchReport:
    claimed: int = 0
    states: dict[str, int] = field(default_factory=dict)
    vectors: int = 0
    seconds: dict[str, float] = field(default_factory=dict)


@dataclass
class ReconcileReport:
    expected_points: int = 0
    actual_points: int = 0
    mismatched: list[tuple[str, int]] = field(default_factory=list)
    orphaned: list[tuple[str, int]] = field(default_factory=list)
    repaired: bool = False

    @property
    def ok(self) -> bool:
        return not self.mismatched and not self.orphaned


class Enricher:
    def __init__(
        self,
        settings: Settings,
        conn: sqlite3.Connection,
        tmp: TmpArea,
        embedder: Embedder,
        index: VectorIndex,
        worker: str,
    ) -> None:
        self.s = settings
        self.conn = conn
        self.tmp = tmp
        self.embedder = embedder
        self.index = index
        self.worker = worker
        self.watermark = 0  # doc_revisions rowid; 0 = scan everything once
        self.last_compaction = time.monotonic()

    # --- one micro-batch ---------------------------------------------------------------

    def step(self) -> BatchReport:
        now = utcnow()
        self.watermark = queue_new(self.conn, PIPELINE_VERSION, self.watermark, now)
        claims = claim(
            self.conn,
            PIPELINE_VERSION,
            self.worker,
            self.s.enricher.batch_docs,
            self.s.enricher.lease_s,
            now,
        )
        if not claims:
            return BatchReport()
        return self.process(claims)

    def _latest_revisions(self, doc_ids: set[str]) -> dict[str, int]:
        out: dict[str, int] = {}
        ordered = sorted(doc_ids)
        for i in range(0, len(ordered), 500):
            chunk = ordered[i : i + 500]
            rows = self.conn.execute(
                "SELECT doc_id, max(revision) AS rev FROM doc_revisions "
                f"WHERE doc_id IN ({','.join('?' * len(chunk))}) GROUP BY doc_id",
                chunk,
            )
            out.update({r["doc_id"]: r["rev"] for r in rows})
        return out

    def process(self, claims: list[EnrichClaim]) -> BatchReport:
        t_start = utcnow()
        docs = read_revisions(self.conn, self.s.data_dir, [(c.doc_id, c.revision) for c in claims])
        latest = self._latest_revisions({c.doc_id for c in claims})
        outcomes: list[_Outcome] = []
        to_write: list[Segment] = []
        points: list[Point] = []
        texts: list[str] = []
        for c in claims:
            doc = docs.get((c.doc_id, c.revision))
            if doc is None:
                retry = c.attempt < self.s.enricher.max_attempts
                outcomes.append(
                    _Outcome(c, "pending" if retry else "failed", error="document_missing")
                )
                continue
            newest = latest.get(c.doc_id, c.revision)
            reason = (
                f"superseded by revision {newest}"
                if newest > c.revision
                else ineligible_reason(doc)
            )
            if reason is not None:
                outcomes.append(_Outcome(c, "skipped", detail=reason))
                continue
            try:
                segs = segment(doc, PIPELINE_VERSION)
            except Exception as e:  # deterministic: retrying the same text won't help
                log.exception("segmenting %s rev %d failed", c.doc_id, c.revision)
                outcomes.append(_Outcome(c, "failed", error="segment_error", detail=repr(e)[:500]))
                continue
            embed = [s for s in segs if s.level in EMBED_LEVELS]
            if not c.segments_committed:
                to_write.extend(segs)
            points.extend(Point(s.segment_id, payload(doc, s)) for s in embed)
            texts.extend(s.text for s in embed)
            outcomes.append(_Outcome(c, "done", segments=len(segs), vectors=len(embed)))
        t_segmented = utcnow()

        vectors = self.embedder.encode(texts)
        t_embedded = utcnow()

        part = stage_part(self.tmp, SEGMENTS, segments_to_table(to_write)) if to_write else None
        if points:
            self.index.upsert(points, vectors)
        done = [o for o in outcomes if o.state == "done"]
        self.index.delete_older_revisions({o.claim.doc_id: o.claim.revision for o in done})
        t_indexed = utcnow()

        with transaction(self.conn):
            now = utcnow()
            if part is not None:
                install_part(self.conn, self.s.data_dir, part, now)
            self._finish(outcomes, now)
            record(
                self.conn,
                self._events(outcomes, t_start, t_segmented, t_embedded, t_indexed),
            )
        report = BatchReport(claimed=len(claims), vectors=len(points))
        for o in outcomes:
            report.states[o.state] = report.states.get(o.state, 0) + 1
        report.seconds = {
            "read+segment": (t_segmented - t_start).total_seconds(),
            "embed": (t_embedded - t_segmented).total_seconds(),
            "index": (t_indexed - t_embedded).total_seconds(),
        }
        return report

    def _finish(self, outcomes: list[_Outcome], now: datetime) -> None:
        ts = to_db(now)
        for o in outcomes:
            c = o.claim
            if o.state == "pending":
                self.conn.execute(
                    "UPDATE enrichments SET state = 'pending', lease_owner = NULL, "
                    "lease_expires_at = NULL, last_error = ?, updated_at = ? "
                    "WHERE doc_id = ? AND revision = ? AND pipeline_version = ?",
                    (o.error, ts, c.doc_id, c.revision, PIPELINE_VERSION),
                )
                continue
            # A vectors-only re-run (segments_committed) keeps the original segment count.
            self.conn.execute(
                "UPDATE enrichments SET state = ?, lease_owner = NULL, lease_expires_at = NULL, "
                "last_error = ?, finished_at = ?, updated_at = ?, "
                "segments = coalesce(segments, ?), vectors = ? "
                "WHERE doc_id = ? AND revision = ? AND pipeline_version = ?",
                (
                    o.state,
                    o.error or (o.detail if o.state == "skipped" else None),
                    ts,
                    ts,
                    o.segments,
                    o.vectors if o.state == "done" else 0,
                    c.doc_id,
                    c.revision,
                    PIPELINE_VERSION,
                ),
            )
            if o.state == "done" and c.revision > 1:
                # Their points were just deleted from the vector store.
                self.conn.execute(
                    "UPDATE enrichments SET vectors = 0, updated_at = ? "
                    "WHERE doc_id = ? AND revision < ? AND pipeline_version = ? AND state = 'done'",
                    (ts, c.doc_id, c.revision, PIPELINE_VERSION),
                )

    def _events(
        self,
        outcomes: list[_Outcome],
        t_start: datetime,
        t_segmented: datetime,
        t_embedded: datetime,
        t_indexed: datetime,
    ) -> list[StageEvent]:
        events = []
        for o in outcomes:
            c = o.claim
            common: dict[str, Any] = {
                "worker": self.worker,
                "pipeline_version": PIPELINE_VERSION,
                "doc_id": c.doc_id,
                "revision": c.revision,
                "attempt": c.attempt,
            }
            if o.state != "done":
                status = {
                    "skipped": StageStatus.SKIPPED,
                    "failed": StageStatus.ERROR,
                    "pending": StageStatus.RETRY,
                }[o.state]
                events.append(
                    new_event(
                        Stage.SEGMENT,
                        status,
                        started_at=t_start,
                        finished_at=t_segmented,
                        enqueued_at=c.ingested_at,
                        error_code=o.error,
                        detail=o.detail,
                        **common,
                    )
                )
                continue
            # enqueued_at chains the stages, so "stored -> searchable" is index.finished_at
            # minus segment.enqueued_at (= the revision's ingested_at).
            for stage, start, end, enqueued in (
                (Stage.SEGMENT, t_start, t_segmented, c.ingested_at),
                (Stage.EMBED, t_segmented, t_embedded, t_segmented),
                (Stage.INDEX, t_embedded, t_indexed, t_embedded),
            ):
                events.append(
                    new_event(
                        stage,
                        StageStatus.OK,
                        started_at=start,
                        finished_at=end,
                        enqueued_at=enqueued,
                        **common,
                    )
                )
        return events

    # --- housekeeping ------------------------------------------------------------------

    def maybe_compact(self) -> None:
        if time.monotonic() - self.last_compaction < self.s.enricher.compact_interval_s:
            return
        report = compact_derived(
            self.conn,
            self.s.data_dir,
            self.tmp,
            SEGMENTS,
            SEGMENT_SCHEMA,
            utcnow(),
            self.s.enricher.compact_target_mb * 1024 * 1024,
        )
        self.last_compaction = time.monotonic()
        if report.inputs:
            log.info("compaction of %s: %s", SEGMENTS, report)

    def check_vectors(self) -> ReconcileReport | None:
        """Cheap startup check: a full reconcile only if the point count is off."""
        actual, expected = self.index.count(), expected_total(self.conn, PIPELINE_VERSION)
        if actual == expected:
            log.info("vector store holds the expected %d points", actual)
            return None
        log.warning("vector store holds %d points, expected %d: reconciling", actual, expected)
        report = reconcile(self.conn, self.index, repair=True, now=utcnow())
        log.warning(
            "reconcile: %d revisions re-queued, %d orphaned revisions removed",
            len(report.mismatched),
            len(report.orphaned),
        )
        return report

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            report = self.step()
            if report.claimed:
                log.info(
                    "batch: %d revisions %s, %d vectors, %s",
                    report.claimed,
                    report.states,
                    report.vectors,
                    {k: round(v, 1) for k, v in report.seconds.items()},
                )
            self.maybe_compact()
            if not report.claimed:
                stop.wait(self.s.enricher.poll_s)

    def run_until_idle(self) -> None:
        """Enrich everything queued, then return. For tests and one-off runs."""
        while self.step().claimed:
            pass


def reconcile(
    conn: sqlite3.Connection, index: VectorIndex, repair: bool, now: datetime
) -> ReconcileReport:
    """Compare the vector store with the ``done`` revisions.

    - **mismatched:** a ``done`` revision whose point count differs (lost writes, say).
      Repair deletes whatever is left of it and re-queues it for its vectors.
    - **orphaned:** points of a revision that isn't ``done`` (a crash after the upsert,
      before the commit). Repair deletes them; the revision's re-run writes them again.

    Repair must run with the enricher lock held: a running enricher's in-flight batch
    has points that are legitimately not ``done`` yet.
    """
    expected = expected_vectors(conn, PIPELINE_VERSION)
    actual = index.counts_by_revision()
    report = ReconcileReport(
        expected_points=sum(expected.values()),
        actual_points=sum(actual.values()),
        mismatched=sorted(k for k, n in expected.items() if actual.get(k, 0) != n),
        orphaned=sorted(k for k in actual if k not in expected),
    )
    if repair and not report.ok:
        index.delete_docs(report.orphaned + [k for k in report.mismatched if k in actual])
        requeue(conn, PIPELINE_VERSION, report.mismatched, now)
        report.repaired = True
    return report


@dataclass
class EnricherRecovery:
    tmp_removed: int
    table_files_removed: int
    requeued: int
    failed: int


def recover_enricher(
    conn: sqlite3.Connection, data_dir: Path, max_attempts: int, now: datetime
) -> EnricherRecovery:
    """Startup recovery. Caller holds the enricher lock."""
    requeued, failed = reclaim_running(conn, max_attempts, now)
    return EnricherRecovery(
        tmp_removed=sweep_tmp(data_dir / "tmp"),
        table_files_removed=remove_uncommitted_table_files(conn, data_dir, "enrichment"),
        requeued=requeued,
        failed=failed,
    )


def wait_for_index(index: VectorIndex, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            index.c.get_collections()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


def run_enricher(
    settings: Settings,
    conn: sqlite3.Connection,
    stop: threading.Event,
    make_embedder: Callable[[], Embedder] | None = None,
    index: VectorIndex | None = None,
) -> None:
    with ProcessLock(settings.locks_dir / ENRICHER_LOCK):
        rec = recover_enricher(conn, settings.data_dir, settings.enricher.max_attempts, utcnow())
        log.info("recovery: %s", rec)
        if index is None:
            from newsrag.enrich.embedder import DIMS

            index = VectorIndex.connect(settings.enricher.qdrant_url, COLLECTION, DIMS)
        wait_for_index(index, settings.enricher.qdrant_wait_s)
        index.ensure_collection()
        if make_embedder is None:
            from newsrag.enrich.embedder import SentenceTransformerEmbedder

            def make_embedder() -> Embedder:
                return SentenceTransformerEmbedder(
                    settings.enricher.device, settings.enricher.embed_batch
                )

        embedder = make_embedder()
        log.info("embedder %s on %s", embedder.model_id, settings.enricher.device)
        with TmpArea(settings.tmp_dir, "enricher") as tmp:
            loop = Enricher(settings, conn, tmp, embedder, index, worker_id("enricher"))
            loop.check_vectors()
            loop.run(stop)
