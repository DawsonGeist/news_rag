"""The storage writer process: lease ``fetched`` rows, extract them in a process pool,
buffer the Documents and flush them through ``StorageWriter.commit``.

A flush happens at ``writer.flush_rows`` Documents or when the oldest buffered one is
``writer.flush_seconds`` old, which bounds the writer's share of ingest lag.

Extraction runs untrusted pages through C extensions, so a worker can hang or die.
Failures must be charged to the page that caused them, never to its neighbours:

- **Hangs.** Each worker reports when it actually starts a job (the executor queues
  calls ahead of its workers, so submit time is not start time). A job running longer
  than ``extract_timeout_s`` is charged a transient attempt; the pool is killed (the
  only way to stop a stuck worker) and its other jobs are handed back uncharged.
- **Crashes.** A dead worker breaks the whole pool and fails every job in it, so the
  culprit is unknown. All of them are handed back uncharged and become *suspects*,
  which then run one at a time in a separate single-worker quarantine pool, where a
  crash can only be the suspect's own. CPython (seen on 3.14.4) often fails to notice
  a dead worker while another worker is busy; the dead worker's job then reported a
  start and never finishes, so the hang rule charges it, still to the right page.

Transient failures retry with backoff up to ``extract_max_attempts``, then fail.
"""

from __future__ import annotations

import logging
import multiprocessing
import sqlite3
import threading
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from newsrag.config import Settings
from newsrag.contracts.v1 import Document
from newsrag.extract.extractor import ExtractContext, ExtractFailure, extract
from newsrag.obs.events import worker_id
from newsrag.storage.blobs import BlobStore
from newsrag.storage.fsio import ProcessLock, TmpArea
from newsrag.storage.recovery import recover_writer
from newsrag.storage.state import transaction
from newsrag.storage.writer import (
    WRITER_LOCK,
    ExtractedDoc,
    ExtractionClaim,
    ExtractionFailed,
    StorageWriter,
    claim_for_extraction,
)
from newsrag.timeutil import to_db, utcnow

log = logging.getLogger(__name__)

PIPELINE_VERSION = "writer-1"


# Must be a module-level function: the pool pickles it by reference.
ExtractJob = Callable[[bytes, ExtractContext], Document | ExtractFailure]

_started_queue: Any = None  # set in each worker process by _init_worker


def _init_worker(started: Any) -> None:
    """Pool initializer: keep the start-report queue and load the models once."""
    global _started_queue
    _started_queue = started
    from newsrag.extract.extractor import detect_language

    detect_language("warm up the language model")


def _run_job(job: ExtractJob, key: str, body: bytes, ctx: ExtractContext) -> Any:
    _started_queue.put(key)
    return job(body, ctx)


def _extract_job(body: bytes, ctx: ExtractContext) -> Document | ExtractFailure:
    return extract(body, ctx)


class _Pool:
    """A process pool plus the queue its workers report job starts on."""

    def __init__(self, workers: int) -> None:
        mp = multiprocessing.get_context()
        self.started = mp.SimpleQueue()
        self.executor = ProcessPoolExecutor(
            max_workers=workers,
            mp_context=mp,
            initializer=_init_worker,
            initargs=(self.started,),
            max_tasks_per_child=1000,  # bounds memory growth from lxml/trafilatura
        )

    def submit(
        self, job: ExtractJob, key: str, body: bytes, ctx: ExtractContext
    ) -> Future[Document | ExtractFailure]:
        return self.executor.submit(_run_job, job, key, body, ctx)

    def started_keys(self) -> list[str]:
        keys = []
        while not self.started.empty():
            keys.append(self.started.get())
        return keys

    def kill(self) -> None:
        kill = getattr(self.executor, "kill_workers", None)  # Python 3.14+
        if kill is not None:
            kill()
        else:  # pragma: no cover - older Pythons
            for proc in list(getattr(self.executor, "_processes", {}).values()):
                proc.kill()
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.started.close()

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.started.close()


def _context(c: ExtractionClaim) -> ExtractContext:
    return ExtractContext(
        final_url=c.final_url or c.url,
        source_id=c.source_id,
        discovered_at=c.discovered_at,
        fetched_at=c.fetched_at,
        pipeline_version=PIPELINE_VERSION,
        content_type=c.content_type,
        feed_id=c.feed_id,
        feed_title=c.feed_title,
        feed_summary=c.feed_summary,
        feed_published_at=c.feed_published_at,
    )


@dataclass
class _InFlight:
    claim: ExtractionClaim
    started: datetime
    attempt: int
    quarantined: bool
    running_since: float | None = None  # monotonic; set when the worker reports in


class WriterLoop:
    def __init__(
        self,
        settings: Settings,
        conn: sqlite3.Connection,
        tmp: TmpArea,
        worker: str,
        job: ExtractJob = _extract_job,
    ) -> None:
        self.s = settings
        self.job = job
        self.conn = conn
        self.worker = worker
        self.blobs = BlobStore(settings.data_dir, tmp)
        self.writer = StorageWriter(conn, settings.data_dir, tmp, worker, PIPELINE_VERSION)
        self.pool = _Pool(settings.writer.extract_workers)
        self.quarantine = _Pool(1)
        self.in_flight: dict[Future[Document | ExtractFailure], _InFlight] = {}
        self.suspects: set[str] = set()  # url_ids caught in a pool crash
        self.waiting_suspects: list[tuple[ExtractionClaim, bytes, datetime, int]] = []
        self.docs: list[ExtractedDoc] = []
        self.failures: list[ExtractionFailed] = []
        self.oldest_buffered: float | None = None

    def _main_in_flight(self) -> int:
        return sum(not j.quarantined for j in self.in_flight.values())

    def _quarantine_busy(self) -> bool:
        return any(j.quarantined for j in self.in_flight.values())

    def _held(self) -> int:
        """Claims this loop is holding: running, waiting for quarantine, or buffered."""
        return (
            len(self.in_flight) + len(self.waiting_suspects) + len(self.docs) + len(self.failures)
        )

    # --- submitting ------------------------------------------------------------------

    def _extract_attempt(self, url_id: str) -> int:
        """1-based: past failed extract events for this URL (the frontier's ``attempts``
        counts fetches) plus this one."""
        row = self.conn.execute(
            "SELECT count(*) FROM stage_events WHERE stage = 'extract' AND url_id = ? "
            "AND status IN ('retry', 'error')",
            (url_id,),
        ).fetchone()
        return int(row[0]) + 1

    def _submit(self, claims: list[ExtractionClaim]) -> None:
        for c in claims:
            started = utcnow()
            attempt = self._extract_attempt(c.url_id)
            present = self.conn.execute(
                "SELECT 1 FROM blobs WHERE blob_hash = ? AND state = 'present'", (c.raw_blob_hash,)
            ).fetchone()
            if present is None:
                self._fail(c, started, attempt, "blob_missing", "raw blob expired or never stored")
                continue
            body = self.blobs.read(c.raw_blob_hash)
            if c.url_id in self.suspects:
                self.waiting_suspects.append((c, body, started, attempt))
            else:
                self._start(c, body, started, attempt, quarantined=False)
        self._start_next_suspect()

    def _start(
        self, c: ExtractionClaim, body: bytes, started: datetime, attempt: int, quarantined: bool
    ) -> None:
        try:
            fut = self._pool(quarantined).submit(self.job, c.url_id, body, _context(c))
        except BrokenProcessPool:
            # The executor saw a worker die before any of our futures reported it.
            self._handle_broken(quarantined)
            fut = self._pool(quarantined).submit(self.job, c.url_id, body, _context(c))
        self.in_flight[fut] = _InFlight(c, started, attempt, quarantined)

    def _pool(self, quarantined: bool) -> _Pool:
        return self.quarantine if quarantined else self.pool

    def _handle_broken(self, quarantined: bool) -> None:
        """A pool lost a worker. In quarantine the one running page is the culprit and is
        charged; in the main pool the culprit is unknown, so every page it was running
        is handed back uncharged and becomes a suspect."""
        if quarantined:
            for fut in [f for f, j in self.in_flight.items() if j.quarantined]:
                job = self.in_flight.pop(fut)
                self._fail(job.claim, job.started, job.attempt, "worker_crashed", "", True)
            self._replace_pool(quarantined=True)
            return
        lost = self._replace_pool(quarantined=False)
        self.suspects.update(j.claim.url_id for j in lost)
        log.warning("extractor pool crashed; %d pages moved to quarantine", len(lost))

    def _start_next_suspect(self) -> None:
        if self.waiting_suspects and not self._quarantine_busy():
            c, body, started, attempt = self.waiting_suspects.pop(0)
            self._start(c, body, started, attempt, quarantined=True)

    # --- results ---------------------------------------------------------------------

    def _fail(
        self,
        c: ExtractionClaim,
        started: datetime,
        attempt: int,
        code: str,
        detail: str,
        transient: bool = False,
    ) -> None:
        retry_at = None
        if transient and attempt < self.s.writer.extract_max_attempts:
            retry_at = utcnow() + timedelta(seconds=self.s.fetch.retry_base_s * 4 ** (attempt - 1))
        self.failures.append(
            ExtractionFailed(
                url_id=c.url_id,
                error_code=code,
                detail=detail,
                extract_enqueued_at=c.ready_at,
                extract_started_at=started,
                extract_finished_at=utcnow(),
                attempt=attempt,
                retry_at=retry_at,
            )
        )
        self._mark_buffered()

    def _mark_buffered(self) -> None:
        if self.oldest_buffered is None:
            self.oldest_buffered = time.monotonic()

    def _collect(self, timeout_s: float) -> None:
        if self.in_flight:
            done, _ = wait(list(self.in_flight), timeout=timeout_s, return_when=FIRST_COMPLETED)
        else:
            done = set()
        main_broken = False
        for fut in done:
            job = self.in_flight.get(fut)
            if job is None:  # already handled with its broken pool
                continue
            try:
                out = fut.result()
            except BrokenProcessPool:
                if job.quarantined:
                    self._handle_broken(quarantined=True)
                else:
                    main_broken = True
                continue
            del self.in_flight[fut]
            if isinstance(out, ExtractFailure):
                self._fail(job.claim, job.started, job.attempt, out.error_code, out.detail)
                continue
            self.suspects.discard(job.claim.url_id)
            self.docs.append(
                ExtractedDoc(
                    doc=out,
                    url_id=job.claim.url_id,
                    extract_enqueued_at=job.claim.ready_at,
                    extract_started_at=job.started,
                    extract_finished_at=utcnow(),
                    attempt=job.attempt,
                )
            )
            self._mark_buffered()
        if main_broken:
            self._handle_broken(quarantined=False)
        self._check_timeouts()
        self._start_next_suspect()

    def _note_starts(self) -> None:
        now = time.monotonic()
        by_key = {j.claim.url_id: j for j in self.in_flight.values()}
        for pool in (self.pool, self.quarantine):
            for key in pool.started_keys():
                if (job := by_key.get(key)) is not None and job.running_since is None:
                    job.running_since = now

    def _check_timeouts(self) -> None:
        self._note_starts()
        limit = self.s.writer.extract_timeout_s
        now = time.monotonic()
        for quarantined in (False, True):
            stuck = [
                f
                for f, j in self.in_flight.items()
                if j.quarantined is quarantined
                and j.running_since is not None
                and now - j.running_since > limit
            ]
            if not stuck:
                continue
            for fut in stuck:
                job = self.in_flight.pop(fut)
                log.warning("extraction of %s exceeded %ss", job.claim.url, limit)
                self._fail(job.claim, job.started, job.attempt, "extract_timeout", "", True)
            self._replace_pool(quarantined)

    def _replace_pool(self, quarantined: bool) -> list[_InFlight]:
        """Kill a pool and hand back, uncharged, every job it was still running."""
        if quarantined:
            self.quarantine.kill()
            self.quarantine = _Pool(1)
        else:
            self.pool.kill()
            self.pool = _Pool(self.s.writer.extract_workers)
        lost_futs = [f for f, j in self.in_flight.items() if j.quarantined is quarantined]
        lost = [self.in_flight.pop(f) for f in lost_futs]
        if lost:
            now = to_db(utcnow())
            with transaction(self.conn):
                self.conn.executemany(
                    "UPDATE frontier SET state = 'fetched', lease_owner = NULL, "
                    "lease_expires_at = NULL, updated_at = ? WHERE url_id = ? "
                    "AND state = 'extracting'",
                    [(now, j.claim.url_id) for j in lost],
                )
        return lost

    # --- flushing and the loop -------------------------------------------------------

    def _should_flush(self) -> bool:
        if not self.docs and not self.failures:
            return False
        if len(self.docs) + len(self.failures) >= self.s.writer.flush_rows:
            return True
        age = time.monotonic() - (self.oldest_buffered or time.monotonic())
        return age >= self.s.writer.flush_seconds

    def flush(self) -> None:
        if not self.docs and not self.failures:
            return
        result = self.writer.commit(self.docs, self.failures)
        log.info(
            "flushed %d new revisions, %d unchanged, %d failures (part %s)",
            len(result.new_revisions),
            len(result.unchanged),
            len(self.failures),
            result.file_id,
        )
        self.docs, self.failures, self.oldest_buffered = [], [], None

    def step(self, wait_s: float = 0.5) -> int:
        """Claim what fits, collect finished extractions, flush if due. Returns claims."""
        room = min(
            self.s.writer.extract_workers - self._main_in_flight(),
            self.s.writer.flush_rows - self._held(),
        )
        claims = (
            claim_for_extraction(self.conn, self.worker, room, self.s.fetch.lease_s)
            if room > 0
            else []
        )
        self._submit(claims)
        self._collect(wait_s if self.in_flight else 0)
        if self._should_flush():
            self.flush()
        return len(claims)

    def busy(self) -> bool:
        return bool(self.in_flight or self.waiting_suspects)

    def run(self, stop: threading.Event, idle_wait_s: float = 1.0) -> None:
        while not stop.is_set():
            claimed = self.step()
            if not claimed and not self.busy():
                stop.wait(idle_wait_s)
        self.drain()

    def drain(self) -> None:
        """Finish in-flight and quarantined extractions and flush everything buffered."""
        while self.busy():
            self._collect(0.5)
        self.flush()

    def close(self) -> None:
        self.pool.shutdown()
        self.quarantine.shutdown()

    def _ready_rows(self) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM frontier WHERE state = 'fetched' AND next_attempt_at <= ? LIMIT 1",
            (to_db(utcnow()),),
        ).fetchone()
        return row is not None

    def run_until_idle(self) -> None:
        """Process every ready ``fetched`` row, then flush. For tests and one-off runs.

        Checks the frontier rather than the last step's claims: a pool crash hands
        rows back after that step has already claimed."""
        while self.step(wait_s=0.05) or self.busy() or self._ready_rows():
            pass
        self.drain()


def run_writer(settings: Settings, conn: sqlite3.Connection, stop: threading.Event) -> None:
    with ProcessLock(settings.locks_dir / WRITER_LOCK):
        report = recover_writer(conn, settings.data_dir, utcnow())
        log.info("recovery: %s", report)
        with TmpArea(settings.tmp_dir, "writer") as tmp:
            loop = WriterLoop(settings, conn, tmp, worker_id("writer"))
            try:
                loop.run(stop)
            finally:
                loop.close()
