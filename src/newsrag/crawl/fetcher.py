"""Component 3, article fetcher: frontier ``pending`` -> ``fetching`` -> ``fetched``.

Owner of those frontier transitions. Politeness (host reservation, robots.txt on
every redirect hop, body limits) lives in ``crawl.http`` and is shared with the poller.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx

from newsrag.config import Settings
from newsrag.contracts.v1 import Stage, StageStatus
from newsrag.crawl import politeness as pol
from newsrag.crawl.http import (
    HOST_READY_SQL,
    GetResult,
    PoliteClient,
    make_client,
    release_host,
    reserve_host,
)
from newsrag.obs.events import new_event, record, worker_id
from newsrag.storage.blobs import BlobStore, StagedBlob
from newsrag.storage.fsio import TmpArea, sweep_tmp
from newsrag.storage.recovery import reclaim_expired_leases
from newsrag.storage.state import transaction
from newsrag.timeutil import from_db, to_db, utcnow

log = logging.getLogger(__name__)

PIPELINE_VERSION = "fetcher-1"


@dataclass(frozen=True)
class FetchClaim:
    url_id: str
    url: str
    host: str
    feed_id: str | None
    attempts: int  # including this attempt
    ready_at: datetime


@dataclass
class FetchOutcome:
    """``kind``: fetched | retry | failed | skipped."""

    kind: str
    final_url: str | None = None
    http_status: int | None = None
    content_type: str | None = None
    body: bytes | None = None
    error_code: str | None = None
    skip_reason: str | None = None
    delay_s: float = 0.0
    host_backoff_s: float | None = None
    network_error: bool = False
    blocked: bool = False  # HTTP 403: the host refused us


def claim_fetch_batch(
    conn: sqlite3.Connection, settings: Settings, worker: str, limit: int, now: datetime
) -> list[FetchClaim]:
    """Lease up to ``limit`` ready URLs, at most one per host, reserving each host."""
    if limit <= 0:
        return []
    n = to_db(now)
    lease = to_db(now + timedelta(seconds=settings.fetch.lease_s))
    with transaction(conn):
        # One candidate per host; hosts that are reserved or backing off are excluded.
        rows = conn.execute(
            "SELECT url_id, url, host, feed_id, attempts, ready_at FROM ("
            "  SELECT f.*, row_number() OVER ("
            "    PARTITION BY f.host ORDER BY f.priority DESC, f.next_attempt_at) AS rn"
            "  FROM frontier f LEFT JOIN hosts h ON h.host = f.host"
            f"  WHERE f.state = 'pending' AND f.next_attempt_at <= :now AND {HOST_READY_SQL}"
            ") WHERE rn = 1 ORDER BY priority DESC, next_attempt_at LIMIT :limit",
            {"now": n, "limit": limit},
        ).fetchall()
        for r in rows:
            reserve_host(conn, settings, r["host"], now)
            conn.execute(
                "UPDATE frontier SET state = 'fetching', lease_owner = ?, lease_expires_at = ?, "
                "attempts = attempts + 1, updated_at = ? WHERE url_id = ?",
                (worker, lease, n, r["url_id"]),
            )
    return [
        FetchClaim(
            r["url_id"],
            r["url"],
            r["host"],
            r["feed_id"],
            r["attempts"] + 1,
            from_db(r["ready_at"]),
        )
        for r in rows
    ]


class Fetcher:
    def __init__(
        self,
        settings: Settings,
        conn: sqlite3.Connection,
        tmp: TmpArea,
        client: httpx.AsyncClient,
        worker: str,
        rng: random.Random | None = None,
    ) -> None:
        self.s = settings
        self.conn = conn
        self.blobs = BlobStore(settings.data_dir, tmp)
        self.http = PoliteClient(settings, conn, client)
        self.worker = worker
        self.rng = rng or random.Random()

    # --- main loop ------------------------------------------------------------------

    async def run(self, stop: asyncio.Event, idle_poll_s: float = 1.0) -> None:
        """Claim and fetch until ``stop`` is set, then drain in-flight fetches."""
        reclaim_expired_leases(self.conn, "fetching", utcnow())
        tasks: set[asyncio.Task[None]] = set()
        last_reclaim = utcnow()
        while not stop.is_set():
            now = utcnow()
            if now - last_reclaim > timedelta(seconds=60):
                # Another fetcher may have died holding leases.
                reclaim_expired_leases(self.conn, "fetching", now)
                last_reclaim = now
            free = self.s.fetch.global_concurrency - len(tasks)
            claims = claim_fetch_batch(
                self.conn, self.s, self.worker, min(free, self.s.fetch.claim_batch), now
            )
            for c in claims:
                tasks.add(asyncio.create_task(self.process(c)))
            if tasks:
                done, tasks = await asyncio.wait(
                    tasks, timeout=idle_poll_s, return_when=asyncio.FIRST_COMPLETED
                )
                for t in done:
                    t.result()  # a bug in process() should crash loudly, not vanish
            else:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), idle_poll_s)
        if tasks:
            await asyncio.gather(*tasks)

    async def run_until_idle(self, idle_poll_s: float = 0.05) -> None:
        """Fetch until no frontier row is pending or fetching. For tests and one-off runs."""
        stop = asyncio.Event()

        async def watch() -> None:
            while True:
                await asyncio.sleep(idle_poll_s)
                busy = self.conn.execute(
                    "SELECT count(*) FROM frontier WHERE state IN ('pending', 'fetching')"
                ).fetchone()[0]
                if not busy:
                    stop.set()
                    return

        watcher = asyncio.create_task(watch())
        await self.run(stop, idle_poll_s)
        await watcher

    async def process(self, claim: FetchClaim) -> None:
        started = utcnow()
        try:
            async with asyncio.timeout(2 * self.s.fetch.request_timeout_s):
                outcome = await self.fetch(claim)
        except TimeoutError:
            outcome = self._transient(claim, "timeout", network=True)
        staged = None
        if outcome.kind == "fetched" and outcome.body is not None:
            staged = await asyncio.to_thread(self.blobs.stage, outcome.body, outcome.content_type)
        self.commit(claim, outcome, staged, started, utcnow())

    # --- one URL ----------------------------------------------------------------------

    def _transient(self, claim: FetchClaim, code: str, network: bool = False) -> FetchOutcome:
        d = pol.decide_transient(
            code, claim.attempts, self.s.fetch.max_attempts, self.s.fetch.retry_base_s, self.rng
        )
        return FetchOutcome(
            "retry" if d.retry else "failed",
            error_code=d.error_code,
            delay_s=d.delay_s,
            network_error=network,
        )

    async def fetch(self, claim: FetchClaim) -> FetchOutcome:
        return self.classify(claim, await self.http.get(claim.url))

    def classify(self, claim: FetchClaim, res: GetResult) -> FetchOutcome:
        """Map an HTTP result to the article fetcher's retry policy."""
        url = res.final_url
        match res.kind:
            case "ok":
                return FetchOutcome(
                    "fetched",
                    final_url=url,
                    http_status=res.status,
                    content_type=res.content_type,
                    body=res.body,
                )
            case "disallowed":
                return FetchOutcome("skipped", final_url=url, skip_reason="robots")
            case "content_type" | "too_large":
                return FetchOutcome(
                    "skipped",
                    final_url=url,
                    http_status=res.status,
                    content_type=res.content_type,
                    skip_reason=res.kind,
                )
            case "robots_unavailable":
                out = self._transient(claim, "robots_unavailable")
                out.host_backoff_s = out.delay_s  # don't re-ask for every URL on the host
                return out
            case "timeout" | "network":
                return self._transient(claim, res.error or res.kind, network=True)
            case "bad_redirect" | "too_many_redirects":
                return FetchOutcome("failed", final_url=url, error_code=res.kind)
            case "http_error" | "not_modified":
                status = res.status or 0
                d = pol.decide_http_error(
                    status,
                    claim.attempts,
                    self.s.fetch.max_attempts,
                    res.retry_after_s,
                    self.s.fetch.retry_base_s,
                    self.s.fetch.max_retry_after_s,
                    self.rng,
                )
                return FetchOutcome(
                    "retry" if d.retry else "failed",
                    final_url=url,
                    http_status=status,
                    error_code=d.error_code,
                    delay_s=d.delay_s,
                    host_backoff_s=d.host_backoff_s,
                    network_error=status >= 500,
                    blocked=status == 403,
                )

    # --- commit -------------------------------------------------------------------------

    def commit(
        self,
        claim: FetchClaim,
        out: FetchOutcome,
        staged: StagedBlob | None,
        started: datetime,
        finished: datetime,
    ) -> None:
        """One transaction: blob manifest, frontier row, host politeness, stage event."""
        now = finished
        n = to_db(now)
        state = {"fetched": "fetched", "retry": "pending"}.get(out.kind, out.kind)
        next_attempt = now + timedelta(seconds=out.delay_s) if out.kind == "retry" else now
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE frontier SET state = ?, lease_owner = NULL, lease_expires_at = NULL, "
                "final_url = ?, http_status = ?, content_type = ?, raw_blob_hash = ?, "
                "fetched_at = coalesce(?, fetched_at), skip_reason = ?, last_error = ?, "
                "next_attempt_at = ?, ready_at = ?, updated_at = ? "
                "WHERE url_id = ? AND state = 'fetching' AND lease_owner = ?",
                (
                    state,
                    out.final_url,
                    out.http_status,
                    out.content_type,
                    staged.blob_hash if staged else None,
                    n if out.kind == "fetched" else None,
                    out.skip_reason,
                    out.error_code,
                    to_db(next_attempt),
                    to_db(next_attempt) if out.kind == "retry" else n,
                    n,
                    claim.url_id,
                    self.worker,
                ),
            )
            if cur.rowcount == 0:
                # Our lease lapsed and someone else owns the row now: record nothing.
                log.warning("lost lease on %s; dropping result", claim.url_id)
                if staged is not None:
                    staged.tmp_path.unlink(missing_ok=True)
                self._release(claim.host, now, out)
                return
            if staged is not None:
                expires = now + timedelta(days=self.s.retention.raw_days)
                self.blobs.commit(self.conn, staged, now, expires)
            self._release(claim.host, now, out)
            status = {
                "fetched": StageStatus.OK,
                "retry": StageStatus.RETRY,
                "failed": StageStatus.ERROR,
                "skipped": StageStatus.SKIPPED,
            }[out.kind]
            record(
                self.conn,
                [
                    new_event(
                        Stage.FETCH,
                        status,
                        worker=self.worker,
                        started_at=started,
                        finished_at=finished,
                        enqueued_at=min(claim.ready_at, started),
                        url_id=claim.url_id,
                        feed_id=claim.feed_id,
                        attempt=claim.attempts,
                        error_code=out.error_code,
                        detail=out.skip_reason,
                        pipeline_version=PIPELINE_VERSION,
                    )
                ],
            )

    def _release(self, host: str, now: datetime, out: FetchOutcome) -> None:
        release_host(
            self.conn,
            self.s,
            host,
            now,
            network_error=out.network_error,
            blocked=out.blocked,
            backoff_s=out.host_backoff_s,
        )


async def run_fetcher(settings: Settings, conn: sqlite3.Connection, stop: asyncio.Event) -> None:
    sweep_tmp(settings.tmp_dir)
    with TmpArea(settings.tmp_dir, "fetcher") as tmp:
        async with make_client(settings) as client:
            await Fetcher(settings, conn, tmp, client, worker_id("fetcher")).run(stop)
