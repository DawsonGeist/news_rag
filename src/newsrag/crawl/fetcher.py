"""Component 3, article fetcher: frontier ``pending`` -> ``fetching`` -> ``fetched``.

Owner of the ``hosts`` table and of those frontier transitions.

**One request in flight per host, across processes.** Claiming a URL reserves its
host: ``hosts.next_allowed_at`` is pushed to ``now + reservation`` inside the claim
transaction, where the reservation outlasts the longest possible fetch (robots.txt
plus the page, each bounded by the request timeout). No other claim, in this
process or another fetcher, can pick that host until the fetch finishes and sets
``next_allowed_at = finished + crawl_delay``. A crashed fetcher's reservation simply
lapses. No extra columns or locks are needed.

Redirects are followed by hand so that robots.txt is checked on every hop.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlsplit

import httpx

from newsrag import ids
from newsrag.config import ROBOTS_AGENT, Settings
from newsrag.contracts.v1 import Stage, StageStatus
from newsrag.crawl import politeness as pol
from newsrag.obs.events import new_event, record, worker_id
from newsrag.storage.blobs import BlobStore, StagedBlob
from newsrag.storage.fsio import TmpArea, sweep_tmp
from newsrag.storage.recovery import reclaim_expired_leases
from newsrag.storage.state import transaction
from newsrag.timeutil import from_db, to_db, utcnow

log = logging.getLogger(__name__)

PIPELINE_VERSION = "fetcher-1"
ROBOTS_MAX_BYTES = 500_000  # RFC 9309 asks crawlers to parse at least 500 KiB


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


@dataclass(frozen=True)
class _HttpError:
    status: int
    retry_after_s: float | None


def host_of(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def _reservation_s(settings: Settings) -> float:
    # robots.txt and the page each get one timeout; slack covers commit time.
    return 2 * settings.fetch.request_timeout_s + 30


def claim_fetch_batch(
    conn: sqlite3.Connection, settings: Settings, worker: str, limit: int, now: datetime
) -> list[FetchClaim]:
    """Lease up to ``limit`` ready URLs, at most one per host, reserving each host."""
    if limit <= 0:
        return []
    n = to_db(now)
    lease = to_db(now + timedelta(seconds=settings.fetch.lease_s))
    reserved = to_db(now + timedelta(seconds=_reservation_s(settings)))
    with transaction(conn):
        # One candidate per host; hosts that are reserved or backing off are excluded.
        rows = conn.execute(
            "SELECT url_id, url, host, feed_id, attempts, ready_at FROM ("
            "  SELECT f.*, row_number() OVER ("
            "    PARTITION BY f.host ORDER BY f.priority DESC, f.next_attempt_at) AS rn"
            "  FROM frontier f LEFT JOIN hosts h ON h.host = f.host"
            "  WHERE f.state = 'pending' AND f.next_attempt_at <= ?"
            "    AND (h.next_allowed_at IS NULL OR h.next_allowed_at <= ?)"
            "    AND (h.backoff_until IS NULL OR h.backoff_until <= ?)"
            ") WHERE rn = 1 ORDER BY priority DESC, next_attempt_at LIMIT ?",
            (n, n, n, limit),
        ).fetchall()
        for r in rows:
            conn.execute(
                "INSERT INTO hosts (host, crawl_delay_s, next_allowed_at) VALUES (?, ?, ?) "
                "ON CONFLICT (host) DO UPDATE SET next_allowed_at = excluded.next_allowed_at",
                (r["host"], settings.fetch.default_crawl_delay_s, reserved),
            )
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
        self.client = client
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
            async with asyncio.timeout(_reservation_s(self.s) - 30):
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
        url = claim.url
        for _ in range(self.s.fetch.max_redirects + 1):
            verdict = await self.robots_check(url)
            if verdict == "disallow":
                return FetchOutcome("skipped", final_url=url, skip_reason="robots")
            if verdict == "unavailable":
                out = self._transient(claim, "robots_unavailable")
                out.host_backoff_s = out.delay_s  # don't re-ask for every URL on the host
                return out
            try:
                result = await self._get(url)
            except httpx.TimeoutException:
                return self._transient(claim, "timeout", network=True)
            except httpx.TransportError as e:
                return self._transient(claim, f"network:{type(e).__name__}", network=True)
            if isinstance(result, str):  # a redirect target
                try:
                    url = ids.normalize_url(result)
                except ids.InvalidURL:
                    return FetchOutcome("failed", final_url=url, error_code="bad_redirect")
                continue
            if isinstance(result, _HttpError):
                d = pol.decide_http_error(
                    result.status,
                    claim.attempts,
                    self.s.fetch.max_attempts,
                    result.retry_after_s,
                    self.s.fetch.retry_base_s,
                    self.s.fetch.max_retry_after_s,
                    self.rng,
                )
                return FetchOutcome(
                    "retry" if d.retry else "failed",
                    final_url=url,
                    http_status=result.status,
                    error_code=d.error_code,
                    delay_s=d.delay_s,
                    host_backoff_s=d.host_backoff_s,
                    network_error=result.status >= 500,
                )
            result.final_url = url
            return result
        return FetchOutcome("failed", final_url=url, error_code="too_many_redirects")

    async def _get(self, url: str) -> FetchOutcome | _HttpError | str:
        """One GET: an outcome, an HTTP error to classify, or the absolute redirect target."""
        max_bytes = self.s.fetch.max_body_bytes
        async with self.client.stream("GET", url) as r:
            if r.status_code in pol.REDIRECT_STATUSES and "location" in r.headers:
                return urljoin(url, r.headers["location"])
            if not 200 <= r.status_code < 300:
                return _HttpError(
                    r.status_code, pol.parse_retry_after(r.headers.get("retry-after"), utcnow())
                )
            ctype = r.headers.get("content-type")
            base = FetchOutcome("skipped", http_status=r.status_code, content_type=ctype)
            if not pol.is_html(ctype):
                base.skip_reason = "content_type"
                return base
            declared = r.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > max_bytes:
                base.skip_reason = "too_large"
                return base
            body = bytearray()
            # Decoded bytes are counted, so a small gzip bomb is caught too.
            async for chunk in r.aiter_bytes():
                body += chunk
                if len(body) > max_bytes:
                    base.skip_reason = "too_large"
                    return base
            base.kind = "fetched"
            base.body = bytes(body)
            return base

    async def robots_check(self, url: str) -> pol.RobotsVerdict:
        host = host_of(url)
        row = self.conn.execute(
            "SELECT robots_txt, robots_status, robots_fetched_at FROM hosts WHERE host = ?",
            (host,),
        ).fetchone()
        now = utcnow()
        if row is not None and row["robots_fetched_at"] is not None:
            definitive = row["robots_status"] is not None and 200 <= row["robots_status"] < 500
            # An unavailable robots.txt is re-asked at the next retry, not cached for a day.
            ttl = self.s.fetch.robots_ttl_s if definitive else self.s.fetch.retry_base_s
            if now - from_db(row["robots_fetched_at"]) < timedelta(seconds=ttl):
                return pol.robots_verdict(
                    row["robots_status"], row["robots_txt"], url, ROBOTS_AGENT
                )
        status, text = await self._fetch_robots(url)
        delay = pol.effective_crawl_delay(
            pol.robots_crawl_delay(status, text, ROBOTS_AGENT),
            self.s.fetch.default_crawl_delay_s,
            self.s.fetch.max_crawl_delay_s,
        )
        with transaction(self.conn):
            self.conn.execute(
                "INSERT INTO hosts (host, robots_txt, robots_status, robots_fetched_at, "
                "crawl_delay_s) VALUES (?, ?, ?, ?, ?) ON CONFLICT (host) DO UPDATE SET "
                "robots_txt = excluded.robots_txt, robots_status = excluded.robots_status, "
                "robots_fetched_at = excluded.robots_fetched_at, "
                "crawl_delay_s = excluded.crawl_delay_s",
                (host, text, status, to_db(now), delay),
            )
        return pol.robots_verdict(status, text, url, ROBOTS_AGENT)

    async def _fetch_robots(self, url: str) -> tuple[int, str | None]:
        parts = urlsplit(url)
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        try:
            r = await self.client.get(robots_url, follow_redirects=True)
        except httpx.HTTPError as e:
            log.info("robots.txt unreachable for %s: %s %s", parts.netloc, type(e).__name__, e)
            return pol.ROBOTS_NETWORK_ERROR, None
        text = r.content[:ROBOTS_MAX_BYTES].decode("utf-8", errors="replace")
        return r.status_code, text if 200 <= r.status_code < 300 else None

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
                self._release_host(claim.host, now, out)
                return
            if staged is not None:
                expires = now + timedelta(days=self.s.retention.raw_days)
                self.blobs.commit(self.conn, staged, now, expires)
            self._release_host(claim.host, now, out)
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

    def _release_host(self, host: str, now: datetime, out: FetchOutcome) -> None:
        """End the reservation: the host is next allowed one crawl delay from now."""
        row = self.conn.execute(
            "SELECT crawl_delay_s FROM hosts WHERE host = ?", (host,)
        ).fetchone()
        delay = row["crawl_delay_s"] if row else self.s.fetch.default_crawl_delay_s
        backoff = (
            to_db(now + timedelta(seconds=out.host_backoff_s))
            if out.host_backoff_s is not None
            else None
        )
        self.conn.execute(
            "UPDATE hosts SET next_allowed_at = ?, "
            "backoff_until = coalesce(?, backoff_until), "
            "consecutive_errors = CASE WHEN ? THEN consecutive_errors + 1 ELSE 0 END "
            "WHERE host = ?",
            (to_db(now + timedelta(seconds=delay)), backoff, out.network_error, host),
        )


def make_client(settings: Settings) -> httpx.AsyncClient:
    """The one HTTP client configuration every fetch uses. Redirects are handled by
    ``Fetcher.fetch`` so robots.txt is checked on every hop."""
    return httpx.AsyncClient(
        http2=True,
        follow_redirects=False,
        timeout=httpx.Timeout(settings.fetch.request_timeout_s),
        headers={"User-Agent": settings.user_agent, "From": settings.contact_email},
        limits=httpx.Limits(
            max_connections=settings.fetch.global_concurrency,
            max_keepalive_connections=min(32, settings.fetch.global_concurrency),
        ),
    )


async def run_fetcher(settings: Settings, conn: sqlite3.Connection, stop: asyncio.Event) -> None:
    sweep_tmp(settings.tmp_dir)
    with TmpArea(settings.tmp_dir, "fetcher") as tmp:
        async with make_client(settings) as client:
            await Fetcher(settings, conn, tmp, client, worker_id("fetcher")).run(stop)
