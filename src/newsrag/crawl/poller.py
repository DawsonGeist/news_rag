"""Component 2, feed poller: active feeds -> new ``pending`` frontier rows.

Owner of the poll columns of ``feeds`` (etag, interval, failures, ``paused``), of
``feed_polls`` and ``url_sightings``, and of inserting frontier rows.

- **Conditional GET** with the stored ETag and Last-Modified; a 304 is a successful
  poll with no new items.
- **Adaptive interval:** x0.7 after a poll that found new items, x1.5 otherwise,
  clamped to [min, max]; the next poll time gets +-10% jitter so feeds don't align.
- **Politeness** is shared with the fetcher (``crawl.http``): robots.txt, one request
  per host. Claiming a feed reserves its host and pushes the feed's ``next_poll_at``
  past the longest possible poll, so a crashed poller's claim simply lapses.
- **Revision policy:** a stored URL is re-queued (low priority) only when the feed
  that first listed it reports a newer ``updated`` time or a changed title.
  Other feeds' titles are ignored: section feeds often abbreviate headlines, and
  comparing across feeds would re-fetch the same article forever.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import feedparser
import httpx

from newsrag import ids
from newsrag.config import Settings
from newsrag.contracts.v1 import Stage, StageEvent, StageStatus
from newsrag.crawl import politeness as pol
from newsrag.crawl.http import (
    GetResult,
    PoliteClient,
    accept_any,
    host_of,
    make_client,
    release_host,
    reservation_s,
    reserve_host,
)
from newsrag.obs.events import new_event, record, worker_id
from newsrag.storage.state import transaction
from newsrag.timeutil import from_db, to_db, utcnow

log = logging.getLogger(__name__)

PIPELINE_VERSION = "poller-1"
REFETCH_PRIORITY = -1  # below newly discovered URLs (priority 0)
MAX_SUMMARY_CHARS = 20_000
NEW_ITEMS_FACTOR = 0.7
NO_NEW_ITEMS_FACTOR = 1.5
JITTER = 0.1
FUTURE_SLACK = timedelta(days=1)


# --- pure pieces --------------------------------------------------------------------------


@dataclass(frozen=True)
class FeedItem:
    url: str  # normalized, absolute
    title: str | None
    summary: str | None
    published_at: datetime | None
    updated_at: datetime | None


@dataclass
class ParsedFeed:
    format: str
    items: list[FeedItem] = field(default_factory=list)
    invalid_links: int = 0
    audio_items: int = 0


def _struct_to_utc(t: time.struct_time | None, now: datetime) -> datetime | None:
    """feedparser normalizes dates to UTC struct_times. Dates far in the future are
    bogus (timezone mistakes, scheduled posts) and dropped."""
    if t is None:
        return None
    try:
        dt = datetime(*t[:6], tzinfo=UTC)
    except (TypeError, ValueError):
        return None
    return None if dt > now + FUTURE_SLACK else dt


def _item_link(entry: Any, feed_url: str) -> str | None:
    # FeedBurner and similar proxies put the real article URL in origlink.
    link = entry.get("feedburner_origlink") or entry.get("link")
    if not link:
        for candidate in entry.get("links", []):
            if candidate.get("rel", "alternate") == "alternate" and candidate.get("href"):
                link = candidate["href"]
                break
    return urljoin(feed_url, link) if link else None


def parse_feed(body: bytes, feed_url: str, content_type: str | None, now: datetime) -> ParsedFeed:
    headers = {"content-type": content_type} if content_type else {}
    parsed = feedparser.parse(body, response_headers=headers)
    version = str(parsed.get("version") or "")
    # An HTML page served without a content type parses "cleanly" as nothing: no
    # entries and no feed version. An empty but valid feed still has a version.
    if not parsed.entries and (parsed.get("bozo") or not version):
        raise ValueError(f"unparseable feed: {parsed.get('bozo_exception')!r}"[:200])
    fmt = (
        "atom" if version.startswith("atom") else "rss" if version.startswith("rss") else "unknown"
    )
    out = ParsedFeed(fmt)
    for e in parsed.entries:
        link = _item_link(e, feed_url)
        try:
            url = ids.normalize_url(link) if link else None
        except ids.InvalidURL:
            url = None
        if url is None:
            out.invalid_links += 1
            continue
        # Counted over kept items only, so audio_items <= len(items) (a ratio of them).
        if any(str(enc.get("type", "")).startswith("audio/") for enc in e.get("enclosures", [])):
            out.audio_items += 1
        summary = e.get("summary")
        # dict.get bypasses FeedParserDict's key aliasing, which silently answers
        # updated_parsed with the published date. An entry with only <updated> (allowed
        # in Atom) uses it as the publication time, on purpose.
        published = _struct_to_utc(dict.get(e, "published_parsed"), now)
        updated = _struct_to_utc(dict.get(e, "updated_parsed"), now)
        out.items.append(
            FeedItem(
                url=url,
                title=(e.get("title") or "").strip() or None,
                summary=summary[:MAX_SUMMARY_CHARS] if summary else None,
                published_at=published or updated,
                updated_at=updated,
            )
        )
    return out


def next_interval_s(current_s: int, found_new: bool, settings: Settings) -> int:
    factor = NEW_ITEMS_FACTOR if found_new else NO_NEW_ITEMS_FACTOR
    p = settings.poll
    return int(min(max(current_s * factor, p.min_interval_s), p.max_interval_s))


def jittered(seconds: float, rng: random.Random) -> timedelta:
    return timedelta(seconds=seconds * rng.uniform(1 - JITTER, 1 + JITTER))


# --- claiming -----------------------------------------------------------------------------


@dataclass(frozen=True)
class FeedClaim:
    feed_id: str
    feed_url: str
    source_id: str
    host: str
    poll_interval_s: int
    etag: str | None
    last_modified: str | None
    consecutive_failures: int


def claim_due_feeds(
    conn: sqlite3.Connection, settings: Settings, limit: int, now: datetime
) -> list[FeedClaim]:
    """Claim up to ``limit`` due active feeds, at most one per host, reserving each host.

    ``feeds`` has no host column, so hosts are matched in Python over a window of the
    most overdue feeds.
    """
    if limit <= 0:
        return []
    n = to_db(now)
    claims: list[FeedClaim] = []
    with transaction(conn):
        rows = conn.execute(
            "SELECT feed_id, feed_url, source_id, poll_interval_s, etag, last_modified, "
            "consecutive_failures FROM feeds WHERE status = 'active' "
            "AND (next_poll_at IS NULL OR next_poll_at <= ?) "
            "ORDER BY next_poll_at IS NOT NULL, next_poll_at LIMIT ?",
            (n, limit * 10),
        ).fetchall()
        busy_hosts = {
            r[0]
            for r in conn.execute(
                "SELECT host FROM hosts WHERE next_allowed_at > ? OR backoff_until > ?", (n, n)
            )
        }
        lease_until = to_db(now + timedelta(seconds=reservation_s(settings)))
        for r in rows:
            host = host_of(r["feed_url"])
            if host in busy_hosts:
                continue
            busy_hosts.add(host)
            reserve_host(conn, settings, host, now)
            conn.execute(
                "UPDATE feeds SET next_poll_at = ? WHERE feed_id = ?", (lease_until, r["feed_id"])
            )
            claims.append(
                FeedClaim(
                    r["feed_id"],
                    r["feed_url"],
                    r["source_id"],
                    host,
                    r["poll_interval_s"],
                    r["etag"],
                    r["last_modified"],
                    r["consecutive_failures"],
                )
            )
            if len(claims) >= limit:
                break
    return claims


# --- the poller ---------------------------------------------------------------------------


@dataclass
class PollOutcome:
    ok: bool
    http_status: int | None = None
    not_modified: bool = False
    feed: ParsedFeed | None = None
    etag: str | None = None
    last_modified: str | None = None
    error_code: str | None = None
    retry_after_s: float | None = None
    host_backoff_s: float | None = None
    network_error: bool = False


@dataclass
class PollStats:
    items_seen: int = 0
    items_new: int = 0
    requeued: int = 0


class Poller:
    def __init__(
        self,
        settings: Settings,
        conn: sqlite3.Connection,
        client: httpx.AsyncClient,
        worker: str,
        rng: random.Random | None = None,
    ) -> None:
        self.s = settings
        self.conn = conn
        self.http = PoliteClient(settings, conn, client)
        self.worker = worker
        self.rng = rng or random.Random()

    async def run(self, stop: asyncio.Event, idle_poll_s: float = 1.0) -> None:
        tasks: set[asyncio.Task[None]] = set()
        while not stop.is_set():
            free = self.s.poll.concurrency - len(tasks)
            for c in claim_due_feeds(self.conn, self.s, free, utcnow()):
                tasks.add(asyncio.create_task(self.process(c)))
            if tasks:
                done, tasks = await asyncio.wait(
                    tasks, timeout=idle_poll_s, return_when=asyncio.FIRST_COMPLETED
                )
                for t in done:
                    t.result()
            else:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), idle_poll_s)
        if tasks:
            await asyncio.gather(*tasks)

    async def poll_due_once(self) -> int:
        """Poll every currently due feed once (waiting for host politeness). For tests and
        one-off runs. Returns the number of polls."""
        polled = 0
        while True:
            due = self.conn.execute(
                "SELECT count(*) FROM feeds WHERE status = 'active' AND "
                "(next_poll_at IS NULL OR next_poll_at <= ?)",
                (to_db(utcnow()),),
            ).fetchone()[0]
            if not due:
                return polled
            claims = claim_due_feeds(self.conn, self.s, self.s.poll.concurrency, utcnow())
            if not claims:
                await asyncio.sleep(0.05)  # hosts still in their crawl delay
                continue
            await asyncio.gather(*(self.process(c) for c in claims))
            polled += len(claims)

    async def process(self, claim: FeedClaim) -> None:
        started = utcnow()
        try:
            async with asyncio.timeout(2 * self.s.fetch.request_timeout_s):
                outcome = await self.poll(claim)
        except TimeoutError:
            outcome = PollOutcome(False, error_code="timeout", network_error=True)
        self.commit(claim, outcome, started, utcnow())

    async def poll(self, claim: FeedClaim) -> PollOutcome:
        headers = {}
        if claim.etag:
            headers["If-None-Match"] = claim.etag
        if claim.last_modified:
            headers["If-Modified-Since"] = claim.last_modified
        res = await self.http.get(claim.feed_url, headers=headers, accept=accept_any)
        return self.classify(claim, res)

    def classify(self, claim: FeedClaim, res: GetResult) -> PollOutcome:
        if res.kind == "not_modified":
            return PollOutcome(True, http_status=304, not_modified=True)
        if res.kind == "ok" and res.body is not None:
            try:
                feed = parse_feed(res.body, res.final_url, res.content_type, utcnow())
            except ValueError as e:
                log.info("feed %s: %s", claim.feed_url, e)
                return PollOutcome(False, http_status=res.status, error_code="unparseable")
            return PollOutcome(
                True,
                http_status=res.status,
                feed=feed,
                etag=res.etag,
                last_modified=res.last_modified,
            )
        out = PollOutcome(False, http_status=res.status, error_code=res.error or res.kind)
        if res.kind == "http_error" and res.status is not None:
            out.error_code = f"http_{res.status}"
            d = pol.decide_http_error(
                res.status,
                claim.consecutive_failures + 1,
                10**6,  # the poller never gives up on a retry; it pauses the feed instead
                res.retry_after_s,
                self.s.fetch.retry_base_s,
                self.s.fetch.max_retry_after_s,
                self.rng,
            )
            out.host_backoff_s = d.host_backoff_s
            out.retry_after_s = d.host_backoff_s
            out.network_error = res.status >= 500
        elif res.kind in ("network", "timeout"):
            out.network_error = True
        elif res.kind == "robots_unavailable":
            out.host_backoff_s = self.s.fetch.retry_base_s
        return out

    def commit(
        self, claim: FeedClaim, out: PollOutcome, started: datetime, finished: datetime
    ) -> PollStats:
        """One transaction: new frontier rows, sightings, re-queues, discover events, the
        feed's poll state, the poll log and the host release."""
        stats = PollStats()
        now = finished
        n = to_db(now)
        events: list[StageEvent] = []
        with transaction(self.conn):
            for item in out.feed.items if out.feed else []:
                stats.items_seen += 1
                if self._discover(claim, item, n):
                    stats.items_new += 1
                    events.append(
                        new_event(
                            Stage.DISCOVER,
                            StageStatus.OK,
                            worker=self.worker,
                            started_at=started,
                            finished_at=finished,
                            url_id=ids.url_id(item.url),
                            feed_id=claim.feed_id,
                            pipeline_version=PIPELINE_VERSION,
                        )
                    )
                elif self._maybe_requeue(claim, item, n):
                    stats.requeued += 1
            self._update_feed(claim, out, stats, now)
            self.conn.execute(
                "INSERT INTO feed_polls (feed_id, polled_at, http_status, not_modified, "
                "items_seen, items_new, duration_ms, error_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT DO NOTHING",
                (
                    claim.feed_id,
                    to_db(started),
                    out.http_status,
                    int(out.not_modified),
                    stats.items_seen,
                    stats.items_new,
                    int((finished - started).total_seconds() * 1000),
                    out.error_code,
                ),
            )
            release_host(
                self.conn,
                self.s,
                claim.host,
                now,
                network_error=out.network_error,
                backoff_s=out.host_backoff_s,
            )
            record(self.conn, events)
        return stats

    def _discover(self, claim: FeedClaim, item: FeedItem, n: str) -> bool:
        uid = ids.url_id(item.url)
        cur = self.conn.execute(
            "INSERT INTO frontier (url_id, url, host, source_id, feed_id, discovered_at, "
            "feed_published_at, feed_updated_at, feed_title, feed_summary, state, priority, "
            "ready_at, next_attempt_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?) "
            "ON CONFLICT (url_id) DO NOTHING",
            (
                uid,
                item.url,
                host_of(item.url),
                claim.source_id,
                claim.feed_id,
                n,
                to_db(item.published_at) if item.published_at else None,
                to_db(item.updated_at) if item.updated_at else None,
                item.title,
                item.summary,
                n,
                n,
                n,
            ),
        )
        self.conn.execute(
            "INSERT INTO url_sightings (url_id, feed_id, first_seen_at) VALUES (?, ?, ?) "
            "ON CONFLICT DO NOTHING",
            (uid, claim.feed_id, n),
        )
        return cur.rowcount == 1

    def _maybe_requeue(self, claim: FeedClaim, item: FeedItem, n: str) -> bool:
        row = self.conn.execute(
            "SELECT state, feed_id, feed_updated_at, feed_title FROM frontier WHERE url_id = ?",
            (ids.url_id(item.url),),
        ).fetchone()
        if row is None or row["state"] != "stored" or row["feed_id"] != claim.feed_id:
            return False
        newer = (
            item.updated_at is not None
            and row["feed_updated_at"] is not None
            and item.updated_at > from_db(row["feed_updated_at"])
        )
        retitled = item.title is not None and row["feed_title"] not in (None, item.title)
        if not (newer or retitled):
            return False
        self.conn.execute(
            "UPDATE frontier SET state = 'pending', priority = ?, attempts = 0, "
            "next_attempt_at = ?, ready_at = ?, updated_at = ?, feed_title = ?, "
            "feed_updated_at = ?, feed_summary = ?, last_error = NULL, skip_reason = NULL "
            "WHERE url_id = ?",
            (
                REFETCH_PRIORITY,
                n,
                n,
                n,
                item.title,
                to_db(item.updated_at) if item.updated_at else row["feed_updated_at"],
                item.summary,
                ids.url_id(item.url),
            ),
        )
        return True

    def _update_feed(
        self, claim: FeedClaim, out: PollOutcome, stats: PollStats, now: datetime
    ) -> None:
        interval = next_interval_s(claim.poll_interval_s, stats.items_new > 0, self.s)
        wait = max(float(interval), out.retry_after_s or 0.0)
        next_poll = to_db(now + jittered(wait, self.rng))
        n = to_db(now)
        if out.ok:
            self.conn.execute(
                "UPDATE feeds SET poll_interval_s = ?, next_poll_at = ?, last_polled_at = ?, "
                "last_success_at = ?, consecutive_failures = 0, "
                "etag = CASE WHEN ? THEN etag ELSE ? END, "
                "last_modified = CASE WHEN ? THEN last_modified ELSE ? END, "
                "last_new_item_at = CASE WHEN ? > 0 THEN ? ELSE last_new_item_at END, "
                "updated_at = ? WHERE feed_id = ?",
                (
                    interval,
                    next_poll,
                    n,
                    n,
                    out.not_modified,
                    out.etag,
                    out.not_modified,
                    out.last_modified,
                    stats.items_new,
                    n,
                    n,
                    claim.feed_id,
                ),
            )
            return
        failures = claim.consecutive_failures + 1
        pause = failures >= self.s.poll.pause_after_failures
        self.conn.execute(
            "UPDATE feeds SET poll_interval_s = ?, next_poll_at = ?, last_polled_at = ?, "
            "consecutive_failures = ?, "
            "status = CASE WHEN ? THEN 'paused' ELSE status END, "
            "status_reason = CASE WHEN ? THEN ? ELSE status_reason END, "
            "updated_at = ? WHERE feed_id = ?",
            (
                interval,
                next_poll,
                n,
                failures,
                pause,
                pause,
                f"{failures} consecutive failures, last: {out.error_code}",
                n,
                claim.feed_id,
            ),
        )
        if pause:
            log.warning("paused feed %s after %d failures", claim.feed_url, failures)


async def run_poller(settings: Settings, conn: sqlite3.Connection, stop: asyncio.Event) -> None:
    async with make_client(settings) as client:
        await Poller(settings, conn, client, worker_id("poller")).run(stop)
