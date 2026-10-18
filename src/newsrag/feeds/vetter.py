"""Feed vetter stages B and C (phase1-design.md, component 1).

Works one publisher (``source_id``) at a time, up to ``vet.concurrency`` publishers
at once:

- **Stage B** fetches and parses each of the publisher's text feeds, up to
  ``vet.feed_attempts`` tries for transient failures (network, 5xx, 429,
  robots.txt unavailable) with backoff.
- **Stage C** samples recent articles, newest first and round-robin across the
  publisher's feeds: up to ``samples_per_feed`` per feed and ``samples_per_source``
  in total, and runs the real extractor on them in a process pool. A feed whose own
  items were not sampled is judged on the publisher's samples (same site, same CMS).

Each publisher's reports are committed together: a ``feed_health`` row per feed, and
``feeds.status`` set from the verdict. Runs resume: feeds already vetted by this
``VETTER_VERSION`` are skipped unless ``recheck`` is set. Sampled pages are not stored
in the archive.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import Executor
from dataclasses import dataclass, field
from datetime import datetime

import httpx

from newsrag.config import Settings
from newsrag.contracts.v1 import Document, FeedHealthReport
from newsrag.crawl.http import HostSlots, PoliteClient, accept_any, host_of
from newsrag.crawl.poller import FeedItem, parse_feed
from newsrag.extract.extractor import ExtractContext, extract
from newsrag.feeds.registry import LATEST_HEALTH_SQL, MANUAL_VERSION, is_manual
from newsrag.feeds.verdict import (
    STATUS_FOR,
    VETTER_VERSION,
    FeedCheck,
    Sample,
    audio_report,
    build_report,
)
from newsrag.storage.state import transaction
from newsrag.timeutil import to_db, utcnow

log = logging.getLogger(__name__)

PIPELINE_VERSION = "vetter-1"
TRANSIENT = frozenset({"network", "timeout", "robots_unavailable"})


@dataclass(frozen=True)
class FeedRow:
    feed_id: str
    feed_url: str
    source_id: str
    medium: str


@dataclass
class _Checked:
    feed: FeedRow
    check: FeedCheck
    items: list[FeedItem] = field(default_factory=list)


def select_feeds(
    conn: sqlite3.Connection,
    *,
    recheck: bool = False,
    limit: int | None = None,
    seed: int | None = None,
    sources: Sequence[str] = (),
) -> dict[str, list[FeedRow]]:
    """Feeds to vet, grouped by publisher. ``limit`` picks whole publishers at random
    (seeded) until at least that many feeds are chosen, for a representative pilot.

    Feeds with a manual decision are never selected, ``recheck`` or not."""
    rows = conn.execute(
        "SELECT f.feed_id, f.feed_url, f.source_id, f.medium FROM feeds f "
        f"LEFT JOIN ({LATEST_HEALTH_SQL}) l ON l.feed_id = f.feed_id "
        "WHERE (l.version IS NULL OR l.version != ?) "
        "AND (? OR l.version IS NULL OR l.version != ?) "
        "ORDER BY f.source_id, f.feed_url",
        (MANUAL_VERSION, recheck, VETTER_VERSION),
    ).fetchall()
    grouped: dict[str, list[FeedRow]] = defaultdict(list)
    for r in rows:
        if not sources or r["source_id"] in sources:
            grouped[r["source_id"]].append(FeedRow(*r))
    if limit is None:
        return dict(grouped)
    order = sorted(grouped)
    random.Random(seed).shuffle(order)
    chosen: dict[str, list[FeedRow]] = {}
    count = 0
    for src in order:
        if count >= limit:
            break
        chosen[src] = grouped[src]
        count += len(grouped[src])
    return chosen


def commit_reports(conn: sqlite3.Connection, reports: Sequence[FeedHealthReport]) -> int:
    """Store reports and set statuses. A feed decided by hand while the run was in
    progress keeps its decision, and its report is dropped. Returns reports stored."""
    stored = 0
    with transaction(conn):
        for r in reports:
            if is_manual(conn, r.feed_id):
                continue
            stored += 1
            conn.execute(
                "INSERT INTO feed_health (feed_id, checked_at, verdict, report_json) "
                "VALUES (?, ?, ?, ?)",
                (r.feed_id, to_db(r.checked_at), r.verdict.value, r.model_dump_json()),
            )
            reason = ",".join(i.value for i in r.issues) or None
            conn.execute(
                "UPDATE feeds SET status = ?, status_reason = ?, updated_at = ?, "
                "next_poll_at = CASE WHEN ? = 'active' THEN NULL ELSE next_poll_at END "
                "WHERE feed_id = ?",
                (
                    STATUS_FOR[r.verdict],
                    reason,
                    to_db(r.checked_at),
                    STATUS_FOR[r.verdict],
                    r.feed_id,
                ),
            )
    return stored


class Vetter:
    def __init__(
        self,
        settings: Settings,
        conn: sqlite3.Connection,
        client: httpx.AsyncClient,
        pool: Executor | None,
    ) -> None:
        self.s = settings
        self.cfg = settings.vet
        self.conn = conn
        self.http = PoliteClient(settings, conn, client)
        self.slots = HostSlots(conn, settings)
        self.pool = pool  # None: extract in the event loop (tests)

    async def run(self, by_source: dict[str, list[FeedRow]]) -> dict[str, int]:
        """Vet every feed in ``by_source``. Returns verdict counts."""
        counts: dict[str, int] = defaultdict(int)
        sem = asyncio.Semaphore(self.cfg.concurrency)
        done = 0

        async def one(source_id: str, feeds: list[FeedRow]) -> None:
            nonlocal done
            async with sem:
                try:
                    reports = await self.vet_source(source_id, feeds)
                except Exception:
                    # One publisher's surprise must not end a multi-hour run; it stays
                    # unvetted and is picked up by the next (resumed) run.
                    log.exception("vetting %s failed", source_id)
                    return
            if commit_reports(self.conn, reports) < len(reports):
                log.info("%s: kept manual decisions made during the run", source_id)
            for r in reports:
                counts[r.verdict.value] += 1
            done += 1
            if done % 25 == 0 or done == len(by_source):
                log.info("vetted %d/%d publishers: %s", done, len(by_source), dict(counts))

        await asyncio.gather(*(one(s, f) for s, f in by_source.items()))
        return dict(counts)

    async def vet_source(self, source_id: str, feeds: list[FeedRow]) -> list[FeedHealthReport]:
        now = utcnow()
        reports = [audio_report(f.feed_id, f.feed_url, now) for f in feeds if f.medium != "text"]
        checked = [await self.check_feed(f) for f in feeds if f.medium == "text"]
        picks = self.pick_samples(checked)
        samples: dict[str, Sample] = {}
        for url, item in picks:
            samples[url] = await self.fetch_sample(url, item, source_id)
        everything = list(samples.values())
        duplicates = find_duplicates(checked)
        for c in checked:
            own = [samples[i.url] for i in c.items if i.url in samples]
            usable = c.check.parse_ok and c.check.item_count > 0
            reports.append(
                build_report(
                    c.feed.feed_id,
                    c.feed.feed_url,
                    c.check,
                    own or (everything if usable else []),
                    utcnow(),
                    self.cfg,
                    duplicate_of=duplicates.get(c.feed.feed_id),
                )
            )
        return reports

    def pick_samples(self, checked: Sequence[_Checked]) -> list[tuple[str, FeedItem]]:
        """Newest items first, round-robin across feeds, deduplicated."""
        epoch = datetime.min.replace(tzinfo=utcnow().tzinfo)
        queues = [
            sorted(c.items, key=lambda i: i.published_at or epoch, reverse=True)[
                : self.cfg.samples_per_feed
            ]
            for c in checked
            if c.check.parse_ok
        ]
        picks: list[tuple[str, FeedItem]] = []
        seen: set[str] = set()
        while any(queues) and len(picks) < self.cfg.samples_per_source:
            for q in queues:
                if q and len(picks) < self.cfg.samples_per_source:
                    item = q.pop(0)
                    if item.url not in seen:
                        seen.add(item.url)
                        picks.append((item.url, item))
        return picks

    async def check_feed(self, feed: FeedRow) -> _Checked:
        check = FeedCheck()
        host = host_of(feed.feed_url)
        for attempt in range(1, self.cfg.feed_attempts + 1):
            async with self.slots.slot(host) as slot:
                res = await self.http.get(feed.feed_url, accept=accept_any)
                slot.network_error = res.kind in ("network", "timeout")
                slot.blocked = res.kind == "http_error" and res.status == 403
            check.http_status = res.status
            check.final_feed_url = res.final_url
            check.error = _error(res.kind, res.status, res.error)
            transient = res.kind in TRANSIENT or (
                res.kind == "http_error" and (res.status or 0) in (429, 500, 502, 503, 504)
            )
            if transient and attempt < self.cfg.feed_attempts:
                await asyncio.sleep(self.s.fetch.retry_base_s * 4 ** (attempt - 1))
                continue
            break
        delay = self.conn.execute(
            "SELECT crawl_delay_s FROM hosts WHERE host = ?", (host,)
        ).fetchone()
        check.crawl_delay_s = delay[0] if delay else None
        if res.kind == "disallowed":
            check.robots_allows_feed = False
            return _Checked(feed, check)
        check.robots_allows_feed = True if res.kind != "robots_unavailable" else None
        if res.kind != "ok" or res.body is None:
            return _Checked(feed, check)
        check.reachable = True
        try:
            parsed = parse_feed(res.body, res.final_url, res.content_type, utcnow())
        except ValueError as e:
            check.error = str(e)[:200]
            return _Checked(feed, check)
        check.parse_ok = True
        check.feed_format = parsed.format
        check.item_dates = [i.published_at for i in parsed.items]
        check.audio_items = parsed.audio_items
        return _Checked(feed, check, parsed.items)

    async def fetch_sample(self, url: str, item: FeedItem, source_id: str) -> Sample:
        async with self.slots.slot(host_of(url)) as slot:
            res = await self.http.get(url)
            slot.network_error = res.kind in ("network", "timeout")
            slot.blocked = res.kind == "http_error" and res.status == 403
        if res.kind == "disallowed":
            return Sample(url, None, robots_disallowed=True, error="robots")
        if res.kind != "ok" or res.body is None:
            return Sample(url, None, error=_error(res.kind, res.status, res.error))
        now = utcnow()
        ctx = ExtractContext(
            final_url=res.final_url,
            source_id=source_id,
            discovered_at=now,
            fetched_at=now,
            pipeline_version=PIPELINE_VERSION,
            content_type=res.content_type,
            feed_summary=item.summary,
            feed_published_at=item.published_at,
        )
        if self.pool is None:
            doc = extract(res.body, ctx)
        else:
            loop = asyncio.get_running_loop()
            doc = await loop.run_in_executor(self.pool, extract, res.body, ctx)
        if not isinstance(doc, Document):
            return Sample(url, None, error=doc.error_code)
        # Any source counts, the feed's included: the question is whether this feed's
        # Documents will carry a publication date.
        return Sample(
            url, doc.content_kind, doc.language, doc.published_at is not None, doc.word_count
        )


def _error(kind: str, status: int | None, error: str | None) -> str | None:
    if kind == "ok":
        return None
    if kind == "http_error" and status is not None:
        return f"http_{status}"
    return error or kind


def find_duplicates(checked: Sequence[_Checked]) -> dict[str, str]:
    """feed_id -> feed_url of the publisher's earlier feed with exactly the same item
    URLs. The shortest URL is kept (usually the main feed). Only identical sets count:
    a subset is a section feed with its own value."""
    keep: dict[frozenset[str], str] = {}
    dupes: dict[str, str] = {}
    for c in sorted(checked, key=lambda c: (len(c.feed.feed_url), c.feed.feed_url)):
        if not c.check.parse_ok or not c.items:
            continue
        key = frozenset(i.url for i in c.items)
        if key in keep:
            dupes[c.feed.feed_id] = keep[key]
        else:
            keep[key] = c.feed.feed_url
    return dupes


def report_counts(conn: sqlite3.Connection) -> dict[str, object]:
    """Latest verdict per feed, by verdict and by issue."""
    latest = conn.execute(
        "SELECT h.verdict, h.report_json FROM feed_health h JOIN ("
        "  SELECT feed_id, max(checked_at) AS t FROM feed_health GROUP BY feed_id"
        ") m ON h.feed_id = m.feed_id AND h.checked_at = m.t"
    ).fetchall()
    verdicts: dict[str, int] = defaultdict(int)
    issues: dict[str, int] = defaultdict(int)
    manual: dict[str, int] = defaultdict(int)
    for r in latest:
        verdicts[r["verdict"]] += 1
        report = FeedHealthReport.model_validate_json(r["report_json"])
        if report.vetter_version == MANUAL_VERSION:
            manual[r["verdict"]] += 1
        for issue in report.issues:
            issues[issue.value] += 1
    total = conn.execute("SELECT count(*) FROM feeds").fetchone()[0]
    return {
        "feeds": total,
        "vetted": len(latest),
        "verdicts": dict(verdicts),
        "manual": dict(manual),
        "issues": dict(issues),
    }
