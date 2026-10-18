import asyncio
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from newsrag import ids
from newsrag.config import FetchSettings, Settings, VetSettings
from newsrag.contracts.v1 import (
    ContentKind,
    FeedCandidate,
    FeedHealthReport,
    FeedIssue,
    FeedMedium,
    FeedOrigin,
    FeedVerdict,
)
from newsrag.crawl.http import HostSlots
from newsrag.feeds.registry import decide_manually, import_candidates
from newsrag.feeds.verdict import FeedCheck, Sample, build_report, items_per_day
from newsrag.feeds.vetter import Vetter, commit_reports, report_counts, select_feeds
from newsrag.storage.state import connect, init_schema
from newsrag.timeutil import utcnow

NOW = datetime(2026, 10, 17, 14, 0, tzinfo=UTC)
CFG = VetSettings()
ARTICLES = Path(__file__).parent / "fixtures" / "synthetic" / "articles"


# --- verdict rules ------------------------------------------------------------------------


def good_check(n: int = 10, newest_days_ago: float = 0.5) -> FeedCheck:
    return FeedCheck(
        http_status=200,
        reachable=True,
        robots_allows_feed=True,
        parse_ok=True,
        feed_format="rss",
        item_dates=[NOW - timedelta(days=newest_days_ago + i / 2) for i in range(n)],
    )


def full(words: int = 600, lang: str = "en", dated: bool = True) -> Sample:
    return Sample("u", ContentKind.FULL_TEXT, lang, dated, words)


def report(check: FeedCheck, samples: list[Sample]) -> FeedHealthReport:
    return build_report("0" * 32, "https://e.com/rss", check, samples, NOW, CFG)


def test_accept() -> None:
    r = report(good_check(), [full(), full(), full()])
    assert (r.verdict, r.issues) == (FeedVerdict.ACCEPT, [])
    assert r.full_text_ratio == 1.0 and r.median_word_count == 600
    assert r.est_items_per_day == 2.0 and r.items_with_date_ratio == 1.0


@pytest.mark.parametrize(
    ("check", "samples", "issue"),
    [
        (FeedCheck(robots_allows_feed=False), [], FeedIssue.ROBOTS_DISALLOWED),
        (FeedCheck(), [], FeedIssue.UNREACHABLE),
        (FeedCheck(http_status=404), [], FeedIssue.HTTP_ERROR),
        (FeedCheck(reachable=True, http_status=200), [], FeedIssue.UNPARSEABLE),
        (FeedCheck(reachable=True, parse_ok=True), [], FeedIssue.EMPTY),
        (good_check(newest_days_ago=40), [full()], FeedIssue.STALE),
        (
            good_check(),
            [Sample("u", ContentKind.SUSPECTED_PAYWALL, "en")] * 2 + [full()],
            FeedIssue.PAYWALLED,
        ),
        (good_check(), [full(lang="es")] * 2 + [full()], FeedIssue.NON_ENGLISH),
        (good_check(), [Sample("u", None, robots_disallowed=True)], FeedIssue.ROBOTS_DISALLOWED),
        (good_check(), [Sample("u", None)] * 3, FeedIssue.ARTICLE_FETCH_FAILED),
    ],
)
def test_rejects(check: FeedCheck, samples: list[Sample], issue: FeedIssue) -> None:
    r = report(check, samples)
    assert r.verdict is FeedVerdict.REJECT and issue in r.issues


def test_audio_heavy_feed_rejected() -> None:
    check = good_check()
    check.audio_items = 8
    assert FeedIssue.AUDIO_FEED in report(check, [full()]).issues


@pytest.mark.parametrize(
    ("samples", "issue"),
    [
        (
            [Sample("u", ContentKind.SUMMARY_ONLY, "en", True, 40)] * 2 + [full()],
            FeedIssue.SUMMARY_ONLY,
        ),
        (
            [Sample("u", ContentKind.NON_ARTICLE, "en", True, 40)] * 2 + [full()],
            FeedIssue.LOW_WORD_COUNT,
        ),
        ([full(dated=False)] * 2 + [full()], FeedIssue.NO_DATES),
        ([full(words=90)] * 3, FeedIssue.LOW_WORD_COUNT),
    ],
)
def test_reviews(samples: list[Sample], issue: FeedIssue) -> None:
    r = report(good_check(), samples)
    assert r.verdict is FeedVerdict.REVIEW and r.issues == [issue]
    assert r.detail and "samples:" in r.detail


def test_duplicate_feed_and_sample_errors_in_detail() -> None:
    r = build_report(
        "0" * 32,
        "https://e.com/rss2",
        good_check(),
        [full(), Sample("u", None, error="http_403")],
        NOW,
        CFG,
        duplicate_of="https://e.com/rss",
    )
    assert r.verdict is FeedVerdict.REJECT and r.issues == [FeedIssue.DUPLICATE_FEED]
    assert r.detail == (
        "same items as https://e.com/rss; samples: full_text 1; sample errors: http_403 1"
    )


def test_one_paywalled_sample_is_only_review() -> None:
    samples = [Sample("u", ContentKind.SUSPECTED_PAYWALL, "en", True, 50), full(), full()]
    r = report(good_check(), samples)
    assert r.verdict is FeedVerdict.REVIEW and r.issues == [FeedIssue.PAYWALLED]


def test_items_per_day() -> None:
    assert items_per_day([None, NOW]) is None
    assert items_per_day([NOW - timedelta(days=2), NOW - timedelta(days=1), NOW]) == 1.0
    assert items_per_day([NOW, NOW, NOW]) == 48.0  # same instant: capped, not infinite


# --- host slots -------------------------------------------------------------------------


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    c = connect(tmp_path / "data" / "state" / "crawl.sqlite3")
    init_schema(c)
    yield c
    c.close()


async def test_host_slots_serialize_with_crawl_delay(
    conn: sqlite3.Connection, tmp_path: Path
) -> None:
    s = Settings(data_dir=tmp_path / "data", fetch=FetchSettings(default_crawl_delay_s=0.2))
    slots = HostSlots(conn, s, poll_s=0.02)
    spans: list[tuple[str, float, float]] = []

    async def use(host: str) -> None:
        async with slots.slot(host):
            start = time.monotonic()
            await asyncio.sleep(0.03)
            spans.append((host, start, time.monotonic()))

    await asyncio.gather(*(use(h) for h in ["a.com"] * 3 + ["b.com"] * 3))
    for host in ("a.com", "b.com"):
        times = sorted((s, e) for h, s, e in spans if h == host)
        gaps = [b[0] - a[1] for a, b in zip(times, times[1:], strict=False)]
        assert min(gaps) >= 0.18, gaps  # one at a time, crawl delay between
    a0 = min(s for h, s, _ in spans if h == "a.com")
    b0 = min(s for h, s, _ in spans if h == "b.com")
    assert abs(a0 - b0) < 0.15  # different hosts in parallel


# --- end to end over mock publishers ----------------------------------------------------------


def rfc822(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def rss(base: str, n: int, days_ago: float = 0.2) -> bytes:
    items = "".join(
        f"<item><title>Story {i}</title><link>{base}/story/{i}</link>"
        f"<description>Short teaser {i}.</description>"
        f"<pubDate>{rfc822(NOW - timedelta(days=days_ago + i / 4))}</pubDate></item>"
        for i in range(n)
    )
    head = '<?xml version="1.0"?><rss version="2.0"><channel><title>t</title>'
    return f"{head}{items}</channel></rss>".encode()


GOOD_PAGE = (ARTICLES / "full_jsonld.html").read_bytes()
PAYWALL_PAGE = (ARTICLES / "paywall_jsonld.html").read_bytes()


def publishers() -> dict[str, Any]:
    days_ago = (utcnow() - NOW).total_seconds() / 86400
    return {
        "good.example/rss": httpx.Response(200, content=rss("https://good.example", 6, -days_ago)),
        "good.example/rss2": httpx.Response(200, content=rss("https://good.example", 6, -days_ago)),
        "good.example/politics": httpx.Response(
            200, content=rss("https://good.example/p", 4, -days_ago)
        ),
        "paywalled.example/rss": httpx.Response(
            200, content=rss("https://paywalled.example", 5, -days_ago)
        ),
        "stale.example/rss": httpx.Response(
            200, content=rss("https://stale.example", 5, 60 - days_ago)
        ),
        "blocked.example/robots.txt": httpx.Response(200, text="User-agent: *\nDisallow: /\n"),
        "down.example/rss": httpx.Response(503),
    }


def handler_for(routes: dict[str, Any], hits: list[str]) -> httpx.MockTransport:
    async def handler(req: httpx.Request) -> httpx.Response:
        key = f"{req.url.host}{req.url.path}"
        hits.append(key)
        if key in routes:
            return routes[key]
        if "/story/" in key:
            page = PAYWALL_PAGE if req.url.host == "paywalled.example" else GOOD_PAGE
            return httpx.Response(200, headers={"content-type": "text/html"}, content=page)
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def candidate(url: str, medium: FeedMedium = FeedMedium.TEXT) -> FeedCandidate:
    norm = ids.normalize_url(url)
    src = ids.registered_domain(norm)
    return FeedCandidate(
        feed_id=ids.feed_id(norm),
        feed_url=norm,
        source_id=src,
        listed_domain=src,
        country="United States",
        categories=["Local News"],
        medium=medium,
        origin=FeedOrigin.MANUAL,
    )


async def test_vet_end_to_end(conn: sqlite3.Connection, tmp_path: Path) -> None:
    s = Settings(
        data_dir=tmp_path / "data",
        fetch=FetchSettings(default_crawl_delay_s=0.0, retry_base_s=0.01, request_timeout_s=5),
    )
    cands = [
        candidate("https://good.example/rss"),
        candidate("https://good.example/politics"),
        candidate("https://good.example/rss2"),  # mirror of /rss
        candidate("https://paywalled.example/rss"),
        candidate("https://stale.example/rss"),
        candidate("https://blocked.example/rss"),
        candidate("https://down.example/rss"),
        candidate("https://good.example/podcast.xml", FeedMedium.AUDIO),
    ]
    import_candidates(conn, cands, utcnow())
    by_source = select_feeds(conn)
    assert sum(len(v) for v in by_source.values()) == 8
    hits: list[str] = []
    async with httpx.AsyncClient(transport=handler_for(publishers(), hits)) as client:
        counts = await Vetter(s, conn, client, pool=None).run(by_source)
    assert counts == {"accept": 2, "reject": 6}

    status = dict(conn.execute("SELECT feed_url, status FROM feeds").fetchall())
    assert status["https://good.example/rss"] == "active"
    assert status["https://good.example/politics"] == "active"
    for bad in ("paywalled", "stale", "blocked", "down"):
        assert status[f"https://{bad}.example/rss"] == "rejected", bad
    assert status["https://good.example/podcast.xml"] == "rejected"
    reasons = dict(conn.execute("SELECT feed_url, status_reason FROM feeds").fetchall())
    assert reasons["https://paywalled.example/rss"] == "paywalled"
    assert reasons["https://stale.example/rss"] == "stale"
    assert reasons["https://blocked.example/rss"] == "robots_disallowed"
    assert reasons["https://down.example/rss"] == "http_error"
    assert reasons["https://good.example/rss2"] == "duplicate_feed"

    # Sampling budget: at most 5 articles per publisher, at most 3 per feed.
    good_samples = [h for h in hits if h.startswith("good.example") and "/story/" in h]
    assert len(good_samples) == 5
    # The down feed was tried feed_attempts times; the podcast was never fetched.
    assert hits.count("down.example/rss") == 3
    assert not any("podcast" in h for h in hits)
    # Blocked: no feed request at all, only robots.txt.
    assert [h for h in hits if h.startswith("blocked.example")] == ["blocked.example/robots.txt"]

    good = FeedHealthReport.model_validate_json(
        conn.execute(
            "SELECT report_json FROM feed_health WHERE feed_id = ?",
            (ids.feed_id("https://good.example/rss"),),
        ).fetchone()[0]
    )
    # Round-robin over the 5-article budget: /politics (sorted first) gets 3, /rss 2.
    assert good.sampled_articles == 2 and good.full_text_ratio == 1.0
    assert good.median_word_count is not None and good.median_word_count > 150

    # Resumable: nothing left to vet; --recheck brings everything back.
    assert select_feeds(conn) == {}
    assert sum(len(v) for v in select_feeds(conn, recheck=True).values()) == 8
    summary = report_counts(conn)
    assert summary["vetted"] == 8 and summary["verdicts"] == {"accept": 2, "reject": 6}
    down = conn.execute(
        "SELECT report_json FROM feed_health WHERE feed_id = ?",
        (ids.feed_id("https://down.example/rss"),),
    ).fetchone()[0]
    assert FeedHealthReport.model_validate_json(down).detail == "http_503"


def test_select_feeds_pilot_picks_whole_publishers(conn: sqlite3.Connection) -> None:
    cands = [candidate(f"https://p{i}.example/rss{j}") for i in range(10) for j in range(3)]
    import_candidates(conn, cands, utcnow())
    picked = select_feeds(conn, limit=7, seed=1)
    assert sum(len(v) for v in picked.values()) == 9  # 3 whole publishers
    assert picked == select_feeds(conn, limit=7, seed=1)  # deterministic
    assert set(select_feeds(conn, sources=["p3.example"])) == {"p3.example"}


async def test_manual_decisions_are_final(conn: sqlite3.Connection, tmp_path: Path) -> None:
    s = Settings(
        data_dir=tmp_path / "data",
        fetch=FetchSettings(default_crawl_delay_s=0.0, retry_base_s=0.01, request_timeout_s=5),
    )
    good, stale = candidate("https://good.example/rss"), candidate("https://stale.example/rss")
    import_candidates(conn, [good, stale], utcnow())
    routes = publishers()

    async def vet(**kw: Any) -> None:
        async with httpx.AsyncClient(transport=handler_for(routes, [])) as client:
            await Vetter(s, conn, client, pool=None).run(select_feeds(conn, **kw))

    await vet()
    # A human overrides both machine verdicts.
    decide_manually(conn, [stale.feed_id], "active", "archive-worthy history site", utcnow())
    decide_manually(conn, [good.feed_id], "rejected", "off-topic", utcnow())

    # Neither a resumed run, --recheck, nor a new vetter version touches them.
    assert select_feeds(conn) == {}
    assert select_feeds(conn, recheck=True) == {}
    with monkeypatch_version("vet-99"):
        assert select_feeds(conn) == {}
    await vet(recheck=True)
    status = dict(conn.execute("SELECT feed_url, status FROM feeds").fetchall())
    assert status == {good.feed_url: "rejected", stale.feed_url: "active"}
    reason = conn.execute(
        "SELECT status_reason FROM feeds WHERE feed_id = ?", (stale.feed_id,)
    ).fetchone()[0]
    assert reason == "manual: archive-worthy history site"
    assert report_counts(conn)["manual"] == {"accept": 1, "reject": 1}


def test_manual_decision_during_a_run_wins(conn: sqlite3.Connection) -> None:
    feed = candidate("https://good.example/rss")
    import_candidates(conn, [feed], utcnow())
    machine = report(good_check(), [full()]).model_copy(
        update={"feed_id": feed.feed_id, "feed_url": feed.feed_url, "checked_at": utcnow()}
    )
    decide_manually(conn, [feed.feed_id], "rejected", "decided mid-run", utcnow())
    assert commit_reports(conn, [machine]) == 0  # the in-flight machine verdict is dropped
    assert conn.execute("SELECT status FROM feeds").fetchone()[0] == "rejected"


@contextmanager
def monkeypatch_version(version: str) -> Iterator[None]:
    import newsrag.feeds.vetter as vetter_mod

    old = vetter_mod.VETTER_VERSION
    vetter_mod.VETTER_VERSION = version
    try:
        yield
    finally:
        vetter_mod.VETTER_VERSION = old
