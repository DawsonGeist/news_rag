import asyncio
import random
import sqlite3
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from newsrag import ids
from newsrag.config import FetchSettings, PollSettings, Settings
from newsrag.contracts.v1 import FeedCandidate, FeedMedium, FeedOrigin
from newsrag.crawl.poller import Poller, claim_due_feeds, next_interval_s, parse_feed
from newsrag.feeds.registry import import_candidates, resolve_feed, set_status
from newsrag.storage.state import connect, init_schema, transaction
from newsrag.timeutil import from_db, to_db, utcnow

FEEDS = Path(__file__).parent / "fixtures" / "synthetic" / "feeds"
NOW = datetime(2026, 10, 17, 14, 0, tzinfo=UTC)


def settings_for(data_dir: Path, **poll: Any) -> Settings:
    return Settings(
        data_dir=data_dir,
        fetch=FetchSettings(default_crawl_delay_s=0.0, request_timeout_s=5.0),
        poll=PollSettings(**poll),
    )


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def conn(data_dir: Path) -> Iterator[sqlite3.Connection]:
    c = connect(data_dir / "state" / "crawl.sqlite3")
    init_schema(c)
    yield c
    c.close()


def add_feed(conn: sqlite3.Connection, url: str, source: str = "ledger.example") -> str:
    norm = ids.normalize_url(url)
    cand = FeedCandidate(
        feed_id=ids.feed_id(norm),
        feed_url=norm,
        source_id=source,
        listed_domain=source,
        country="United States",
        categories=["Local News"],
        medium=FeedMedium.TEXT,
        origin=FeedOrigin.MANUAL,
    )
    import_candidates(conn, [cand], utcnow())
    set_status(conn, [cand.feed_id], "active", "test", utcnow())
    return cand.feed_id


Responder = Callable[[httpx.Request], httpx.Response]


def transport(routes: dict[str, Responder | httpx.Response]) -> httpx.MockTransport:
    async def handler(req: httpx.Request) -> httpx.Response:
        key = f"{req.url.host}{req.url.path}"
        r = routes.get(key)
        if r is None:
            return httpx.Response(404)
        return r(req) if callable(r) else r

    return httpx.MockTransport(handler)


def xml(name: str, **headers: str) -> httpx.Response:
    return httpx.Response(
        200,
        headers={"content-type": "application/rss+xml"} | headers,
        content=(FEEDS / name).read_bytes(),
    )


async def poll_once(s: Settings, conn: sqlite3.Connection, routes: dict[str, Any]) -> int:
    async with httpx.AsyncClient(transport=transport(routes)) as client:
        return await asyncio.wait_for(
            Poller(s, conn, client, "test:poller", random.Random(0)).poll_due_once(), 20
        )


def frontier(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    return {r["url"]: r for r in conn.execute("SELECT * FROM frontier")}


def make_due(conn: sqlite3.Connection) -> None:
    with transaction(conn):
        conn.execute("UPDATE feeds SET next_poll_at = ?", (to_db(utcnow()),))
        conn.execute("UPDATE hosts SET next_allowed_at = NULL, backoff_until = NULL")


# --- pure pieces ------------------------------------------------------------------------


def test_parse_rss2() -> None:
    feed = parse_feed(
        (FEEDS / "rss2.xml").read_bytes(), "https://millbrookledger.example/rss", None, NOW
    )
    assert feed.format == "rss" and feed.invalid_links == 1
    urls = [i.url for i in feed.items]
    assert urls == [
        "https://millbrookledger.example/news/solar-farm-approved",  # tracking param dropped
        "https://millbrookledger.example/news/river-cleanup",  # relative link resolved
        "https://millbrookledger.example/2026/10/15/library-hours",  # origLink preferred
    ]
    first = feed.items[0]
    assert first.published_at == datetime(2026, 10, 16, 22, 30, tzinfo=UTC)
    assert first.summary == "<p>The council voted five to two on Thursday night.</p>"


def test_parse_atom_and_undated_and_podcast() -> None:
    atom = parse_feed(
        (FEEDS / "atom.xml").read_bytes(), "https://riverside.example/feed", None, NOW
    )
    assert atom.format == "atom"
    assert atom.items[0].published_at == datetime(2026, 10, 17, 13, 0, tzinfo=UTC)
    assert atom.items[0].updated_at == datetime(2026, 10, 17, 14, 15, tzinfo=UTC)
    storm = atom.items[1]  # only <updated>: used as the publication time
    assert storm.published_at == storm.updated_at == datetime(2026, 10, 13, 22, 10, tzinfo=UTC)
    undated = parse_feed(
        (FEEDS / "no_dates.xml").read_bytes(), "https://undated.example/", None, NOW
    )
    assert [i.published_at for i in undated.items] == [None, None]
    assert [i.updated_at for i in undated.items] == [None, None]
    pod = parse_feed(
        (FEEDS / "podcast.xml").read_bytes(), "https://millbrookledger.example/p", None, NOW
    )
    assert pod.audio_items == 1


def test_audio_counted_only_over_kept_items() -> None:
    # Podcast entries without a usable link must not push the audio ratio above 1.
    body = (
        (FEEDS / "podcast.xml")
        .read_bytes()
        .replace(
            b"</channel>",
            b'<item><title>No link</title><enclosure url="https://cdn.audio.example/x.mp3" '
            b'type="audio/mpeg"/></item></channel>',
        )
    )
    feed = parse_feed(body, "https://millbrookledger.example/p", None, NOW)
    assert (len(feed.items), feed.audio_items, feed.invalid_links) == (1, 1, 1)


def test_parse_rejects_garbage() -> None:
    for ctype in ("text/html", None):
        with pytest.raises(ValueError):
            parse_feed(b"<html><body>not a feed</body></html>", "https://e.com/", ctype, NOW)
    empty = b'<?xml version="1.0"?><rss version="2.0"><channel><title>t</title></channel></rss>'
    assert parse_feed(empty, "https://e.com/", None, NOW).items == []  # empty but valid


def test_future_dates_dropped() -> None:
    body = (
        (FEEDS / "no_dates.xml")
        .read_bytes()
        .replace(
            b"<title>First story</title>",
            b"<title>First story</title><pubDate>Mon, 01 Jan 2035 00:00:00 GMT</pubDate>",
        )
    )
    feed = parse_feed(body, "https://undated.example/", None, NOW)
    assert feed.items[0].published_at is None


def test_next_interval(data_dir: Path) -> None:
    s = settings_for(data_dir)
    assert next_interval_s(1800, True, s) == 1260
    assert next_interval_s(1800, False, s) == 2700
    assert next_interval_s(310, True, s) == 300  # clamped to min
    assert next_interval_s(20000, False, s) == 21600  # clamped to max


# --- polling ------------------------------------------------------------------------------


async def test_poll_discovers_items(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    fid = add_feed(conn, "https://millbrookledger.example/rss")
    assert (
        await poll_once(s, conn, {"millbrookledger.example/rss": xml("rss2.xml", etag='"v1"')}) == 1
    )
    rows = frontier(conn)
    assert len(rows) == 3
    solar = rows["https://millbrookledger.example/news/solar-farm-approved"]
    assert (solar["state"], solar["feed_id"], solar["source_id"]) == (
        "pending",
        fid,
        "ledger.example",
    )
    assert solar["feed_title"] == "Millbrook council approves solar farm"
    assert from_db(solar["feed_published_at"]) == datetime(2026, 10, 16, 22, 30, tzinfo=UTC)
    feed = conn.execute("SELECT * FROM feeds").fetchone()
    assert feed["etag"] == '"v1"' and feed["consecutive_failures"] == 0
    assert feed["poll_interval_s"] == 1260  # new items: interval shrinks
    nxt = from_db(feed["next_poll_at"]) - from_db(feed["last_polled_at"])
    assert timedelta(seconds=1260 * 0.9) <= nxt <= timedelta(seconds=1260 * 1.1)
    events = conn.execute("SELECT stage, count(*) FROM stage_events GROUP BY 1").fetchall()
    assert [tuple(e) for e in events] == [("discover", 3)]
    log = conn.execute("SELECT items_seen, items_new, http_status FROM feed_polls").fetchone()
    assert tuple(log) == (3, 3, 200)


async def test_second_poll_is_conditional(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    add_feed(conn, "https://millbrookledger.example/rss")
    seen: list[dict[str, str]] = []

    def respond(req: httpx.Request) -> httpx.Response:
        seen.append(dict(req.headers))
        if req.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304)
        return xml("rss2.xml", etag='"v1"', **{"last-modified": "Fri, 16 Oct 2026 22:00:00 GMT"})

    routes = {"millbrookledger.example/rss": respond}
    await poll_once(s, conn, routes)
    make_due(conn)
    await poll_once(s, conn, routes)
    assert seen[1]["if-none-match"] == '"v1"'
    assert seen[1]["if-modified-since"] == "Fri, 16 Oct 2026 22:00:00 GMT"
    feed = conn.execute("SELECT etag, poll_interval_s FROM feeds").fetchone()
    assert feed["etag"] == '"v1"'  # kept across a 304
    assert feed["poll_interval_s"] == 1890  # 1260 * 1.5: nothing new
    assert len(frontier(conn)) == 3
    polls = conn.execute("SELECT not_modified FROM feed_polls ORDER BY polled_at").fetchall()
    assert [p[0] for p in polls] == [0, 1]


async def test_requeue_only_on_update_from_first_feed(
    conn: sqlite3.Connection, data_dir: Path
) -> None:
    s = settings_for(data_dir)
    atom_feed = add_feed(conn, "https://riverside.example/feed", "riverside.example")
    other = add_feed(conn, "https://riverside.example/other", "riverside.example")
    body = (FEEDS / "atom.xml").read_bytes()
    routes: dict[str, Any] = {
        "riverside.example/feed": httpx.Response(200, content=body),
        "riverside.example/other": httpx.Response(
            200, content=body.replace(b"Farmers market moves", b"Market moves")
        ),
    }
    await poll_once(s, conn, routes)
    url = "https://riverside.example/2026/10/farmers-market"
    assert frontier(conn)[url]["feed_id"] in (atom_feed, other)
    first_feed = frontier(conn)[url]["feed_id"]
    with transaction(conn):
        conn.execute("UPDATE frontier SET state = 'stored'")

    # Same content again from both feeds: nothing is re-queued, even though the other
    # feed's title differs.
    make_due(conn)
    await poll_once(s, conn, routes)
    assert {r["state"] for r in frontier(conn).values()} == {"stored"}

    # The first feed reports a newer updated time: re-queued at low priority.
    first_key = "riverside.example/feed" if first_feed == atom_feed else "riverside.example/other"
    routes[first_key] = httpx.Response(
        200, content=routes[first_key].content.replace(b"10:15:00-04:00", b"11:30:00-04:00")
    )
    make_due(conn)
    await poll_once(s, conn, routes)
    row = frontier(conn)[url]
    assert (row["state"], row["priority"], row["attempts"]) == ("pending", -1, 0)
    assert from_db(row["feed_updated_at"]) == datetime(2026, 10, 17, 15, 30, tzinfo=UTC)
    assert frontier(conn)["https://riverside.example/2026/10/storm"]["state"] == "stored"


async def test_failures_pause_feed(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir, pause_after_failures=3)
    add_feed(conn, "https://dead.example/rss")
    for _ in range(3):
        make_due(conn)
        await poll_once(s, conn, {"dead.example/rss": httpx.Response(500)})
    feed = conn.execute("SELECT status, status_reason, consecutive_failures FROM feeds").fetchone()
    assert feed["status"] == "paused" and feed["consecutive_failures"] == 3
    assert "http_500" in feed["status_reason"]
    make_due(conn)
    assert await poll_once(s, conn, {}) == 0  # paused feeds are not polled


async def test_unparseable_and_retry_after(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    add_feed(conn, "https://a.example/rss", "a.example")
    add_feed(conn, "https://b.example/rss", "b.example")
    await poll_once(
        s,
        conn,
        {
            "a.example/rss": httpx.Response(200, content=b"<html>moved</html>"),
            "b.example/rss": httpx.Response(429, headers={"retry-after": "7200"}),
        },
    )
    feeds = {r["feed_url"]: r for r in conn.execute("SELECT * FROM feeds")}
    a, b = feeds["https://a.example/rss"], feeds["https://b.example/rss"]
    assert a["consecutive_failures"] == 1 and a["status"] == "active"
    log = dict(conn.execute("SELECT feed_id, error_code FROM feed_polls").fetchall())
    assert set(log.values()) == {"unparseable", "http_429"}
    assert from_db(b["next_poll_at"]) - from_db(b["last_polled_at"]) >= timedelta(
        seconds=7200 * 0.9
    )
    host = conn.execute("SELECT backoff_until FROM hosts WHERE host = 'b.example'").fetchone()
    assert from_db(host[0]) > utcnow() + timedelta(seconds=7000)


def test_claim_one_feed_per_host_and_lease(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    add_feed(conn, "https://same.example/a", "same.example")
    add_feed(conn, "https://same.example/b", "same.example")
    add_feed(conn, "https://other.example/rss", "other.example")
    claims = claim_due_feeds(conn, s, 10, utcnow())
    assert sorted(c.host for c in claims) == ["other.example", "same.example"]
    assert claim_due_feeds(conn, s, 10, utcnow()) == []  # hosts reserved, feeds leased


def test_registry_import_is_idempotent_and_keeps_status(conn: sqlite3.Connection) -> None:
    fid = add_feed(conn, "https://millbrookledger.example/rss")
    assert resolve_feed(conn, "https://millbrookledger.example/rss") == fid
    assert resolve_feed(conn, fid) == fid
    assert resolve_feed(conn, "https://unknown.example/") is None
    cand = FeedCandidate(
        feed_id=fid,
        feed_url="https://millbrookledger.example/rss",
        source_id="ledger.example",
        listed_domain="ledger.example",
        country="United States",
        categories=["Politics & Society"],
        medium=FeedMedium.TEXT,
        origin=FeedOrigin.MANUAL,
    )
    stats = import_candidates(conn, [cand], utcnow())
    assert (stats.feeds_new, stats.feeds_updated) == (0, 1)
    row = conn.execute("SELECT status, categories_json FROM feeds").fetchone()
    assert row["status"] == "active" and "Politics" in row["categories_json"]
