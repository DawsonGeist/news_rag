import asyncio
import gzip
import random
import sqlite3
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest

from newsrag import ids
from newsrag.config import FetchSettings, Settings
from newsrag.crawl.fetcher import Fetcher, claim_fetch_batch, host_of, make_client
from newsrag.storage.blobs import BlobStore
from newsrag.storage.fsio import TmpArea
from newsrag.storage.state import connect, init_schema, transaction
from newsrag.timeutil import from_db, to_db, utcnow

Handler = Callable[[httpx.Request], Awaitable[httpx.Response]]
HTML = {"content-type": "text/html; charset=utf-8"}


def settings_for(data_dir: Path, **fetch: Any) -> Settings:
    defaults: dict[str, Any] = {"default_crawl_delay_s": 0.0, "request_timeout_s": 5.0}
    return Settings(data_dir=data_dir, fetch=FetchSettings(**(defaults | fetch)))


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def conn(data_dir: Path) -> Iterator[sqlite3.Connection]:
    c = connect(data_dir / "state" / "crawl.sqlite3")
    init_schema(c)
    yield c
    c.close()


def add_urls(conn: sqlite3.Connection, *urls: str) -> list[str]:
    now = to_db(utcnow())
    rows = [(ids.url_id(u), ids.normalize_url(u), host_of(u), now, now, now, now) for u in urls]
    with transaction(conn):
        conn.executemany(
            "INSERT INTO frontier (url_id, url, host, source_id, discovered_at, state, ready_at, "
            "next_attempt_at, updated_at) VALUES (?, ?, ?, 'example.com', ?, 'pending', ?, ?, ?)",
            rows,
        )
    return [r[0] for r in rows]


def row(conn: sqlite3.Connection, url_id: str) -> sqlite3.Row:
    r: sqlite3.Row = conn.execute("SELECT * FROM frontier WHERE url_id = ?", (url_id,)).fetchone()
    return r


def routes(table: dict[str, httpx.Response | Callable[[], httpx.Response]]) -> Handler:
    """Mock server: '<host><path>' -> response. robots.txt defaults to 404 (allow all)."""

    async def handler(req: httpx.Request) -> httpx.Response:
        key = f"{req.url.host}{req.url.raw_path.decode()}"
        if key in table:
            resp = table[key]
            return resp() if callable(resp) else resp
        if req.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(404)

    return handler


async def run(
    settings: Settings, conn: sqlite3.Connection, handler: Handler, seed: int = 0
) -> Fetcher:
    with TmpArea(settings.tmp_dir, "test") as tmp:
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            headers={"User-Agent": settings.user_agent, "From": settings.contact_email},
        )
        async with client:
            fetcher = Fetcher(settings, conn, tmp, client, "test:fetch-1:0", random.Random(seed))
            await asyncio.wait_for(fetcher.run_until_idle(), 20)
    return fetcher


async def process_one(settings: Settings, conn: sqlite3.Connection, handler: Handler) -> None:
    """Claim and fetch at most one URL, bypassing the run loop."""
    with TmpArea(settings.tmp_dir, "one") as tmp:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for claim in claim_fetch_batch(conn, settings, "w", 1, utcnow()):
                await Fetcher(settings, conn, tmp, client, "w", random.Random(0)).process(claim)


def no_retries_pending(conn: sqlite3.Connection) -> None:
    """Make retried rows due again now, so run_until_idle can drive them to a final state."""
    with transaction(conn):
        conn.execute(
            "UPDATE frontier SET next_attempt_at = ? WHERE state = 'pending'", (to_db(utcnow()),)
        )
        conn.execute("UPDATE hosts SET backoff_until = NULL")


# --- outcomes -------------------------------------------------------------------------


async def test_success_stores_blob(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    body = b"<html><body><p>Hello</p></body></html>"
    seen_ua = []

    async def handler(req: httpx.Request) -> httpx.Response:
        seen_ua.append((req.headers["user-agent"], req.headers.get("from")))
        if req.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nAllow: /\n")
        return httpx.Response(200, headers=HTML, content=body)

    (uid,) = add_urls(conn, "https://news.example.com/a")
    await run(s, conn, handler)
    r = row(conn, uid)
    assert (r["state"], r["http_status"], r["raw_blob_hash"]) == (
        "fetched",
        200,
        ids.sha256_hex(body),
    )
    assert r["final_url"] == "https://news.example.com/a" and r["fetched_at"]
    assert r["attempts"] == 1 and r["lease_owner"] is None
    with TmpArea(s.tmp_dir, "read") as tmp:
        assert BlobStore(s.data_dir, tmp).read(r["raw_blob_hash"]) == body
    blob = conn.execute("SELECT * FROM blobs").fetchone()
    assert from_db(blob["expires_at"]) - from_db(blob["stored_at"]) == timedelta(days=60)
    ev = conn.execute("SELECT stage, status, attempt FROM stage_events").fetchall()
    assert [tuple(e) for e in ev] == [("fetch", "ok", 1)]
    assert set(seen_ua) == {(s.user_agent, s.contact_email)}
    host = conn.execute("SELECT * FROM hosts WHERE host = 'news.example.com'").fetchone()
    assert host["robots_status"] == 200 and host["consecutive_errors"] == 0


async def test_robots_disallow_skips(conn: sqlite3.Connection, data_dir: Path) -> None:
    handler = routes(
        {
            "e.com/robots.txt": httpx.Response(
                200, text="User-agent: newsrag\nDisallow: /private\nCrawl-delay: 3\n"
            ),
            "e.com/ok": httpx.Response(200, headers=HTML, content=b"<p>ok</p>"),
        }
    )
    blocked, ok = add_urls(conn, "https://e.com/private/x", "https://e.com/ok")
    await run(settings_for(data_dir), conn, handler)
    assert (row(conn, blocked)["state"], row(conn, blocked)["skip_reason"]) == ("skipped", "robots")
    assert row(conn, ok)["state"] == "fetched"
    host = conn.execute("SELECT crawl_delay_s FROM hosts WHERE host = 'e.com'").fetchone()
    assert host[0] == 3.0


async def test_robots_5xx_retries_and_backs_off_host(
    conn: sqlite3.Connection, data_dir: Path
) -> None:
    handler = routes({"e.com/robots.txt": httpx.Response(503)})
    (uid,) = add_urls(conn, "https://e.com/a")
    await process_one(settings_for(data_dir, max_attempts=2), conn, handler)
    r = row(conn, uid)
    assert (r["state"], r["last_error"]) == ("pending", "robots_unavailable")
    assert from_db(r["next_attempt_at"]) > utcnow() + timedelta(seconds=40)
    host = conn.execute("SELECT backoff_until FROM hosts WHERE host = 'e.com'").fetchone()
    assert from_db(host[0]) > utcnow()


@pytest.mark.parametrize(
    ("response", "state", "detail"),
    [
        (httpx.Response(404), "failed", "http_404"),
        (httpx.Response(410), "failed", "http_410"),
        (
            httpx.Response(200, headers={"content-type": "application/pdf"}),
            "skipped",
            "content_type",
        ),
        (
            httpx.Response(200, headers=HTML | {"content-length": "999999999"}),
            "skipped",
            "too_large",
        ),
        (httpx.Response(200, headers=HTML, content=b"x" * 2000), "skipped", "too_large"),
    ],
)
async def test_terminal_outcomes(
    conn: sqlite3.Connection, data_dir: Path, response: httpx.Response, state: str, detail: str
) -> None:
    (uid,) = add_urls(conn, "https://e.com/a")
    await run(settings_for(data_dir, max_body_bytes=1000), conn, routes({"e.com/a": response}))
    r = row(conn, uid)
    assert r["state"] == state
    assert detail in (r["last_error"], r["skip_reason"])
    assert conn.execute("SELECT count(*) FROM blobs").fetchone()[0] == 0


async def test_retry_after_and_host_backoff(conn: sqlite3.Connection, data_dir: Path) -> None:
    handler = routes({"e.com/a": httpx.Response(429, headers={"retry-after": "900"})})
    a, b = add_urls(conn, "https://e.com/a", "https://e.com/b")
    s = settings_for(data_dir)
    await process_one(s, conn, handler)
    r = row(conn, a)
    assert (r["state"], r["http_status"], r["last_error"]) == ("pending", 429, "http_429")
    assert from_db(r["next_attempt_at"]) >= utcnow() + timedelta(seconds=850)
    # The whole host is backing off, so b is not claimable even though it is due.
    assert claim_fetch_batch(conn, s, "w", 10, utcnow()) == []
    assert row(conn, b)["state"] == "pending"


async def test_other_4xx_retries_once(conn: sqlite3.Connection, data_dir: Path) -> None:
    (uid,) = add_urls(conn, "https://e.com/a")
    handler = routes({"e.com/a": httpx.Response(403)})
    s = settings_for(data_dir)
    await process_one(s, conn, handler)
    assert row(conn, uid)["state"] == "pending"
    no_retries_pending(conn)
    await process_one(s, conn, handler)
    r = row(conn, uid)
    assert (r["state"], r["attempts"], r["last_error"]) == ("failed", 2, "http_403")


async def test_network_errors_retry_until_max(conn: sqlite3.Connection, data_dir: Path) -> None:
    async def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=req)

    (uid,) = add_urls(conn, "https://e.com/a")
    s = settings_for(data_dir, max_attempts=3)
    for _ in range(3):
        no_retries_pending(conn)
        await process_one(s, conn, handler)
    r = row(conn, uid)
    # robots.txt is unreachable too, so the URL never gets past the robots check.
    assert (r["state"], r["attempts"], r["last_error"]) == ("failed", 3, "robots_unavailable")
    statuses = [x[0] for x in conn.execute("SELECT status FROM stage_events ORDER BY event_id")]
    assert statuses == ["retry", "retry", "error"]


# --- redirects ------------------------------------------------------------------------


async def test_redirect_chain(conn: sqlite3.Connection, data_dir: Path) -> None:
    handler = routes(
        {
            "e.com/a": httpx.Response(301, headers={"location": "/b?utm_source=x"}),
            "e.com/b": httpx.Response(302, headers={"location": "https://www.other.com/c"}),
            "www.other.com/c": httpx.Response(200, headers=HTML, content=b"<p>c</p>"),
        }
    )
    (uid,) = add_urls(conn, "https://e.com/a")
    await run(settings_for(data_dir), conn, handler)
    r = row(conn, uid)
    assert (r["state"], r["final_url"]) == ("fetched", "https://www.other.com/c")
    # robots.txt was checked on the redirect target's host too.
    hosts = {h[0] for h in conn.execute("SELECT host FROM hosts WHERE robots_status IS NOT NULL")}
    assert hosts == {"e.com", "www.other.com"}


async def test_redirect_into_disallowed_path(conn: sqlite3.Connection, data_dir: Path) -> None:
    handler = routes(
        {
            "e.com/a": httpx.Response(301, headers={"location": "https://x.com/private"}),
            "x.com/robots.txt": httpx.Response(200, text="User-agent: *\nDisallow: /private\n"),
        }
    )
    (uid,) = add_urls(conn, "https://e.com/a")
    await run(settings_for(data_dir), conn, handler)
    assert (row(conn, uid)["state"], row(conn, uid)["skip_reason"]) == ("skipped", "robots")


async def test_redirect_loop_fails(conn: sqlite3.Connection, data_dir: Path) -> None:
    handler = routes(
        {
            "e.com/a": httpx.Response(302, headers={"location": "/b"}),
            "e.com/b": httpx.Response(302, headers={"location": "/a"}),
        }
    )
    (uid,) = add_urls(conn, "https://e.com/a")
    await run(settings_for(data_dir), conn, handler)
    assert (row(conn, uid)["state"], row(conn, uid)["last_error"]) == (
        "failed",
        "too_many_redirects",
    )


# --- claiming and politeness ------------------------------------------------------------


def test_claim_one_per_host(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    add_urls(conn, "https://a.com/1", "https://a.com/2", "https://b.com/1")
    claims = claim_fetch_batch(conn, s, "w", 10, utcnow())
    assert sorted(c.host for c in claims) == ["a.com", "b.com"]
    # Both hosts are now reserved: nothing else is claimable until they are released.
    assert claim_fetch_batch(conn, s, "w", 10, utcnow()) == []
    later = utcnow() + timedelta(seconds=s.fetch.request_timeout_s * 2 + 31)
    assert [c.host for c in claim_fetch_batch(conn, s, "w", 10, later)] == ["a.com"]


async def test_lost_lease_records_nothing(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    (uid,) = add_urls(conn, "https://e.com/a")
    handler = routes({"e.com/a": httpx.Response(200, headers=HTML, content=b"<p>a</p>")})
    with TmpArea(s.tmp_dir, "t") as tmp:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            (claim,) = claim_fetch_batch(conn, s, "w", 1, utcnow())
            with transaction(conn):  # lease lapsed and another fetcher took the row
                conn.execute("UPDATE frontier SET lease_owner = 'other'")
            await Fetcher(s, conn, tmp, c, "w").process(claim)
        assert list(tmp.dir.iterdir()) == []
    assert row(conn, uid)["state"] == "fetching"
    assert conn.execute("SELECT count(*) FROM blobs").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM stage_events").fetchone()[0] == 0


async def test_one_request_per_host_with_crawl_delay(
    conn: sqlite3.Connection, data_dir: Path
) -> None:
    delay = 0.3
    log: list[tuple[str, float, float]] = []
    in_flight: dict[str, int] = {}
    max_in_flight: dict[str, int] = {}

    async def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/robots.txt":
            return httpx.Response(404)
        host = req.url.host
        in_flight[host] = in_flight.get(host, 0) + 1
        max_in_flight[host] = max(max_in_flight.get(host, 0), in_flight[host])
        start = time.monotonic()
        await asyncio.sleep(0.05)
        in_flight[host] -= 1
        log.append((host, start, time.monotonic()))
        return httpx.Response(200, headers=HTML, content=f"<p>{req.url}</p>".encode())

    add_urls(conn, *[f"https://{h}.com/{i}" for h in ("a", "b") for i in range(3)])
    await run(settings_for(data_dir, default_crawl_delay_s=delay), conn, handler)

    assert max_in_flight == {"a.com": 1, "b.com": 1}
    for host in ("a.com", "b.com"):
        times = sorted((s, e) for h, s, e in log if h == host)
        assert len(times) == 3
        gaps = [nxt[0] - prev[1] for prev, nxt in zip(times, times[1:], strict=False)]
        assert min(gaps) >= delay * 0.9, gaps
    # Different hosts are fetched in parallel.
    a0 = min(s for h, s, _ in log if h == "a.com")
    b0 = min(s for h, s, _ in log if h == "b.com")
    assert abs(a0 - b0) < delay


# --- real sockets -----------------------------------------------------------------------


class _Server(BaseHTTPRequestHandler):
    page = b"<html><body>" + b"<p>Real server paragraph.</p>" * 200 + b"</body></html>"

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        if self.path == "/robots.txt":
            body = b"User-agent: *\nDisallow: /nope\n"
            self.send_response(200)
            self.send_header("content-type", "text/plain")
        elif self.path == "/article":
            body = gzip.compress(self.page)
            self.send_response(200)
            self.send_header("content-type", "text/html; charset=utf-8")
            self.send_header("content-encoding", "gzip")
        else:
            body = b""
            self.send_response(404)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        pass


async def test_real_http_server_gzip(conn: sqlite3.Connection, data_dir: Path) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Server)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        ok, blocked = add_urls(conn, f"{base}/article", f"{base}/nope")
        s = settings_for(data_dir)
        with TmpArea(s.tmp_dir, "real") as tmp:
            async with make_client(s) as client:
                f = Fetcher(s, conn, tmp, client, "w")
                await asyncio.wait_for(f.run_until_idle(), 20)
            assert BlobStore(s.data_dir, tmp).read(row(conn, ok)["raw_blob_hash"]) == _Server.page
        assert row(conn, ok)["state"] == "fetched"
        assert row(conn, blocked)["skip_reason"] == "robots"
    finally:
        server.shutdown()


async def test_make_client_identifies_us(data_dir: Path) -> None:
    s = settings_for(data_dir)
    async with make_client(s) as client:
        assert client.headers["user-agent"] == s.user_agent
        assert client.headers["from"] == s.contact_email
        assert client.follow_redirects is False  # redirects go through robots checks
