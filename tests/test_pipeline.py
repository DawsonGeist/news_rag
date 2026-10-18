"""Fetch -> extract -> store, end to end over the synthetic fixtures, plus the writer
loop's failure handling (hung and crashed extractor workers, missing blobs)."""

import asyncio
import random
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from newsrag import ids
from newsrag.config import FetchSettings, Settings, WriterSettings
from newsrag.contracts.v1 import ContentKind
from newsrag.crawl.fetcher import Fetcher
from newsrag.crawl.http import host_of
from newsrag.storage.blobs import BlobStore
from newsrag.storage.fsio import TmpArea
from newsrag.storage.recovery import verify_storage
from newsrag.storage.state import connect, init_schema, transaction
from newsrag.storage.tables import DOCUMENTS, committed_files, read_documents
from newsrag.storage.writer_loop import WriterLoop
from newsrag.testing.jobs import CRASH_MARKER, HANG_MARKER, faulty_extract
from newsrag.timeutil import to_db, utcnow

FIXTURES = Path(__file__).parent / "fixtures" / "synthetic" / "articles"
SPEC = yaml.safe_load((FIXTURES / "expected.yaml").read_text(encoding="utf-8"))
HOST = "news.example"


def settings_for(data_dir: Path, **writer: Any) -> Settings:
    w: dict[str, Any] = {"extract_workers": 2, "flush_rows": 500, "flush_seconds": 0.0} | writer
    return Settings(
        data_dir=data_dir,
        fetch=FetchSettings(default_crawl_delay_s=0.0, request_timeout_s=5.0),
        writer=WriterSettings(**w),
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


def discover(conn: sqlite3.Connection, url: str, **hints: Any) -> str:
    """Stand-in for the poller (P1.4): insert one pending frontier row."""
    now = to_db(utcnow())
    cols = {
        "url_id": ids.url_id(url),
        "url": ids.normalize_url(url),
        "host": host_of(url),
        "source_id": HOST,
        "discovered_at": now,
        "state": "pending",
        "ready_at": now,
        "next_attempt_at": now,
        "updated_at": now,
        **hints,
    }
    with transaction(conn):
        conn.execute(
            f"INSERT INTO frontier ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            list(cols.values()),
        )
    return str(cols["url_id"])


def serve(pages: dict[str, bytes]) -> httpx.MockTransport:
    async def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path in pages:
            return httpx.Response(
                200, headers={"content-type": "text/html"}, content=pages[req.url.path]
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def fetch_all(settings: Settings, conn: sqlite3.Connection, pages: dict[str, bytes]) -> None:
    async def go() -> None:
        with TmpArea(settings.tmp_dir, "fetch") as tmp:
            async with httpx.AsyncClient(transport=serve(pages)) as client:
                f = Fetcher(settings, conn, tmp, client, "test:fetch", random.Random(0))
                await asyncio.wait_for(f.run_until_idle(), 30)

    asyncio.run(go())


def store_all(settings: Settings, conn: sqlite3.Connection, **kw: Any) -> None:
    with TmpArea(settings.tmp_dir, "writer") as tmp:
        loop = WriterLoop(settings, conn, tmp, "test:writer", **kw)
        try:
            loop.run_until_idle()
        finally:
            loop.close()


def states(conn: sqlite3.Connection) -> dict[str, tuple[str, str | None]]:
    return {
        r["url"]: (r["state"], r["last_error"])
        for r in conn.execute("SELECT url, state, last_error FROM frontier")
    }


def test_fetch_extract_store_end_to_end(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    pages = {f"/{p.stem}": p.read_bytes() for p in FIXTURES.glob("*.html")}
    summary = SPEC["cases"]["summary_only"]["context"]["feed_summary"]
    for path in pages:
        hints = {"feed_summary": summary} if path == "/summary_only" else {}
        discover(conn, f"https://{HOST}{path}", **hints)

    fetch_all(s, conn, pages)
    store_all(s, conn)

    assert {st for st, _ in states(conn).values()} == {"stored"}
    docs = read_documents(committed_files(conn, data_dir, DOCUMENTS))
    by_url = {d.final_url.rsplit("/", 1)[1]: d for d in docs}
    assert set(by_url) == set(SPEC["cases"])
    for name, case in SPEC["cases"].items():
        if "content_kind" in case["expect"]:
            assert by_url[name].content_kind.value == case["expect"]["content_kind"], name
    assert by_url["summary_only"].content_kind is ContentKind.SUMMARY_ONLY
    assert all(d.revision == 1 and d.source_id == HOST for d in docs)
    assert verify_storage(conn, data_dir) == []
    events = conn.execute("SELECT stage, status, count(*) FROM stage_events GROUP BY 1, 2")
    n = len(pages)
    assert {(r[0], r[1]): r[2] for r in events} == {
        ("fetch", "ok"): n,
        ("extract", "ok"): n,
        ("store", "ok"): n,
    }


def test_refetch_with_new_text_is_a_new_revision(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    body = (FIXTURES / "meta_date.html").read_bytes()
    discover(conn, f"https://{HOST}/story")
    fetch_all(s, conn, {"/story": body})
    store_all(s, conn)
    # A feed reports an update (P1.4 re-queues it); the page now has an extra paragraph.
    with transaction(conn):
        conn.execute(
            "UPDATE frontier SET state = 'pending', next_attempt_at = ?, attempts = 0",
            (to_db(utcnow()),),
        )
        conn.execute("UPDATE hosts SET next_allowed_at = NULL")
    edited = body.replace(
        b"</div>", b"<p>Update: the cleanup raised four thousand dollars.</p></div>"
    )
    fetch_all(s, conn, {"/story": edited})
    store_all(s, conn)
    docs = sorted(
        read_documents(committed_files(conn, data_dir, DOCUMENTS)), key=lambda d: d.revision
    )
    assert [d.revision for d in docs] == [1, 2]
    assert docs[0].doc_id == docs[1].doc_id
    assert "four thousand dollars" in docs[1].extracted_text
    row = conn.execute("SELECT state, revision FROM frontier").fetchone()
    assert tuple(row) == ("stored", 2)


def _fetched_blob(conn: sqlite3.Connection, s: Settings, url: str, body: bytes) -> str:
    """Put a blob in the store and a 'fetched' frontier row pointing at it."""
    uid = discover(conn, url)
    with TmpArea(s.tmp_dir, "seed") as tmp:
        store = BlobStore(s.data_dir, tmp)
        staged = store.stage(body)
        with transaction(conn):
            now = utcnow()
            store.commit(conn, staged, now, None)
            conn.execute(
                "UPDATE frontier SET state = 'fetched', final_url = url, raw_blob_hash = ?, "
                "fetched_at = ?, content_type = 'text/html' WHERE url_id = ?",
                (staged.blob_hash, to_db(now), uid),
            )
    return uid


def test_hung_and_crashed_workers(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir, extract_timeout_s=1.5)
    good = (FIXTURES / "meta_date.html").read_bytes()
    _fetched_blob(conn, s, f"https://{HOST}/hang", HANG_MARKER + good)
    _fetched_blob(conn, s, f"https://{HOST}/crash", CRASH_MARKER + good)
    for i in range(4):
        _fetched_blob(
            conn, s, f"https://{HOST}/ok{i}", good.replace(b"Saturday", f"day {i}".encode())
        )

    started = datetime.now(UTC)
    store_all(s, conn, job=faulty_extract)
    assert (datetime.now(UTC) - started).total_seconds() < 30

    st = states(conn)
    assert st[f"https://{HOST}/hang"] == ("fetched", "extract_timeout")  # retried later
    # A crash is caught as a broken pool, or (when CPython misses the dead worker) as
    # a job that started and never finished. Either way it is charged to this page.
    assert st[f"https://{HOST}/crash"][0] == "fetched"
    assert st[f"https://{HOST}/crash"][1] in ("worker_crashed", "extract_timeout")
    # Healthy pages are stored, and never charged for their neighbours' faults.
    assert all(st[f"https://{HOST}/ok{i}"] == ("stored", None) for i in range(4))
    charged = conn.execute(
        "SELECT f.url FROM stage_events e JOIN frontier f USING (url_id) "
        "WHERE e.stage = 'extract' AND e.status != 'ok'"
    ).fetchall()
    assert sorted(r[0] for r in charged) == [f"https://{HOST}/crash", f"https://{HOST}/hang"]
    assert verify_storage(conn, data_dir) == []


def test_transient_failures_give_up_after_max_attempts(
    conn: sqlite3.Connection, data_dir: Path
) -> None:
    s = settings_for(data_dir, extract_timeout_s=0.5, extract_max_attempts=2)
    uid = _fetched_blob(conn, s, f"https://{HOST}/hang", HANG_MARKER + b"<p>x</p>")
    for _ in range(2):
        with transaction(conn):
            conn.execute("UPDATE frontier SET next_attempt_at = ?", (to_db(utcnow()),))
        store_all(s, conn, job=faulty_extract)
    row = conn.execute("SELECT state, last_error FROM frontier WHERE url_id = ?", (uid,)).fetchone()
    assert tuple(row) == ("failed", "extract_timeout")


def test_missing_blob_and_unparseable_fail(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir)
    _fetched_blob(conn, s, f"https://{HOST}/empty", b"")
    gone = _fetched_blob(conn, s, f"https://{HOST}/gone", b"<p>soon expired</p>")
    with transaction(conn):
        conn.execute(
            "UPDATE blobs SET state = 'expired' WHERE blob_hash = "
            "(SELECT raw_blob_hash FROM frontier WHERE url_id = ?)",
            (gone,),
        )
    store_all(s, conn)
    st = states(conn)
    assert st[f"https://{HOST}/empty"] == ("failed", "unparseable")
    assert st[f"https://{HOST}/gone"] == ("failed", "blob_missing")
    errors = conn.execute("SELECT count(*) FROM stage_events WHERE status = 'error'").fetchone()
    assert errors[0] == 2


def test_crash_is_quarantined_and_charged_alone(conn: sqlite3.Connection, data_dir: Path) -> None:
    s = settings_for(data_dir, extract_workers=3)
    good = (FIXTURES / "meta_date.html").read_bytes()
    _fetched_blob(conn, s, f"https://{HOST}/crash", CRASH_MARKER + good)
    for i in range(5):
        _fetched_blob(
            conn, s, f"https://{HOST}/ok{i}", good.replace(b"Saturday", f"day {i}".encode())
        )
    store_all(s, conn, job=faulty_extract)
    st = states(conn)
    assert st[f"https://{HOST}/crash"] == ("fetched", "worker_crashed")
    assert all(st[f"https://{HOST}/ok{i}"] == ("stored", None) for i in range(5))
    events = conn.execute(
        "SELECT status, count(*) FROM stage_events WHERE stage = 'extract' GROUP BY 1"
    )
    assert dict(events.fetchall()) == {"ok": 5, "retry": 1}
