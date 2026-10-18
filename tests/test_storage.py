import os
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from tests.test_contracts import make_doc

from newsrag import ids
from newsrag.contracts.v1 import Document
from newsrag.storage.blobs import BlobStore, blob_rel_path, expire_blobs, sweep_blobs
from newsrag.storage.fsio import LockHeld, ProcessLock, TmpArea, sweep_tmp
from newsrag.storage.recovery import (
    reclaim_expired_leases,
    recover_writer,
    remove_uncommitted_table_files,
    verify_storage,
)
from newsrag.storage.state import connect, init_schema, transaction
from newsrag.storage.tables import (
    DOCUMENT_SCHEMA,
    DOCUMENTS,
    committed_files,
    install_part,
    read_documents,
    stage_documents_part,
)
from newsrag.storage.writer import ExtractedDoc, StorageWriter, claim_for_extraction
from newsrag.timeutil import to_db

T0 = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def conn(data_dir: Path) -> Iterator[sqlite3.Connection]:
    c = connect(data_dir / "state" / "crawl.sqlite3")
    init_schema(c)
    yield c
    c.close()


@pytest.fixture
def tmp(data_dir: Path) -> Iterator[TmpArea]:
    with TmpArea(data_dir / "tmp", "test") as area:
        yield area


def doc_for(url: str, text: str, **kw: Any) -> Document:
    return make_doc(
        doc_id=ids.doc_id(url),
        canonical_url=url,
        final_url=url,
        extracted_text=text,
        content_hash=ids.content_hash("A story", text),
        **kw,
    )


def extracted(doc: Document, url_id: str | None = None) -> ExtractedDoc:
    return ExtractedDoc(doc, url_id, T0, T0, T0)


# --- fsio -----------------------------------------------------------------------------


def test_tmp_area_sweep_spares_live_and_removes_dead(data_dir: Path) -> None:
    root = data_dir / "tmp"
    with TmpArea(root, "live") as live:
        live.new_path(".x").write_bytes(b"in flight")
        # A dead process's area: directory left behind, lock file not held.
        (root / "dead-1-X").mkdir()
        (root / "dead-1-X" / "f").write_bytes(b"")
        (root / "dead-1-X.lock").touch()
        (root / "stray.bin").write_bytes(b"")
        assert sweep_tmp(root) == 2
        assert live.dir.exists() and any(live.dir.iterdir())
        assert sorted(p.name for p in root.iterdir()) == sorted([live.name, f"{live.name}.lock"])
    assert list(root.iterdir()) == []


def test_process_lock_is_exclusive(tmp_path: Path) -> None:
    path = tmp_path / "locks" / "writer.lock"
    # flock locks belong to the open file description, so a second open in the same
    # process conflicts too, which is what lets us test this in-process.
    with ProcessLock(path), pytest.raises(LockHeld):
        ProcessLock(path).acquire()
    ProcessLock(path).acquire().release()


# --- blobs ----------------------------------------------------------------------------


def test_blob_roundtrip_and_idempotent_commit(
    conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea
) -> None:
    store = BlobStore(data_dir, tmp)
    body = b"<html>hello</html>" * 100
    for expires in (T0 + timedelta(days=60), T0 + timedelta(days=90), T0 + timedelta(days=1)):
        staged = store.stage(body, "text/html")
        with transaction(conn):
            store.commit(conn, staged, T0, expires)
    h = ids.sha256_hex(body)
    assert store.read(h) == body
    row = conn.execute("SELECT * FROM blobs WHERE blob_hash = ?", (h,)).fetchone()
    assert row["state"] == "present"
    assert row["expires_at"] == to_db(T0 + timedelta(days=90))  # expiry only moves later
    assert row["rel_path"] == blob_rel_path(h) and row["size_raw"] == len(body)
    assert list(tmp.dir.iterdir()) == []


def test_blob_commit_requires_transaction(
    conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea
) -> None:
    store = BlobStore(data_dir, tmp)
    with pytest.raises(RuntimeError):
        store.commit(conn, store.stage(b"x"), T0, None)


def test_expire_then_refetch_and_sweep(
    conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea
) -> None:
    store = BlobStore(data_dir, tmp)
    old, keep, forever = b"old page", b"fresh page", b"golden page"
    for body, exp in ((old, T0), (keep, T0 + timedelta(days=60)), (forever, None)):
        staged = store.stage(body)
        with transaction(conn):
            store.commit(conn, staged, T0, exp)
    assert expire_blobs(conn, data_dir, T0 + timedelta(days=1)) == 1
    assert not store.path(ids.sha256_hex(old)).exists()
    assert store.path(ids.sha256_hex(keep)).exists()
    assert store.path(ids.sha256_hex(forever)).exists()

    # Re-fetching an expired body revives it.
    staged = store.stage(old)
    with transaction(conn):
        store.commit(conn, staged, T0, T0 + timedelta(days=60))
    assert store.read(ids.sha256_hex(old)) == old

    # An orphan (crash after install, before commit) is swept; present blobs are not.
    orphan = store.path("ab" * 32)
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"")
    assert sweep_blobs(conn, data_dir) == 1
    assert not orphan.exists()
    assert verify_storage(conn, data_dir) == []


# --- tables ---------------------------------------------------------------------------


def test_arrow_schema_matches_document_model() -> None:
    assert DOCUMENT_SCHEMA.names == list(Document.model_fields)


def test_part_roundtrip(conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea) -> None:
    docs = [
        make_doc(),
        doc_for(
            "https://example.com/b",
            "Only one paragraph, with ünïcödé.",
            title=None,
            published_at=None,
            published_at_source="none",
            pipeline_meta={"b": 1, "a": [1, 2]},
            authors=["A. Writer"],
        ),
    ]
    part = stage_documents_part(tmp, docs)
    with transaction(conn):
        install_part(conn, data_dir, part, T0)
    files = committed_files(conn, data_dir, DOCUMENTS)
    assert [f.name for f in files] == [f"part-{part.file_id}.parquet"]
    assert read_documents(files) == docs


# --- writer ---------------------------------------------------------------------------


def test_writer_revisions(conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea) -> None:
    writer = StorageWriter(conn, data_dir, tmp, "test:writer-1:0", "test-0")
    url = "https://example.com/news/1"
    v1, v2 = doc_for(url, "Version one."), doc_for(url, "Version two.")
    other = doc_for("https://example.com/news/2", "Version one.")  # same text, other doc_id

    r1 = writer.commit([extracted(v1), extracted(v1), extracted(other)])
    assert r1.new_revisions == [(v1.doc_id, 1), (other.doc_id, 1)]
    assert r1.unchanged == [(v1.doc_id, 1)]  # duplicate within the batch

    r2 = writer.commit([extracted(v2), extracted(v1)])
    assert r2.new_revisions == [(v1.doc_id, 2)]
    assert r2.unchanged == [(v1.doc_id, 1)]

    r3 = writer.commit([extracted(v1)])  # nothing new: no part is written
    assert r3.file_id is None

    stored = read_documents(committed_files(conn, data_dir, DOCUMENTS))
    assert sorted((d.doc_id, d.revision, d.extracted_text) for d in stored) == sorted(
        [
            (v1.doc_id, 1, "Version one."),
            (v1.doc_id, 2, "Version two."),
            (other.doc_id, 1, "Version one."),
        ]
    )
    assert all(d.ingested_at >= d.fetched_at for d in stored)
    assert verify_storage(conn, data_dir) == []
    events = conn.execute("SELECT stage, status, count(*) FROM stage_events GROUP BY 1, 2")
    assert {(r[0], r[1]): r[2] for r in events} == {
        ("extract", "ok"): 6,
        ("store", "ok"): 3,
        ("store", "skipped"): 3,
    }


def _insert_frontier(conn: sqlite3.Connection, url: str, state: str, **cols: Any) -> str:
    uid = ids.url_id(url)
    now = to_db(T0)
    base = {
        "url_id": uid,
        "url": url,
        "host": "example.com",
        "source_id": "example.com",
        "discovered_at": now,
        "state": state,
        "ready_at": now,
        "next_attempt_at": now,
        "updated_at": now,
        **cols,
    }
    with transaction(conn):
        conn.execute(
            f"INSERT INTO frontier ({', '.join(base)}) VALUES ({', '.join('?' * len(base))})",
            list(base.values()),
        )
    return uid


def test_claim_commit_updates_frontier(
    conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea
) -> None:
    url = "https://example.com/news/story-1"
    uid = _insert_frontier(conn, url, "fetched", raw_blob_hash="0" * 64)
    claims = claim_for_extraction(conn, "w", 10, 300, now=T0)
    assert [c.url_id for c in claims] == [uid]
    assert conn.execute("SELECT state FROM frontier").fetchone()[0] == "extracting"

    StorageWriter(conn, data_dir, tmp, "w", "test-0").commit([extracted(make_doc(), uid)])
    row = conn.execute("SELECT * FROM frontier WHERE url_id = ?", (uid,)).fetchone()
    assert (row["state"], row["doc_id"], row["revision"]) == ("stored", ids.doc_id(url), 1)
    assert row["lease_owner"] is None


# --- recovery -------------------------------------------------------------------------


def test_reclaim_only_expired_leases(conn: sqlite3.Connection) -> None:
    lapsed = to_db(T0 - timedelta(seconds=1))
    live = to_db(T0 + timedelta(minutes=5))
    a = _insert_frontier(conn, "https://e.com/a", "extracting", lease_expires_at=lapsed)
    b = _insert_frontier(conn, "https://e.com/b", "extracting", lease_expires_at=live)
    c = _insert_frontier(conn, "https://e.com/c", "fetching", lease_expires_at=lapsed)
    assert reclaim_expired_leases(conn, "extracting", T0) == 1
    states = dict(conn.execute("SELECT url_id, state FROM frontier").fetchall())
    assert states == {a: "fetched", b: "extracting", c: "fetching"}


def test_recover_removes_uncommitted_parts(
    conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea
) -> None:
    StorageWriter(conn, data_dir, tmp, "w", "test-0").commit([extracted(make_doc())])
    # Simulate a crash after install but before commit: a part with no manifest row.
    crashed = stage_documents_part(tmp, [doc_for("https://e.com/z", "Lost.")])
    dest = data_dir / crashed.rel_path
    os.replace(crashed.tmp_path, dest)
    report = recover_writer(conn, data_dir, T0)
    assert report.table_files_removed == 1 and not dest.exists()
    assert len(committed_files(conn, data_dir, DOCUMENTS)) == 1
    assert remove_uncommitted_table_files(conn, data_dir) == 0
    assert verify_storage(conn, data_dir) == []


def test_verify_detects_corruption(conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea) -> None:
    StorageWriter(conn, data_dir, tmp, "w", "test-0").commit([extracted(make_doc())])
    (path,) = committed_files(conn, data_dir, DOCUMENTS)
    path.write_bytes(path.read_bytes()[:-10])
    assert any("sha256 mismatch" in p for p in verify_storage(conn, data_dir))
    path.unlink()
    assert any("missing" in p for p in verify_storage(conn, data_dir))
