"""Compaction, retention, reports and the re-extraction check (P1.6)."""

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from tests.test_contracts import make_doc

from newsrag import ids
from newsrag.config import Settings
from newsrag.contracts.v1 import Document
from newsrag.extract.extractor import ExtractContext, extract
from newsrag.obs.reports import disk_report, lag_report, percentile
from newsrag.storage.blobs import BlobStore
from newsrag.storage.compaction import compact, plan
from newsrag.storage.fsio import TmpArea
from newsrag.storage.recovery import verify_storage
from newsrag.storage.reextract import ReextractReport, _compare
from newsrag.storage.retention import run_retention
from newsrag.storage.state import connect, init_schema, transaction
from newsrag.storage.tables import (
    DOCUMENTS,
    committed_files,
    install_part,
    read_documents,
    stage_documents_part,
)
from newsrag.timeutil import to_db, utcnow

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


def doc(i: int, ingested: datetime) -> Document:
    url = f"https://example.com/news/{i}"
    text = f"Story number {i}.\n\nSecond paragraph of story {i}."
    return make_doc(
        doc_id=ids.doc_id(url),
        canonical_url=url,
        final_url=url,
        extracted_text=text,
        content_hash=ids.content_hash("A story", text),
        discovered_at=ingested - timedelta(minutes=5),
        fetched_at=ingested - timedelta(minutes=1),
        ingested_at=ingested,
        published_at=ingested - timedelta(hours=2),
    )


def hot_part(conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea, docs: list[Document]) -> None:
    """Commit one hot part the way the writer does."""
    part = stage_documents_part(tmp, docs)
    with transaction(conn):
        install_part(conn, data_dir, part, utcnow())
        conn.executemany(
            "INSERT INTO doc_revisions (doc_id, revision, content_hash, raw_blob_hash, "
            "canonical_url, source_id, content_kind, file_id, published_at, ingested_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    d.doc_id,
                    d.revision,
                    d.content_hash,
                    d.raw_blob_hash,
                    d.canonical_url,
                    d.source_id,
                    d.content_kind.value,
                    part.file_id,
                    to_db(d.published_at) if d.published_at else None,
                    to_db(d.ingested_at),
                )
                for d in docs
            ],
        )


# --- compaction -------------------------------------------------------------------------


def test_compaction_merges_by_month_including_a_straddling_part(
    conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea
) -> None:
    sept = datetime(2026, 9, 30, 23, 0, tzinfo=UTC)
    octo = datetime(2026, 10, 1, 1, 0, tzinfo=UTC)
    hot_part(conn, data_dir, tmp, [doc(0, sept), doc(1, sept)])
    hot_part(conn, data_dir, tmp, [doc(2, sept), doc(3, octo)])  # spans the month boundary
    hot_part(conn, data_dir, tmp, [doc(4, octo), doc(5, octo)])
    before = sorted(
        read_documents(committed_files(conn, data_dir, DOCUMENTS)), key=lambda d: d.doc_id
    )
    old_paths = committed_files(conn, data_dir, DOCUMENTS)

    report = compact(conn, data_dir, tmp, utcnow())
    assert (report.months, report.inputs, report.outputs, report.rows) == (2, 3, 2, 6)

    files = committed_files(conn, data_dir, DOCUMENTS)
    assert sorted(
        f.parent.relative_to(data_dir / "tables" / DOCUMENTS).as_posix() for f in files
    ) == [
        "2026/09",
        "2026/10",
    ]
    after = sorted(read_documents(files), key=lambda d: d.doc_id)
    assert after == before  # same rows, no duplicates, nothing lost
    assert not any(p.exists() for p in old_paths)  # superseded files deleted
    assert verify_storage(conn, data_dir) == []
    states = dict(conn.execute("SELECT state, count(*) FROM table_files GROUP BY 1").fetchall())
    assert states == {"committed": 2, "superseded": 3}
    assert plan(conn, 128 * 1024 * 1024) == {}  # one part per month: nothing to do


def test_small_monthly_parts_merge_again(
    conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea
) -> None:
    for i in range(3):
        hot_part(conn, data_dir, tmp, [doc(i, T0)])
        compact(conn, data_dir, tmp, utcnow())
    # Each run merged the new hot part with the month's small part from the run before.
    assert len(committed_files(conn, data_dir, DOCUMENTS)) == 1
    assert len(read_documents(committed_files(conn, data_dir, DOCUMENTS))) == 3
    assert verify_storage(conn, data_dir) == []


def test_tiny_target_splits_output(conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea) -> None:
    for i in range(4):
        hot_part(conn, data_dir, tmp, [doc(i, T0)])
    report = compact(conn, data_dir, tmp, utcnow(), target_bytes=1)
    assert report.outputs == 4  # every input part fills a target
    assert verify_storage(conn, data_dir) == []


# --- retention ----------------------------------------------------------------------------


def test_retention(conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea) -> None:
    s = Settings(data_dir=data_dir)
    store = BlobStore(data_dir, tmp)
    for body, exp in ((b"old", T0 - timedelta(days=1)), (b"new", T0 + timedelta(days=30))):
        staged = store.stage(body)
        with transaction(conn):
            store.commit(conn, staged, T0, exp)
    fid = "f" * 32
    with transaction(conn):
        conn.execute(
            "INSERT INTO sources (source_id, country, created_at) VALUES ('e.com', 'US', ?)",
            (to_db(T0),),
        )
        conn.execute(
            "INSERT INTO feeds (feed_id, feed_url, source_id, categories_json, medium, origin, "
            "created_at, updated_at) VALUES (?, 'https://e.com/rss', 'e.com', '[]', 'text', "
            "'manual', ?, ?)",
            (fid, to_db(T0), to_db(T0)),
        )
        for days in (1, 20):
            conn.execute(
                "INSERT INTO feed_polls (feed_id, polled_at) VALUES (?, ?)",
                (fid, to_db(T0 - timedelta(days=days))),
            )
    report = run_retention(conn, s, T0)
    assert (report.blobs_expired, report.polls_pruned) == (1, 1)
    assert not store.path(ids.sha256_hex(b"old")).exists()
    assert store.read(ids.sha256_hex(b"new")) == b"new"


# --- reports -------------------------------------------------------------------------------


def test_percentile_nearest_rank() -> None:
    values = list(range(1, 101))
    assert (percentile(values, 50), percentile(values, 95), percentile(values, 99)) == (50, 95, 99)
    assert percentile([], 50) is None
    assert percentile([7.0], 99) == 7.0


def test_reextract_compare_flags_mismatches() -> None:
    page = (Path(__file__).parent / "fixtures/synthetic/articles/meta_date.html").read_bytes()
    url = "https://news.example/meta_date"
    ctx = ExtractContext(
        final_url=url,
        source_id="news.example",
        discovered_at=T0,
        fetched_at=T0,
        pipeline_version="t",
    )
    real = extract(page, ctx)
    assert isinstance(real, Document)
    tampered = real.model_copy(update={"content_hash": "0" * 64})
    report = ReextractReport()
    _compare([(real, page, ctx), (tampered, page, ctx)], None, report)
    assert (report.compared, report.matched) == (2, 1)
    assert report.mismatches == [f"{real.doc_id}:{real.revision}"]


def test_disk_report_on_empty_archive(conn: sqlite3.Connection, data_dir: Path) -> None:
    r = disk_report(conn, Settings(data_dir=data_dir), utcnow())
    assert r.blob_count == 0 and r.parquet_rows == 0 and r.fits_12mo
    assert r.sqlite_bytes > 0 and r.fs_free > 0


def test_lag_report_empty(conn: sqlite3.Connection) -> None:
    r = lag_report(conn, utcnow(), timedelta(hours=24))
    assert r.groups == {} and set(r.stages) == {"discover", "fetch", "extract", "store"}
