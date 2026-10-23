"""Enricher (P2.3): micro-batches from stored revisions to segments and vectors, crash
safety and reconcile. Uses Qdrant's in-process mode and a deterministic fake embedder."""

import hashlib
import sqlite3
from collections.abc import Iterator, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import pytest
from qdrant_client import QdrantClient
from tests.test_contracts import T0
from tests.test_storage import doc_for

from newsrag.config import EnricherSettings, Settings
from newsrag.contracts.v1 import ContentKind, Document, Segment, SegmentLevel
from newsrag.enrich import enricher as enricher_mod
from newsrag.enrich.embedder import Vectors
from newsrag.enrich.enricher import (
    PIPELINE_VERSION,
    Enricher,
    reconcile,
    recover_enricher,
)
from newsrag.enrich.state import claim, queue_new
from newsrag.enrich.tables import (
    SEGMENT_SCHEMA,
    SEGMENTS,
    segments_to_table,
    table_to_segments,
)
from newsrag.enrich.vectors import VectorIndex, point_id
from newsrag.segment.segmenter import segment
from newsrag.storage.compaction import compact_derived
from newsrag.storage.fsio import TmpArea
from newsrag.storage.recovery import verify_storage
from newsrag.storage.state import connect, init_schema
from newsrag.storage.tables import committed_files
from newsrag.storage.writer import ExtractedDoc, StorageWriter
from newsrag.timeutil import utcnow

DIMS = 8
PARAS = [
    "The council voted on Tuesday to approve the new budget. Members argued for hours.",
    "Critics said the plan cuts too deep into school funding. Supporters disagreed.",
    "The mayor is expected to sign the measure next week, her office said.",
]


class FakeEmbedder:
    model_id = "fake@0"
    dims = DIMS

    def __init__(self) -> None:
        self.calls = 0

    def encode(self, texts: Sequence[str]) -> Vectors:
        self.calls += 1
        rows = []
        for t in texts:
            h = hashlib.sha256(t.encode()).digest()
            v = np.frombuffer(h[:DIMS], dtype=np.uint8).astype(np.float32) + 1
            rows.append(v / np.linalg.norm(v))
        return np.array(rows, dtype=np.float32).reshape(len(texts), DIMS)


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
def settings(data_dir: Path) -> Settings:
    return Settings(data_dir=data_dir, enricher=EnricherSettings(batch_docs=50))


@pytest.fixture
def index() -> VectorIndex:
    idx = VectorIndex(QdrantClient(location=":memory:"), "passages_test", DIMS)
    idx.ensure_collection()
    return idx


@pytest.fixture
def tmp(data_dir: Path) -> Iterator[TmpArea]:
    with TmpArea(data_dir / "tmp", "test-enricher") as area:
        yield area


def store(conn: sqlite3.Connection, data_dir: Path, tmp: TmpArea, *docs: Document) -> None:
    writer = StorageWriter(conn, data_dir, tmp, "test-writer", "test")
    writer.commit([ExtractedDoc(d, None, T0, T0, T0) for d in docs])


def article(n: int, paras: list[str] = PARAS, **kw: Any) -> Document:
    return doc_for(f"https://example.com/news/{n}", "\n\n".join(paras), **kw)


def make(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex
) -> Enricher:
    return Enricher(settings, conn, tmp, FakeEmbedder(), index, "test-enricher")


def states(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        r["doc_id"] + f"@{r['revision']}": r["state"]
        for r in conn.execute("SELECT doc_id, revision, state FROM enrichments")
    }


def segment_rows(conn: sqlite3.Connection, data_dir: Path) -> list[Segment]:
    out: list[Segment] = []
    for p in committed_files(conn, data_dir, SEGMENTS):
        out.extend(table_to_segments(pq.read_table(p, schema=SEGMENT_SCHEMA)))
    return out


def embedded(doc: Document) -> list[Segment]:
    return [s for s in segment(doc, PIPELINE_VERSION) if s.level in enricher_mod.EMBED_LEVELS]


def test_segment_schema_matches_model() -> None:
    assert SEGMENT_SCHEMA.names == list(Segment.model_fields)
    segs = segment(article(1), PIPELINE_VERSION)
    assert table_to_segments(segments_to_table(segs)) == segs


def test_enrich_end_to_end(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex, data_dir: Path
) -> None:
    good = article(1)
    other = article(2, content_kind=ContentKind.NON_ARTICLE)
    german = article(3, language="de")
    store(conn, data_dir, tmp, good, other, german)
    make(settings, conn, tmp, index).run_until_idle()

    rows = {
        r["doc_id"]: dict(r)
        for r in conn.execute(
            "SELECT doc_id, state, last_error, segments, vectors FROM enrichments"
        )
    }
    assert rows[good.doc_id]["state"] == "done"
    assert (rows[other.doc_id]["state"], rows[other.doc_id]["last_error"]) == (
        "skipped",
        "content_kind non_article",
    )
    assert rows[german.doc_id]["last_error"] == "language de"

    want = segment(good, PIPELINE_VERSION)
    assert segment_rows(conn, data_dir) == want
    vecs = embedded(good)
    assert rows[good.doc_id]["segments"] == len(want)
    assert rows[good.doc_id]["vectors"] == len(vecs) == index.count()
    assert {s.level for s in vecs} == {SegmentLevel.PASSAGE, SegmentLevel.TITLE}
    point = index.c.retrieve(index.collection, [point_id(vecs[1].segment_id)])[0]
    p = point.payload or {}
    assert (p["doc_id"], p["revision"], p["level"], p["source_id"]) == (
        good.doc_id,
        1,
        vecs[1].level.value,
        "example.com",
    )
    assert good.extracted_text[p["start"] : p["end"]] == vecs[1].text
    assert p["published"] == int(good.published_at.timestamp())  # type: ignore[union-attr]

    events = conn.execute(
        "SELECT stage, status FROM stage_events WHERE doc_id = ? AND stage IN "
        "('segment', 'embed', 'index') ORDER BY stage",
        (good.doc_id,),
    ).fetchall()
    assert [tuple(e) for e in events] == [("embed", "ok"), ("index", "ok"), ("segment", "ok")]
    assert verify_storage(conn, data_dir) == []
    assert reconcile(conn, index, repair=False, now=utcnow()).ok


def test_new_revision_replaces_old_points(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex, data_dir: Path
) -> None:
    v1 = article(1)
    store(conn, data_dir, tmp, v1)
    e = make(settings, conn, tmp, index)
    e.run_until_idle()
    v2 = article(1, paras=[*PARAS, "Update: the mayor signed it on Friday."])
    store(conn, data_dir, tmp, v2)
    e.run_until_idle()

    assert states(conn) == {f"{v1.doc_id}@1": "done", f"{v1.doc_id}@2": "done"}
    revisions = {r.payload["revision"] for r in index.c.scroll(index.collection, limit=100)[0]}  # type: ignore[index]
    assert revisions == {2}
    assert reconcile(conn, index, repair=False, now=utcnow()).ok


def test_older_revision_queued_behind_newer_is_skipped(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex, data_dir: Path
) -> None:
    store(conn, data_dir, tmp, article(1))
    store(conn, data_dir, tmp, article(1, paras=PARAS[:2]))
    make(settings, conn, tmp, index).run_until_idle()
    doc_id = article(1).doc_id
    assert states(conn) == {f"{doc_id}@1": "skipped", f"{doc_id}@2": "done"}
    assert reconcile(conn, index, repair=False, now=utcnow()).ok


def test_claims_newest_first(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, data_dir: Path
) -> None:
    old = article(1)
    store(conn, data_dir, tmp, old)
    new = article(2)
    store(conn, data_dir, tmp, new)  # the writer stamps ingested_at at flush: later
    queue_new(conn, PIPELINE_VERSION, 0, utcnow())
    got = claim(conn, PIPELINE_VERSION, "w", 1, 60, utcnow())
    assert [c.doc_id for c in got] == [new.doc_id]


def test_crash_between_index_and_commit_is_idempotent(
    settings: Settings,
    conn: sqlite3.Connection,
    tmp: TmpArea,
    index: VectorIndex,
    data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    doc = article(1)
    store(conn, data_dir, tmp, doc)

    def crash(*_: Any) -> None:
        raise RuntimeError("killed")

    with monkeypatch.context() as m:
        m.setattr(enricher_mod, "install_part", crash)
        with pytest.raises(RuntimeError):
            make(settings, conn, tmp, index).run_until_idle()
    n = len(embedded(doc))
    assert index.count() == n  # written before the crash
    assert states(conn) == {f"{doc.doc_id}@1": "running"}

    rec = recover_enricher(conn, data_dir, settings.enricher.max_attempts, utcnow())
    assert rec.requeued == 1
    make(settings, conn, tmp, index).run_until_idle()
    assert states(conn) == {f"{doc.doc_id}@1": "done"}
    assert index.count() == n  # overwritten, not duplicated
    assert segment_rows(conn, data_dir) == segment(doc, PIPELINE_VERSION)
    assert verify_storage(conn, data_dir) == []
    assert reconcile(conn, index, repair=False, now=utcnow()).ok


def test_reconcile_requeues_lost_vectors_without_duplicating_segments(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex, data_dir: Path
) -> None:
    a, b = article(1), article(2, paras=PARAS[1:])
    store(conn, data_dir, tmp, a, b)
    e = make(settings, conn, tmp, index)
    e.run_until_idle()
    index.delete_docs([(a.doc_id, 1)])  # e.g. a power cut lost Qdrant's last writes

    report = e.check_vectors()
    assert report is not None and report.mismatched == [(a.doc_id, 1)] and report.repaired
    assert states(conn)[f"{a.doc_id}@1"] == "pending"
    e.run_until_idle()
    assert e.check_vectors() is None
    assert index.count() == len(embedded(a)) + len(embedded(b))
    segs = segment_rows(conn, data_dir)
    assert len(segs) == len({s.segment_id for s in segs})  # vectors redone, segments not


def test_reconcile_removes_orphans(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex, data_dir: Path
) -> None:
    doc = article(1)
    store(conn, data_dir, tmp, doc)
    make(settings, conn, tmp, index).run_until_idle()
    conn.execute("UPDATE enrichments SET state = 'pending'")  # points without a done row
    r = reconcile(conn, index, repair=True, now=utcnow())
    assert r.orphaned == [(doc.doc_id, 1)] and index.count() == 0


def test_poison_revision_fails_after_max_attempts(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, data_dir: Path
) -> None:
    store(conn, data_dir, tmp, article(1))
    queue_new(conn, PIPELINE_VERSION, 0, utcnow())
    for _ in range(settings.enricher.max_attempts):
        assert claim(conn, PIPELINE_VERSION, "w", 10, 60, utcnow())
        recover_enricher(conn, data_dir, settings.enricher.max_attempts, utcnow())
    assert list(states(conn).values()) == ["failed"]


def test_compact_derived_merges_hot_parts(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex, data_dir: Path
) -> None:
    s = settings.model_copy(update={"enricher": EnricherSettings(batch_docs=1)})
    docs = [article(i, paras=PARAS[: 1 + i % 3]) for i in range(4)]
    store(conn, data_dir, tmp, *docs)
    make(s, conn, tmp, index).run_until_idle()
    before = sorted(x.segment_id for x in segment_rows(conn, data_dir))
    assert len(committed_files(conn, data_dir, SEGMENTS)) == 4

    report = compact_derived(conn, data_dir, tmp, SEGMENTS, SEGMENT_SCHEMA, utcnow() + timedelta(1))
    assert (report.inputs, report.outputs) == (4, 1)
    files = committed_files(conn, data_dir, SEGMENTS)
    assert len(files) == 1 and "/merged/" in str(files[0])
    assert sorted(x.segment_id for x in segment_rows(conn, data_dir)) == before
    assert verify_storage(conn, data_dir) == []
    assert not list((data_dir / "tables" / SEGMENTS / "hot").glob("*.parquet"))


def test_lag_and_disk_reports_cover_the_enricher(
    settings: Settings, conn: sqlite3.Connection, tmp: TmpArea, index: VectorIndex, data_dir: Path
) -> None:
    from newsrag.obs.reports import disk_report, format_disk, format_lag, lag_report

    store(conn, data_dir, tmp, article(1), article(2))
    make(settings, conn, tmp, index).run_until_idle()
    lag = lag_report(conn, utcnow(), timedelta(hours=1))
    assert lag.index.n == 2 and lag.index.p95 is not None and lag.index.p95 < 60
    assert lag.enrich_queue == {"done": 2} and lag.oldest_unenriched_s is None
    assert lag.stage_status["index"] == {"ok": 2}
    assert "index" in format_lag(lag)
    disk = disk_report(conn, settings, utcnow())
    assert disk.derived_bytes > 0 and disk.derived_bytes_per_doc > 0
    assert "derived" in format_disk(disk)
