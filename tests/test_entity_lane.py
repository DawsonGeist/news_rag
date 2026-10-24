"""The enricher's entity queue end to end, with fake NER and coreference models, a KB
built from the synthetic mini-dump of test_kb, and Qdrant in-process. Names invented."""

import re
import sqlite3
from collections.abc import Iterator, Sequence
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from qdrant_client import QdrantClient
from tests.test_enricher import FakeEmbedder, store
from tests.test_kb import NOW, dump  # noqa: F401  (fixture)
from tests.test_storage import doc_for

from newsrag.config import EnricherSettings, Settings
from newsrag.enrich.enricher import Enricher, reconcile
from newsrag.enrich.vectors import VectorIndex
from newsrag.kb.build import build_snapshot
from newsrag.kb.dump import extract_dump
from newsrag.kb.index import KbIndex, build_index
from newsrag.link.coref import Cluster
from newsrag.link.lane import EntityLane, entity_pipeline_version
from newsrag.link.linker import LINKER_VERSION
from newsrag.link.ner import Span
from newsrag.link.stage import EntityStage, VectorSource
from newsrag.link.tables import MENTION_LINKS, MENTIONS, SNIPPETS, from_table
from newsrag.storage.fsio import TmpArea
from newsrag.storage.recovery import verify_storage
from newsrag.storage.state import connect, init_schema
from newsrag.storage.tables import committed_files
from newsrag.timeutil import utcnow

TEXT = (
    "Rosa Delgado toured Millbrook on Monday. She praised the new library.\n\n"
    "Critics in Millbrook said the plan was rushed. Delgado disagreed."
)
KNOWN = {"Rosa Delgado": "person", "Delgado": "person", "Millbrook": "location"}


class FakeNer:
    model_id = "fake-ner@0"

    def predict(self, texts: Sequence[str]) -> list[list[Span]]:
        out = []
        for t in texts:
            spans = []
            for name, label in KNOWN.items():
                for m in re.finditer(rf"\b{name}\b", t):
                    if not any(s.start <= m.start() < s.end for s in spans):
                        spans.append(Span(m.start(), m.end(), label, 0.9))
            out.append(spans)
        return out


class FakeCoref:
    model_id = "fake-coref@0"

    def clusters(self, texts: Sequence[str]) -> list[list[Cluster]]:
        out = []
        for t in texts:
            name, she = t.find("Rosa Delgado"), t.find("She ")
            out.append([[(name, name + 12), (she, she + 3)]] if name >= 0 and she >= 0 else [])
        return out


@pytest.fixture
def kb_dir(dump: Path, tmp_path: Path) -> Path:  # noqa: F811
    shards = tmp_path / "extract"
    extract_dump(dump, shards, workers=1, progress=False)
    out = tmp_path / "data" / "kb" / "20261019"
    build_snapshot(shards, out, "20261019", NOW)
    build_index(out)
    return out


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    c = connect(tmp_path / "data" / "state" / "crawl.sqlite3")
    init_schema(c)
    yield c
    c.close()


def setup(
    tmp_path: Path, conn: sqlite3.Connection, kb_dir: Path, tmp: TmpArea
) -> tuple[Enricher, VectorIndex]:
    settings = Settings(data_dir=tmp_path / "data", enricher=EnricherSettings(batch_docs=50))
    index = VectorIndex(QdrantClient(location=":memory:"), "passages_test", 8)
    index.ensure_collection()
    embedder = FakeEmbedder()
    enricher = Enricher(settings, conn, tmp, embedder, index, "test")
    version = entity_pipeline_version(kb_dir)
    stage = EntityStage(
        FakeNer(),
        FakeCoref(),
        KbIndex(kb_dir),
        embedder,
        VectorSource(embedder, index.vectors),
        version,
    )
    enricher.entity_lane = EntityLane(settings, conn, tmp, stage, index, "test", version)
    return enricher, index


def read(conn: sqlite3.Connection, data_dir: Path, name: str) -> list:  # type: ignore[type-arg]
    out = []
    for p in committed_files(conn, data_dir, name):
        out.extend(from_table(name, pq.read_table(p)))
    return out


def test_entity_queue_end_to_end(tmp_path: Path, conn: sqlite3.Connection, kb_dir: Path) -> None:
    data_dir = tmp_path / "data"
    with TmpArea(data_dir / "tmp", "t") as tmp:
        doc = doc_for("https://example.com/news/visit", TEXT, title="Delgado visits Millbrook")
        store(conn, data_dir, tmp, doc)
        enricher, index = setup(tmp_path, conn, kb_dir, tmp)
        enricher.run_until_idle()

        version = enricher.entity_lane.pipeline_version
        assert version == f"ents-1/{LINKER_VERSION}+20261019.kb-3"
        states = dict(conn.execute("SELECT pipeline_version, state FROM enrichments").fetchall())
        assert states == {"enrich-1": "done", version: "done"}

        mentions = read(conn, data_dir, MENTIONS)
        surfaces = sorted(m.surface for m in mentions)
        assert surfaces.count("Millbrook") == 3  # two in the body, one in the title
        assert "She" in surfaces  # resolved by coreference
        links = read(conn, data_dir, MENTION_LINKS)
        by_surface = {m.mention_id: m.surface for m in mentions}
        linked = {(by_surface[x.mention_id], x.entity_id) for x in links}
        assert ("Rosa Delgado", "wd:Q1001") in linked or any(
            s == "Rosa Delgado" and e.startswith("local:") for s, e in linked
        )
        she = next(x for x in links if by_surface[x.mention_id] == "She")
        delgado = next(x.entity_id for x in links if by_surface[x.mention_id] == "Rosa Delgado")
        assert she.method == "coref" and she.entity_id == delgado
        snippets = read(conn, data_dir, SNIPPETS)
        assert snippets and all(s.text is None for s in snippets)
        for s in snippets:
            src = doc.title if s.locator.field == "title" else doc.extracted_text
            assert src and src[s.locator.start : s.locator.end]
        assert verify_storage(conn, data_dir) == []

        payloads = [r.payload for r in index.c.scroll(index.collection, limit=100)[0]]
        assert payloads and all(delgado in (p or {}).get("entities", []) for p in payloads)


def test_entities_survive_vectors_being_rewritten(
    tmp_path: Path, conn: sqlite3.Connection, kb_dir: Path
) -> None:
    data_dir = tmp_path / "data"
    with TmpArea(data_dir / "tmp", "t") as tmp:
        doc = doc_for("https://example.com/news/visit", TEXT)
        store(conn, data_dir, tmp, doc)
        enricher, index = setup(tmp_path, conn, kb_dir, tmp)
        enricher.run_until_idle()
        index.delete_docs([(doc.doc_id, 1)])  # lost vectors
        report = reconcile(conn, index, repair=True, now=utcnow())
        assert report.mismatched == [(doc.doc_id, 1)]
        enricher.run_until_idle()  # vectors only; the entity queue doesn't re-run
        payloads = [r.payload or {} for r in index.c.scroll(index.collection, limit=100)[0]]
        assert payloads and all(p.get("entities") for p in payloads)
        assert len(read(conn, data_dir, MENTIONS)) == len(
            {m.mention_id for m in read(conn, data_dir, MENTIONS)}
        )
