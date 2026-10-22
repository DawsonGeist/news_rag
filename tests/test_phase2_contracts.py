"""P2.0: provisional contract changes, enrichment state DDL and table-group storage."""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from pydantic import ValidationError

from newsrag import ids
from newsrag.contracts.v1 import (
    EntityRelation,
    RelationSource,
    Segment,
    SegmentLevel,
    TextLocator,
)
from newsrag.storage.fsio import TmpArea
from newsrag.storage.recovery import recover_writer, remove_uncommitted_table_files, verify_storage
from newsrag.storage.state import DDL_FILES, DDL_PACKAGE, connect, init_schema, transaction
from newsrag.storage.tables import GROUP_LOCKS, TABLE_GROUPS, group_of, install_part, stage_part

T0 = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)


def relation(**kw: object) -> EntityRelation:
    fields: dict[str, object] = {
        "relation_id": ids.hash32("wd:Q1:P108:wd:Q2:wikidata:"),
        "src_entity_id": "wd:Q1",
        "dst_entity_id": "wd:Q2",
        "relation": "P108",
        "source": RelationSource.WIKIDATA,
        "confidence": 1.0,
        "kb_snapshot": "20261001",
        "pipeline_version": "kb-1",
        "created_at": T0,
    }
    return EntityRelation.model_validate(fields | kw)


def test_entity_relation_rules() -> None:
    r = relation(valid_from=datetime(2021, 1, 20, tzinfo=UTC))
    assert r.valid_to is None and r.weight == 1
    with pytest.raises(ValidationError):
        relation(valid_from=T0, valid_to=datetime(2020, 1, 1, tzinfo=UTC))
    with pytest.raises(ValidationError):
        relation(dst_entity_id="wd:Q1")  # self-loop
    with pytest.raises(ValidationError):
        relation(valid_from=datetime(2021, 1, 20))  # naive datetime


def test_passage_and_title_segments() -> None:
    for level, loc in (
        (SegmentLevel.PASSAGE, TextLocator(start=0, end=120)),
        (SegmentLevel.TITLE, TextLocator(field="title", start=0, end=20)),
    ):
        Segment(
            segment_id="0" * 32,
            doc_id="1" * 32,
            revision=1,
            level=level,
            position=0,
            text="x",
            locator=loc,
            chunker_version="c1",
            pipeline_version="p1",
        )


def test_enrichment_tables_added_to_an_existing_database(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    old = sqlite3.connect(db)  # a Phase 1 database: crawl state only
    from importlib import resources

    old.executescript(resources.files(DDL_PACKAGE).joinpath(DDL_FILES[0]).read_text())
    old.close()
    conn = connect(db)
    init_schema(conn)
    init_schema(conn)  # idempotent
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"enrichments", "link_overrides", "frontier", "doc_revisions"} <= tables
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO link_overrides VALUES ('x', 'maybe', 's', NULL, 'wd:Q1', 'r', 't')"
        )


def test_table_groups_are_disjoint_and_locked() -> None:
    seen: set[str] = set()
    for tables in TABLE_GROUPS.values():
        assert not seen & set(tables)
        seen |= set(tables)
    assert set(GROUP_LOCKS) == set(TABLE_GROUPS)
    assert group_of("segments") == "enrichment" and group_of("documents") == "documents"
    with pytest.raises(KeyError):
        group_of("nobody_owns_this")


def test_recovery_is_scoped_to_the_callers_group(tmp_path: Path) -> None:
    data = tmp_path / "data"
    conn = connect(data / "state" / "crawl.sqlite3")
    init_schema(conn)
    with TmpArea(data / "tmp", "t") as tmp:
        part = stage_part(tmp, "segments", pa.table({"segment_id": ["a", "b"]}))
        # The enricher has renamed its part into place but not committed yet.
        dest = data / part.rel_path
        dest.parent.mkdir(parents=True)
        part.tmp_path.rename(dest)
        report = recover_writer(conn, data, T0)  # the documents writer starts up meanwhile
        assert report.table_files_removed == 0 and dest.exists()
        # The enricher's own recovery (after a crash) does remove its uncommitted part.
        assert remove_uncommitted_table_files(conn, data, "enrichment") == 1
        assert not dest.exists()


def test_derived_parts_install_and_verify(tmp_path: Path) -> None:
    data = tmp_path / "data"
    conn = connect(data / "state" / "crawl.sqlite3")
    init_schema(conn)
    with TmpArea(data / "tmp", "t") as tmp:
        part = stage_part(tmp, "entity_relations", pa.table({"relation_id": ["r1", "r2", "r3"]}))
        with transaction(conn):
            install_part(conn, data, part, T0)
    assert verify_storage(conn, data) == []
    with transaction(conn):
        conn.execute("UPDATE table_files SET row_count = 4")
    assert any("row_count mismatch" in p for p in verify_storage(conn, data))
    with pytest.raises(KeyError), TmpArea(data / "tmp", "u") as tmp:
        stage_part(tmp, "unowned", pa.table({"x": [1]}))
