"""Knowledge base, step 4: a lookup index over a snapshot, for the entity linker.

The snapshot's Parquet files suit bulk work; linking needs point lookups (alias →
candidates, entity → attributes, entity → neighbours) many times a second. ``kb.duckdb``
in the snapshot directory holds the same rows with ART indexes, opened read-only by
the linker. It is derived from the snapshot and rebuilt with ``newsrag kb index``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

INDEX_FILE = "kb.duckdb"


def build_index(snapshot_dir: Path) -> dict[str, int]:
    """(Re)build ``kb.duckdb`` from the snapshot's Parquet files, atomically."""
    final = snapshot_dir / INDEX_FILE
    tmp = snapshot_dir / f"{INDEX_FILE}.tmp"
    tmp.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    con.execute(f"SET temp_directory = '{snapshot_dir / '_duckdb_tmp'}'")
    con.execute("SET memory_limit = '12GB'")
    con.execute(f"""
        CREATE TABLE entities AS
        SELECT entity_id, label, description, type, seed, sitelinks, enwiki, cik, ticker, website
        FROM read_parquet('{snapshot_dir / "entities.parquet"}')
    """)
    con.execute(f"""
        CREATE TABLE aliases AS
        SELECT alias, entity_id, sitelinks, kind
        FROM read_parquet('{snapshot_dir / "aliases.parquet"}')
    """)
    con.execute(f"""
        CREATE TABLE relations AS
        SELECT src_entity_id AS src, dst_entity_id AS dst, relation, valid_from, valid_to
        FROM read_parquet('{snapshot_dir / "relations.parquet"}')
    """)
    con.execute("CREATE INDEX entities_id ON entities (entity_id)")
    con.execute("CREATE INDEX aliases_alias ON aliases (alias)")
    con.execute("CREATE INDEX relations_src ON relations (src)")
    con.execute("CREATE INDEX relations_dst ON relations (dst)")
    counts = {
        t: int(con.execute(f"SELECT count(*) FROM {t}").fetchone()[0])  # type: ignore[index]
        for t in ("entities", "aliases", "relations")
    }
    con.execute("CHECKPOINT")
    con.close()
    tmp.replace(final)
    return counts


@dataclass(frozen=True)
class KbEntity:
    entity_id: str
    label: str
    description: str | None
    type: str
    sitelinks: int


@dataclass(frozen=True)
class Candidate:
    alias: str
    entity: KbEntity
    kind: str  # 'label' or 'alias'


@dataclass(frozen=True)
class KbRelation:
    src: str
    dst: str
    relation: str
    valid_from: datetime | None
    valid_to: datetime | None


class KbIndex:
    """Read-only lookups over one snapshot's ``kb.duckdb``."""

    def __init__(self, snapshot_dir: Path) -> None:
        self.snapshot = snapshot_dir.name
        self.con = duckdb.connect(str(snapshot_dir / INDEX_FILE), read_only=True)

    def close(self) -> None:
        self.con.close()

    def candidates(self, aliases: Iterable[str], per_alias: int = 20) -> dict[str, list[Candidate]]:
        """Normalized alias → its most popular entities (by sitelink count)."""
        wanted = sorted(set(aliases))
        out: dict[str, list[Candidate]] = {a: [] for a in wanted}
        if not wanted:
            return out
        rows = self.con.execute(
            """
            SELECT a.alias, e.entity_id, e.label, e.description, e.type, e.sitelinks, a.kind
            FROM (SELECT alias, entity_id, kind,
                         row_number() OVER (PARTITION BY alias ORDER BY sitelinks DESC) AS rn
                  FROM aliases WHERE alias IN (SELECT unnest(?))) a
            JOIN entities e USING (entity_id)
            WHERE a.rn <= ?
            ORDER BY a.alias, e.sitelinks DESC
            """,
            [wanted, per_alias],
        ).fetchall()
        for alias, eid, label, desc, typ, sitelinks, kind in rows:
            out[alias].append(Candidate(alias, KbEntity(eid, label, desc, typ, sitelinks), kind))
        return out

    def entities(self, entity_ids: Iterable[str]) -> dict[str, KbEntity]:
        ids = sorted(set(entity_ids))
        if not ids:
            return {}
        rows = self.con.execute(
            "SELECT entity_id, label, description, type, sitelinks FROM entities "
            "WHERE entity_id IN (SELECT unnest(?))",
            [ids],
        ).fetchall()
        return {r[0]: KbEntity(*r) for r in rows}

    def relations_among(self, entity_ids: Sequence[str]) -> list[KbRelation]:
        """KB edges with both ends in ``entity_ids`` (the document coherence signal)."""
        ids = sorted(set(entity_ids))
        if len(ids) < 2:
            return []
        rows = self.con.execute(
            "SELECT src, dst, relation, valid_from, valid_to FROM relations "
            "WHERE src IN (SELECT unnest(?)) AND dst IN (SELECT unnest(?))",
            [ids, ids],
        ).fetchall()
        return [KbRelation(*r) for r in rows]
