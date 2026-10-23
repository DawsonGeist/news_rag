"""Knowledge base, step 3: shards -> the KB snapshot (phase2-design.md, "Knowledge base").

Run in DuckDB over the extract shards:
1. **Class closure:** every class that is a (transitive) subclass of a root class, by
   ``EntityType``.
2. **Seeds:** items that are instances of a closed class and are notable (at least one
   sitelink, or a CIK, ticker or official website).
3. **1 hop** (decisions.md, 2026-10-22): every item linked to a seed by a kept relation,
   in either direction. These are the "background" entities of the vision: an owner or
   employer that is rarely in the news itself.
4. **Outputs** in ``data/kb/<snapshot>/``: ``entities``, ``relations`` (both ends in the
   KB) and ``aliases`` (normalized surface → qid, with the sitelink count as the
   popularity prior), plus ``meta.json``.
"""

from __future__ import annotations

import json
import unicodedata
from datetime import datetime
from pathlib import Path

import duckdb

from newsrag.contracts.v1 import EntityType
from newsrag.kb.extract import RELATION_PROPS

KB_VERSION = "kb-1"

# Root classes per entity type. The first matching type wins (person before
# organization before location ...), because some classes sit under several roots.
ROOT_CLASSES: dict[EntityType, tuple[str, ...]] = {
    EntityType.PERSON: ("Q5",),  # human
    EntityType.ORGANIZATION: ("Q43229", "Q4830453"),  # organization, business
    EntityType.LOCATION: (
        "Q2221906",
        "Q486972",
        "Q56061",
        "Q6256",
    ),  # geo location, settlement, admin territory, country
    EntityType.LAW: (
        "Q820655",
        "Q7748",
        "Q686822",
        "Q2334719",
        "Q131569",
    ),  # statute, law, bill, legal case, treaty
    EntityType.EVENT: ("Q1656682", "Q1190554"),  # event, occurrence
}
TYPE_ORDER = list(ROOT_CLASSES)


def normalize_alias(s: str) -> str:
    """The alias key: NFKC, case-folded, whitespace collapsed. Matching mentions must use
    the same function."""
    return " ".join(unicodedata.normalize("NFKC", s).casefold().split())


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row else 0


def build_snapshot(
    extract_dir: Path, out_dir: Path, snapshot: str, now: datetime
) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{out_dir / '_duckdb_tmp'}'")
    # The machine also runs the crawl, Neo4j and Qdrant: cap DuckDB and let it spill.
    con.execute("SET memory_limit = '12GB'")
    con.create_function("normalize_alias", normalize_alias, ["VARCHAR"], "VARCHAR")
    ents = str(extract_dir / "entities-*.parquet")
    claims = str(extract_dir / "claims-*.parquet")
    con.execute(f"CREATE VIEW e AS SELECT * FROM read_parquet('{ents}')")
    con.execute(f"CREATE VIEW c AS SELECT * FROM read_parquet('{claims}')")

    roots = [(t.value, i, q) for i, (t, qs) in enumerate(ROOT_CLASSES.items()) for q in qs]
    con.execute("CREATE TABLE roots (type VARCHAR, prio INTEGER, cls VARCHAR)")
    con.executemany("INSERT INTO roots VALUES (?, ?, ?)", roots)
    con.execute(
        "CREATE TABLE p279 AS SELECT qid AS cls, unnest(subclass_of) AS sup "
        "FROM e WHERE len(subclass_of) > 0"
    )
    # 1. closure: class -> (type, prio) for every subclass of every root
    con.execute("""
        CREATE TABLE closure AS
        WITH RECURSIVE sub(cls, type, prio) AS (
            SELECT cls, type, prio FROM roots
            UNION
            SELECT p.cls, s.type, s.prio FROM p279 p JOIN sub s ON p.sup = s.cls
        )
        SELECT cls, arg_min(type, prio) AS type, min(prio) AS prio FROM sub GROUP BY cls
    """)
    # 2. seeds: instance of a closed class, and notable
    con.execute("""
        CREATE TABLE typed AS
        SELECT e.qid, arg_min(cl.type, cl.prio) AS type
        FROM (SELECT qid, unnest(instance_of) AS cls FROM e) i
        JOIN closure cl ON cl.cls = i.cls
        JOIN e ON e.qid = i.qid
        GROUP BY e.qid
    """)
    con.execute("""
        CREATE TABLE seeds AS
        SELECT t.qid, t.type FROM typed t JOIN e USING (qid)
        WHERE e.sitelinks > 0 OR e.cik IS NOT NULL OR e.ticker IS NOT NULL OR e.website IS NOT NULL
    """)
    # 3. one hop, both directions, over the kept relations
    props = ", ".join(f"'{p}'" for p in sorted(RELATION_PROPS))
    con.execute(f"""
        CREATE TABLE kb_ids AS
        SELECT qid FROM seeds
        UNION SELECT c.target FROM c JOIN seeds s ON c.qid = s.qid WHERE c.prop IN ({props})
        UNION SELECT c.qid FROM c JOIN seeds s ON c.target = s.qid WHERE c.prop IN ({props})
    """)
    # 4. outputs
    con.execute(f"""
        COPY (
            SELECT 'wd:' || e.qid AS entity_id, e.qid, e.label, e.description, e.aliases,
                   coalesce(s.type, t.type, 'other') AS type, s.qid IS NOT NULL AS seed,
                   e.sitelinks, e.enwiki, e.cik, e.ticker, e.website, e.instance_of
            FROM kb_ids k JOIN e ON e.qid = k.qid
            LEFT JOIN seeds s ON s.qid = k.qid LEFT JOIN typed t ON t.qid = k.qid
        ) TO '{out_dir / "entities.parquet"}' (FORMAT parquet, COMPRESSION zstd)
    """)
    con.execute(f"""
        COPY (
            SELECT 'wd:' || c.qid AS src_entity_id, 'wd:' || c.target AS dst_entity_id,
                   c.prop AS relation, c.start AS valid_from, c."end" AS valid_to, c.rank
            FROM c JOIN kb_ids a ON a.qid = c.qid JOIN kb_ids b ON b.qid = c.target
            WHERE c.prop IN ({props}) AND c.qid <> c.target
        ) TO '{out_dir / "relations.parquet"}' (FORMAT parquet, COMPRESSION zstd)
    """)
    con.execute(f"""
        COPY (
            SELECT DISTINCT normalize_alias(a.surface) AS alias, 'wd:' || e.qid AS entity_id,
                   e.sitelinks, a.kind
            FROM (SELECT qid, label AS surface, 'label' AS kind FROM e
                  UNION ALL SELECT qid, unnest(aliases), 'alias' FROM e) a
            JOIN kb_ids k ON k.qid = a.qid JOIN e ON e.qid = a.qid
            WHERE length(a.surface) > 1
        ) TO '{out_dir / "aliases.parquet"}' (FORMAT parquet, COMPRESSION zstd)
    """)
    counts = {
        "classes_closed": _count(con, "SELECT count(*) FROM closure"),
        "seeds": _count(con, "SELECT count(*) FROM seeds"),
        "entities": _count(con, f"SELECT count(*) FROM '{out_dir / 'entities.parquet'}'"),
        "relations": _count(con, f"SELECT count(*) FROM '{out_dir / 'relations.parquet'}'"),
        "aliases": _count(con, f"SELECT count(*) FROM '{out_dir / 'aliases.parquet'}'"),
    }
    by_type = dict(
        con.execute(
            f"SELECT type, count(*) FROM '{out_dir / 'entities.parquet'}' GROUP BY 1"
        ).fetchall()
    )
    meta = {
        "kb_snapshot": snapshot,
        "kb_version": KB_VERSION,
        "built_at": now.isoformat(),
        "counts": counts,
        "entities_by_type": by_type,
        "root_classes": {t.value: list(q) for t, q in ROOT_CLASSES.items()},
        "relation_props": sorted(RELATION_PROPS),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    con.close()
    return counts
