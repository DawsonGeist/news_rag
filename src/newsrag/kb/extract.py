"""Knowledge base, step 1: Wikidata JSON entity -> compact rows. Pure functions.

The dump holds about 115M items and expands to about 1.5 TB of JSON. Workers keep only
what the entity map needs (phase2-design.md, "Knowledge base") and drop the bulk
classes that never matter for news (scholarly articles, Wikimedia internals, genes,
chemicals, astronomical objects) before anything is written.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import orjson

# Relations kept as graph edges (decisions.md, 2026-10-22: 1-hop subset).
RELATION_PROPS = frozenset(
    {
        "P108",  # employer
        "P127",  # owned by
        "P749",  # parent organization
        "P355",  # has subsidiary
        "P463",  # member of
        "P39",  # position held
        "P102",  # member of political party
        "P26",  # spouse
        "P40",  # child
        "P22",  # father
        "P25",  # mother
        "P3373",  # sibling
        "P488",  # chairperson
        "P169",  # chief executive officer
        "P112",  # founded by
        "P131",  # located in the administrative territorial entity
        "P17",  # country
        "P159",  # headquarters location
        "P27",  # country of citizenship
        "P106",  # occupation
    }
)
CLASS_PROPS = ("P31", "P279")  # instance of, subclass of
ID_PROPS = {"P5531": "cik", "P249": "ticker", "P856": "website"}
QUALIFIER_START, QUALIFIER_END = "P580", "P582"

# Instances of these classes are never news entities; dropping them early saves most of
# the output. Exact class match only (cheap); the build's seed rules do the real
# selection.
DROP_CLASSES = frozenset(
    {
        "Q13442814",  # scholarly article
        "Q4167410",  # Wikimedia disambiguation page
        "Q4167836",  # Wikimedia category
        "Q11266439",  # Wikimedia template
        "Q13406463",  # Wikimedia list article
        "Q17633526",  # Wikinews article
        "Q7187",  # gene
        "Q8054",  # protein
        "Q11173",  # chemical compound
        "Q113145171",  # type of chemical entity
        "Q523",  # star
        "Q318",  # galaxy
        "Q3863",  # asteroid
        "Q1931185",  # astronomical radio source
        "Q67206691",  # infrared source
        "Q2154519",  # astrophysical X-ray source
        "Q1457376",  # eclipsing binary star
    }
)


@dataclass
class Claim:
    qid: str
    prop: str
    target: str
    start: datetime | None = None
    end: datetime | None = None
    rank: str = "normal"


@dataclass
class EntityRow:
    qid: str
    label: str
    description: str | None
    aliases: list[str]
    sitelinks: int
    enwiki: str | None
    instance_of: list[str]
    subclass_of: list[str]
    cik: str | None = None
    ticker: str | None = None
    website: str | None = None
    claims: list[Claim] = field(default_factory=list)


def parse_time(value: dict[str, Any] | None) -> datetime | None:
    """Wikidata time values ('+2021-01-20T00:00:00Z', precision 9 = year) -> UTC.
    Coarse precisions keep their first day; BCE and unknown values are dropped."""
    if not value or not isinstance(value.get("time"), str):
        return None
    t = value["time"]
    if not t.startswith("+"):
        return None
    try:
        year, month, day = int(t[1:5]), int(t[6:8]) or 1, int(t[9:11]) or 1
        return datetime(year, month, day, tzinfo=UTC)
    except ValueError:
        return None


def _main_value(stmt: dict[str, Any]) -> Any:
    snak = stmt.get("mainsnak") or {}
    if snak.get("snaktype") != "value":
        return None
    return (snak.get("datavalue") or {}).get("value")


def _qualifier_time(stmt: dict[str, Any], prop: str) -> datetime | None:
    for q in (stmt.get("qualifiers") or {}).get(prop, []):
        if q.get("snaktype") == "value":
            return parse_time((q.get("datavalue") or {}).get("value"))
    return None


def _item_targets(stmts: list[dict[str, Any]]) -> list[str]:
    out = []
    for s in stmts:
        if s.get("rank") == "deprecated":
            continue
        v = _main_value(s)
        if isinstance(v, dict) and v.get("entity-type") == "item" and v.get("id"):
            out.append(v["id"])
    return out


def entity_row(e: dict[str, Any]) -> EntityRow | None:
    """Compact row for one item, or None if it's not wanted (no English name, a
    dropped class, or not an item)."""
    if e.get("type") != "item":
        return None
    claims = e.get("claims") or {}
    instance_of = _item_targets(claims.get("P31", []))
    if DROP_CLASSES.intersection(instance_of):
        return None
    labels = e.get("labels") or {}
    en = labels.get("en") or labels.get("mul")
    # Wikidata moves language-neutral names (acronyms such as "EU") from "en" to "mul".
    all_aliases = e.get("aliases") or {}
    aliases = list(
        dict.fromkeys(
            a["value"]
            for lang in ("en", "mul")
            for a in all_aliases.get(lang, [])
            if a.get("value")
        )
    )
    if not en and not aliases:
        return None
    sitelinks = e.get("sitelinks") or {}
    row = EntityRow(
        qid=e["id"],
        label=en["value"] if en else aliases[0],
        description=((e.get("descriptions") or {}).get("en") or {}).get("value"),
        aliases=aliases,
        sitelinks=len(sitelinks),
        enwiki=(sitelinks.get("enwiki") or {}).get("title"),
        instance_of=instance_of,
        subclass_of=_item_targets(claims.get("P279", [])),
    )
    for prop, attr in ID_PROPS.items():
        for s in claims.get(prop, []):
            v = _main_value(s)
            if isinstance(v, str) and s.get("rank") != "deprecated":
                setattr(row, attr, v)
                break
    for prop in RELATION_PROPS.intersection(claims):
        for s in claims[prop]:
            if s.get("rank") == "deprecated":
                continue
            v = _main_value(s)
            if not (isinstance(v, dict) and v.get("entity-type") == "item"):
                continue
            row.claims.append(
                Claim(
                    qid=row.qid,
                    prop=prop,
                    target=v["id"],
                    start=_qualifier_time(s, QUALIFIER_START),
                    end=_qualifier_time(s, QUALIFIER_END),
                    rank=s.get("rank", "normal"),
                )
            )
    return row


def iter_dump_lines(block: bytes) -> Iterator[bytes]:
    """Entity JSON lines from a block of the dump ('[', one entity per line with a
    trailing comma, ']')."""
    for line in block.split(b"\n"):
        line = line.strip().rstrip(b",")
        if len(line) > 2:  # skips '[' and ']'
            yield line


def rows_from_block(block: bytes) -> Iterator[EntityRow]:
    for line in iter_dump_lines(block):
        row = entity_row(orjson.loads(line))
        if row is not None:
            yield row
