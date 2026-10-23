"""Knowledge base: extraction rules and the snapshot build, on a synthetic mini-dump in
Wikidata's JSON format (all entities invented)."""

import gzip
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import orjson
import pytest

from newsrag.kb.build import build_snapshot, normalize_alias
from newsrag.kb.dump import DumpError, extract_dump
from newsrag.kb.extract import entity_row, parse_time

NOW = datetime(2026, 10, 23, tzinfo=UTC)


def item(
    qid: str,
    label: str | None,
    p31: list[str] = (),  # type: ignore[assignment]
    p279: list[str] = (),  # type: ignore[assignment]
    sitelinks: int = 0,
    aliases: list[str] = (),  # type: ignore[assignment]
    claims: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    def rel(target: str, **extra: Any) -> dict[str, Any]:
        return {
            "mainsnak": {
                "snaktype": "value",
                "property": "x",
                "datavalue": {
                    "type": "wikibase-entityid",
                    "value": {"entity-type": "item", "id": target},
                },
            },
            "rank": extra.get("rank", "normal"),
            "qualifiers": extra.get("qualifiers", {}),
        }

    all_claims: dict[str, list[dict[str, Any]]] = {}
    if p31:
        all_claims["P31"] = [rel(c) for c in p31]
    if p279:
        all_claims["P279"] = [rel(c) for c in p279]
    for prop, stmts in (claims or {}).items():
        all_claims[prop] = [
            rel(s["target"], **{k: v for k, v in s.items() if k != "target"})
            if "target" in s
            else s
            for s in stmts
        ]
    return {
        "type": "item",
        "id": qid,
        "labels": {"en": {"language": "en", "value": label}}
        if label
        else {"de": {"language": "de", "value": "X"}},
        "descriptions": {"en": {"language": "en", "value": f"desc of {label}"}} if label else {},
        "aliases": {"en": [{"language": "en", "value": a} for a in aliases]},
        "claims": all_claims,
        "sitelinks": {f"wiki{i}": {"site": f"wiki{i}", "title": label} for i in range(sitelinks)},
    }


def time_q(prop: str, iso: str) -> dict[str, Any]:
    return {prop: [{"snaktype": "value", "datavalue": {"value": {"time": iso, "precision": 11}}}]}


def string_claim(value: str) -> dict[str, Any]:
    return {
        "mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": value}},
        "rank": "normal",
    }


ENTITIES = [
    # Class hierarchy: news company -> business -> organization; settlement root.
    item("Q43229", "organization"),
    item("Q4830453", "business", p279=["Q43229"]),
    item("Q900", "news company", p279=["Q4830453"]),
    item("Q5", "human"),
    item("Q486972", "human settlement"),
    item("Q515", "city", p279=["Q486972"]),
    # Seeds.
    item(
        "Q1001",
        "Rosa Delgado",
        p31=["Q5"],
        sitelinks=3,
        aliases=["R. Delgado"],
        claims={
            "P108": [{"target": "Q2001", "qualifiers": time_q("P580", "+2021-01-20T00:00:00Z")}],
            "P27": [{"target": "Q4001"}],
            "P26": [{"target": "Q1002", "rank": "deprecated"}],  # deprecated: dropped
        },
    ),
    item(
        "Q2001",
        "Millbrook Ledger Company",
        p31=["Q900"],
        sitelinks=0,  # no sitelink, but a CIK makes it notable
        claims={"P5531": [string_claim("0000123456")], "P127": [{"target": "Q3001"}]},
    ),
    item("Q4001", "Millbrook", p31=["Q515"], sitelinks=5),
    # Background: not notable itself, reachable only through Q2001's "owned by".
    item("Q3001", "Ignatius Holdings Trust", p31=["Q43229"]),
    # Not notable and unconnected: not in the KB.
    item("Q3002", "Some Unknown LLC", p31=["Q4830453"]),
    # Dropped at extraction.
    item("Q7001", "A Study of Things", p31=["Q13442814"], sitelinks=1),  # scholarly article
    item("Q7002", "Delgado (disambiguation)", p31=["Q4167410"], sitelinks=4),
    item("Q7003", None, p31=["Q5"], sitelinks=2),  # no English label or alias
    {"type": "property", "id": "P108", "labels": {"en": {"value": "employer"}}},
]


@pytest.fixture
def dump(tmp_path: Path) -> Path:
    path = tmp_path / "wikidata-20261019-all.json.gz"
    lines = [orjson.dumps(e) for e in ENTITIES]
    with gzip.open(path, "wb") as f:
        f.write(b"[\n" + b",\n".join(lines) + b"\n]\n")
    return path


def test_parse_time() -> None:
    assert parse_time({"time": "+2021-01-20T00:00:00Z"}) == datetime(2021, 1, 20, tzinfo=UTC)
    assert parse_time({"time": "+1990-00-00T00:00:00Z"}) == datetime(
        1990, 1, 1, tzinfo=UTC
    )  # year precision
    assert parse_time({"time": "-0044-03-15T00:00:00Z"}) is None  # BCE
    assert parse_time(None) is None


def test_entity_row_rules() -> None:
    by_id = {e["id"]: e for e in ENTITIES}
    rosa = entity_row(by_id["Q1001"])
    assert rosa is not None and rosa.label == "Rosa Delgado" and rosa.sitelinks == 3
    assert [(c.prop, c.target, c.start) for c in rosa.claims if c.prop == "P108"] == [
        ("P108", "Q2001", datetime(2021, 1, 20, tzinfo=UTC))
    ]
    assert not [c for c in rosa.claims if c.prop == "P26"]  # deprecated
    company = entity_row(by_id["Q2001"])
    assert company is not None and company.cik == "0000123456"
    for dropped in ("Q7001", "Q7002", "Q7003"):
        assert entity_row(by_id[dropped]) is None
    assert entity_row(ENTITIES[-1]) is None  # a property


def test_normalize_alias() -> None:
    assert normalize_alias("  Rosa DELGADO ") == "rosa delgado"
    assert normalize_alias("ﬁnance") == "finance"  # NFKC ligature


def test_dump_to_snapshot(dump: Path, tmp_path: Path) -> None:
    shards = tmp_path / "extract"
    stats = extract_dump(dump, shards, workers=2, progress=False)
    assert stats.entities == len(ENTITIES) - 4  # 3 dropped items + 1 property
    out = tmp_path / "kb" / "20261019"
    counts = build_snapshot(shards, out, "20261019", NOW)

    con = duckdb.connect()
    ents = {
        r[0]: r[1:]
        for r in con.execute(f"SELECT qid, type, seed FROM '{out / 'entities.parquet'}'").fetchall()
    }
    assert ents["Q1001"] == ("person", True)
    assert ents["Q2001"] == ("organization", True)  # via news company -> business -> organization
    assert ents["Q4001"] == ("location", True)  # via city -> settlement
    assert ents["Q3001"] == ("organization", False)  # background, 1 hop from a seed
    assert "Q3002" not in ents  # not notable, not connected
    rels = con.execute(
        "SELECT src_entity_id, relation, dst_entity_id, valid_from "
        f"FROM '{out / 'relations.parquet'}' ORDER BY 1, 2"
    ).fetchall()
    assert ("wd:Q1001", "P108", "wd:Q2001", datetime(2021, 1, 20, tzinfo=UTC)) in rels
    assert ("wd:Q2001", "P127", "wd:Q3001", None) in rels
    assert not [r for r in rels if r[1] == "P26"]
    aliases = dict(
        con.execute(f"SELECT alias, entity_id FROM '{out / 'aliases.parquet'}'").fetchall()
    )
    assert aliases["r. delgado"] == "wd:Q1001" and aliases["millbrook"] == "wd:Q4001"
    assert counts["seeds"] == 3 and counts["entities"] == len(ents)
    meta = orjson.loads((out / "meta.json").read_bytes())
    assert meta["kb_snapshot"] == "20261019" and meta["entities_by_type"]["person"] >= 1


def test_truncated_dump_fails_loudly(dump: Path, tmp_path: Path) -> None:
    truncated = tmp_path / "truncated.json.gz"
    data = dump.read_bytes()
    truncated.write_bytes(data[: len(data) // 2])
    with pytest.raises(DumpError):
        extract_dump(truncated, tmp_path / "x", workers=1, progress=False)
