"""Entity stage pure parts: NER cleanup, mention grouping, coref chunking, link scoring.
All names are invented."""

from collections.abc import Sequence

from newsrag.contracts.v1 import EntityType
from newsrag.kb.index import Candidate, KbEntity, KbRelation
from newsrag.link.coref import chunks
from newsrag.link.linker import (
    LINK_THRESHOLD,
    Group,
    NamedMention,
    group_mentions,
    link_document,
    local_entity_id,
    variants,
)
from newsrag.link.ner import Span, clean, strip_title


def test_clean_keeps_names_only() -> None:
    text = "Gov. Rosa Delgado said we would sign the bill at Millbrook Ledger's office."
    spans = [
        Span(0, 17, "person", 0.9),  # "Gov. Rosa Delgado"
        Span(23, 25, "organization", 0.96),  # "we": a pronoun
        Span(40, 44, "law", 0.8),  # "bill": not a name
        Span(48, 66, "organization", 0.9),  # "Millbrook Ledger's"
        Span(48, 57, "location", 0.4),  # under threshold
    ]
    got = [(text[s.start : s.end], s.label) for s in clean(text, spans)]
    assert got == [("Rosa Delgado", "person"), ("Millbrook Ledger", "organization")]


def test_strip_title_handles_stacked_titles() -> None:
    t = "Former President Rosa Delgado"
    s, e = strip_title(t, 0, len(t))
    assert t[s:e] == "Rosa Delgado"
    assert strip_title("Mayor", 0, 5) == (0, 5)  # never strips to nothing


def test_variants() -> None:
    assert variants("The Millbrook Ledger") == ["the millbrook ledger", "millbrook ledger"]
    assert "rosa o'delgado" in variants("Rosa O’Delgado")
    assert "florida" in variants("Fla.")
    assert "millbrook speedway" in variants("Millbrook (Fla.) Speedway")
    assert "clipclop" in variants("ClipClops", EntityType.PRODUCT)
    assert "clipclop" not in variants("ClipClops", EntityType.PERSON)


def test_lone_surname_gets_surname_candidates() -> None:
    from newsrag.link.linker import candidate_keys

    keys = candidate_keys(Group("delgado", EntityType.PERSON, ["Delgado"], ["a" * 32], {0}))
    assert "surname:delgado" in keys


def test_short_org_name_joins_longer_one() -> None:
    groups = group_mentions(
        [nm(1, "Zero", EntityType.ORGANIZATION), nm(2, "Zero Motorcycles", EntityType.ORGANIZATION)]
    )
    assert [g.key for g in groups] == ["zero motorcycles"] and len(groups[0].mention_ids) == 2


def nm(i: int, surface: str, t: EntityType = EntityType.PERSON, passage: int = 0) -> NamedMention:
    return NamedMention(f"{i:032x}", surface, t, passage)


def test_short_names_join_the_full_name() -> None:
    groups = group_mentions(
        [
            nm(1, "Rosa Delgado"),
            nm(2, "Delgado", passage=3),
            nm(3, "Millbrook", EntityType.LOCATION),
            nm(4, "Smith"),  # two Smiths below: stays on its own
            nm(5, "Ann Smith"),
            nm(6, "Bo Smith"),
        ]
    )
    by_key = {g.key: g for g in groups}
    assert by_key["rosa delgado"].mention_ids == [f"{1:032x}", f"{2:032x}"]
    assert by_key["rosa delgado"].passages == {0, 3}
    assert "delgado" not in by_key and "smith" in by_key


def test_chunks_split_on_paragraphs_with_offsets() -> None:
    text = "\n\n".join(["one two three"] * 5)
    parts = chunks(text, max_words=6)
    assert [t for _, t in parts] == ["one two three\n\none two three"] * 2 + ["one two three"]
    for off, t in parts:
        assert text[off : off + len(t)] == t


def cand(eid: str, label: str, type_: str, sitelinks: int, kind: str = "label") -> Candidate:
    return Candidate(label.lower(), KbEntity(eid, label, None, type_, sitelinks), kind)


def test_link_document_uses_context_and_coherence() -> None:
    city_mi = cand("wd:Q1", "Millbrook", "location", 50)
    city_ks = cand("wd:Q2", "Millbrook", "location", 60)  # more popular, wrong state
    person = cand("wd:Q3", "Rosa Delgado", "person", 20)
    by_alias = {"millbrook": [city_ks, city_mi], "rosa delgado": [person]}
    groups = [
        Group("millbrook", EntityType.LOCATION, ["Millbrook"], ["a" * 32], {0}),
        Group("rosa delgado", EntityType.PERSON, ["Rosa Delgado"], ["b" * 32], {0}),
        Group("nobody known", EntityType.PERSON, ["Nobody Known"], ["c" * 32], {1}),
    ]

    def context(g: Group, cands: Sequence[Candidate]) -> list[float]:
        return [0.65 if c.entity.entity_id in ("wd:Q1", "wd:Q3") else 0.45 for c in cands]

    def relations(ids: Sequence[str]) -> list[KbRelation]:
        return [KbRelation("wd:Q3", "wd:Q1", "P27", None, None)]  # she's from the MI one

    out = {gl.group.key: gl for gl in link_document(groups, by_alias, context, relations)}
    assert out["millbrook"].entity_id == "wd:Q1"
    assert out["millbrook"].confidence >= LINK_THRESHOLD
    assert out["millbrook"].links[0].features["coherence"] > 0
    assert out["rosa delgado"].entity_id == "wd:Q3"
    nobody = out["nobody known"]
    assert nobody.links == [] and nobody.entity_id == local_entity_id(
        EntityType.PERSON, "Nobody Known"
    )
