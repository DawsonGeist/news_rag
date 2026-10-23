"""Entity linking (component 7, steps 3-5): mention groups -> scored KB candidates.

Pure functions over plain data, so the scoring can be tested and tuned without models.

1. **Groups.** Within a document, mentions with the same normalized name are one group
   ("one sense per document"). A one-word person name that ends a longer person name in
   the same document ("Whitmer" after "Gretchen Whitmer") joins that name's group.
2. **Candidates** from the KB alias index, for the name and its variants (no leading
   "the", no possessive).
3. **Features** per candidate, each in [0, 1]:
   - ``prior``: share of the candidates' sitelinks (popularity);
   - ``type``: the KB type agrees with the NER label (1), is a common confusion such as
     a team named after its city (0.5), or disagrees (0);
   - ``label``: the name is the entity's label rather than an alias;
   - ``context``: cosine between the group's passages and the entity's
     "label: description", rescaled;
   - ``coherence``: KB edges to the other groups' leading candidates.
4. **Decision.** A weighted sum through a logistic gives the confidence. The best
   candidate links at or above ``LINK_THRESHOLD``; runners-up at or above
   ``KEEP_THRESHOLD`` are kept as lower-confidence links. A group with no link becomes a
   ``local:`` entity.

The weights are hand-set to start with and are fitted on the author-verified linking
test set (Gate 2, G2.2: precision at least 0.9 at the default threshold).
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from newsrag import ids
from newsrag.contracts.v1 import EntityType
from newsrag.kb.build import normalize_alias
from newsrag.kb.index import Candidate, KbRelation

LINKER_VERSION = "link-1"
LINK_THRESHOLD = 0.6
KEEP_THRESHOLD = 0.3
MAX_CANDIDATES = 20

WEIGHTS = {"bias": -3.0, "prior": 1.5, "type": 1.5, "label": 0.5, "context": 2.5, "coherence": 1.5}
CONTEXT_LOW, CONTEXT_HIGH = 0.30, 0.70  # bge-m3 cosines between related texts

# NER label -> KB types that count as a partial match: teams and governments are
# named after places, and Wikidata types some organizations as locations.
_PARTIAL = {
    EntityType.ORGANIZATION: {"location", "other"},
    EntityType.LOCATION: {"organization"},
    EntityType.EVENT: {"other"},
    EntityType.PRODUCT: {"organization", "other"},
    EntityType.WORK: {"other"},
    EntityType.LAW: {"other"},
    EntityType.PERSON: set(),
}


@dataclass
class Group:
    """Mentions of one name in one document."""

    key: str  # normalized name
    type: EntityType
    surfaces: list[str] = field(default_factory=list)
    mention_ids: list[str] = field(default_factory=list)
    passages: set[int] = field(default_factory=set)  # passage positions it occurs in
    full_name: str | None = None  # for a short person name: the longer name it joined


@dataclass(frozen=True)
class Scored:
    entity_id: str
    confidence: float
    features: dict[str, float]


@dataclass
class GroupLinks:
    group: Group
    links: list[Scored]  # best first; empty means a local entity
    local_id: str | None = None
    scored: list[Scored] = field(default_factory=list)  # every candidate (for fitting)

    @property
    def entity_id(self) -> str:
        if self.links and self.links[0].confidence >= LINK_THRESHOLD:
            return self.links[0].entity_id
        assert self.local_id is not None
        return self.local_id

    @property
    def confidence(self) -> float:
        if self.links and self.links[0].confidence >= LINK_THRESHOLD:
            return self.links[0].confidence
        return 1.0 if self.local_id else 0.0


@dataclass(frozen=True)
class NamedMention:
    mention_id: str
    surface: str
    type: EntityType
    passage: int  # passage position; -1 for the title


def variants(name: str) -> list[str]:
    """Normalized lookup keys for a name: itself, without a leading "the", without a
    possessive."""
    base = normalize_alias(name)
    out = [base]
    for v in (base.removeprefix("the "), base.removesuffix("'s").removesuffix("’s")):
        if v and v not in out:
            out.append(v)
    return out


def group_mentions(mentions: Sequence[NamedMention]) -> list[Group]:
    groups: dict[tuple[str, EntityType], Group] = {}
    for m in mentions:
        key = normalize_alias(m.surface)
        g = groups.setdefault((key, m.type), Group(key, m.type))
        g.surfaces.append(m.surface)
        g.mention_ids.append(m.mention_id)
        if m.passage >= 0:
            g.passages.add(m.passage)
    # Short person names join the longer name they end: "Whitmer" -> "Gretchen Whitmer".
    persons = [g for (k, t), g in groups.items() if t is EntityType.PERSON]
    longs = [g for g in persons if " " in g.key]
    for g in persons:
        if " " in g.key:
            continue
        hosts = [h for h in longs if h.key.endswith(" " + g.key)]
        if len(hosts) == 1:  # ambiguous ("Smith" with two Smiths) stays on its own
            host = hosts[0]
            host.surfaces.extend(g.surfaces)
            host.mention_ids.extend(g.mention_ids)
            host.passages |= g.passages
            del groups[(g.key, g.type)]
    return list(groups.values())


def candidate_keys(group: Group) -> list[str]:
    keys: list[str] = []
    for s in [group.key, *{normalize_alias(x) for x in group.surfaces}]:
        for v in variants(s):
            if v not in keys:
                keys.append(v)
    return keys


def pool(group: Group, by_alias: dict[str, list[Candidate]]) -> list[Candidate]:
    """The group's candidates: the most popular entities over all its keys, one row per
    entity (an exact label match preferred)."""
    best: dict[str, Candidate] = {}
    for k in candidate_keys(group):
        for c in by_alias.get(k, []):
            prev = best.get(c.entity.entity_id)
            if prev is None or (prev.kind != "label" and c.kind == "label"):
                best[c.entity.entity_id] = c
    ranked = sorted(best.values(), key=lambda c: -c.entity.sitelinks)
    return ranked[:MAX_CANDIDATES]


def type_score(ner: EntityType, kb_type: str) -> float:
    if kb_type == ner.value:
        return 1.0
    return 0.5 if kb_type in _PARTIAL[ner] else 0.0


def rescale(cos: float) -> float:
    return min(1.0, max(0.0, (cos - CONTEXT_LOW) / (CONTEXT_HIGH - CONTEXT_LOW)))


def confidence(features: dict[str, float], weights: dict[str, float] | None = None) -> float:
    w = weights or WEIGHTS
    z = w["bias"] + sum(w[k] * v for k, v in features.items())
    return 1.0 / (1.0 + math.exp(-z))


def local_entity_id(type_: EntityType, name: str) -> str:
    return "local:" + ids.hash32(f"{type_.value}:{normalize_alias(name)}")


def link_document(
    groups: Sequence[Group],
    by_alias: dict[str, list[Candidate]],
    context: Callable[[Group, Sequence[Candidate]], list[float]],
    relations: Callable[[Sequence[str]], list[KbRelation]],
) -> list[GroupLinks]:
    """Link every group of one document.

    ``context(group, candidates)`` returns the cosine per candidate; ``relations(ids)``
    returns the KB edges among entity ids. Coherence uses each other group's two
    leading candidates by the first-pass score, so a document's links support each
    other ("Lansing" near "Gretchen Whitmer" is the Michigan one).
    """
    pools = {id(g): pool(g, by_alias) for g in groups}
    first: dict[int, list[tuple[Candidate, dict[str, float]]]] = {}
    for g in groups:
        cands = pools[id(g)]
        if not cands:
            first[id(g)] = []
            continue
        total = sum(c.entity.sitelinks for c in cands) or 1
        cos = context(g, cands)
        first[id(g)] = [
            (
                c,
                {
                    "prior": c.entity.sitelinks / total,
                    "type": type_score(g.type, c.entity.type),
                    "label": 1.0 if c.kind == "label" else 0.0,
                    "context": rescale(cs),
                },
            )
            for c, cs in zip(cands, cos, strict=True)
        ]

    leaders: dict[int, list[str]] = {
        gid: [c.entity.entity_id for c, f in sorted(fs, key=lambda x: -confidence(x[1]))[:2]]
        for gid, fs in first.items()
    }
    all_ids = sorted(
        {e for ls in leaders.values() for e in ls}
        | {c.entity.entity_id for fs in first.values() for c, _ in fs}
    )
    neighbours: dict[str, set[str]] = defaultdict(set)
    for r in relations(all_ids):
        neighbours[r.src].add(r.dst)
        neighbours[r.dst].add(r.src)

    out: list[GroupLinks] = []
    for g in groups:
        others = {e for gid, ls in leaders.items() if gid != id(g) for e in ls}
        scored = []
        for c, f in first[id(g)]:
            support = len(neighbours[c.entity.entity_id] & others)
            feats = {**f, "coherence": min(1.0, support / 2)}
            scored.append(Scored(c.entity.entity_id, confidence(feats), feats))
        scored.sort(key=lambda s: -s.confidence)
        kept = [s for s in scored if s.confidence >= KEEP_THRESHOLD][:3]
        gl = GroupLinks(g, kept, scored=scored)
        if not kept or kept[0].confidence < LINK_THRESHOLD:
            gl.local_id = local_entity_id(g.type, g.key)
        out.append(gl)
    return out
