"""Entity linking (component 7, steps 3-5): mention groups -> scored KB candidates.

Pure functions over plain data, so the scoring can be tested and tuned without models.

1. **Groups.** Within a document, mentions with the same normalized name are one group
   ("one sense per document"). A one-word person name that ends a longer person name in
   the same document ("Whitmer" after "Gretchen Whitmer") joins that name's group.
2. **Candidates** from the KB alias index, for the name and its variants (no leading
   "the", no possessive).
3. **Features** per candidate (``base_features``):
   - ``popularity`` (log sitelinks), ``top_prior`` and ``dominant`` (the most popular
     candidate, by a wide margin);
   - ``type``: the KB type agrees with the NER label (1), is a common confusion such as
     a team named after its city (0.5), or disagrees (0);
   - ``label``: the name is the entity's label rather than an alias;
   - ``context``: cosine between the group's passages and the entity's
     "label: description", rescaled, and ``context_rel``, relative to the best candidate;
   - ``coherence``: KB edges to the other groups' leading candidates;
   - ``partial``: found only by surname; ``is_person`` etc.: the NER type, so the bar
     for linking differs by type (local people often share a famous person's name).
4. **Decision.** A softmax over the candidates and a "not in the KB" option gives each
   candidate's probability. The best
   candidate links at or above ``LINK_THRESHOLD``; runners-up at or above
   ``KEEP_THRESHOLD`` are kept as lower-confidence links. A group with no link becomes a
   ``local:`` entity.

The weights are hand-set to start with and are fitted on the author-verified linking
test set (Gate 2, G2.2: precision at least 0.9 at the default threshold).
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from newsrag import ids
from newsrag.contracts.v1 import EntityType
from newsrag.kb.build import normalize_alias
from newsrag.kb.index import Candidate, KbRelation

LINKER_VERSION = "link-2"
LINK_THRESHOLD = 0.7  # cross-validated precision 0.91, recall 0.62 (eval/linking.yaml)
KEEP_THRESHOLD = 0.3
MAX_CANDIDATES = 20

# A conditional logit: a mention's candidates and a "not in the KB" option (``nil``)
# compete in one softmax, so a confidence is the probability that this candidate is
# the entity, given everything else on offer. Weights fitted on the author-verified
# linking test set (``newsrag eval linking --fit``, 2026-10-23).
FEATURES = (
    "popularity",
    "top_prior",
    "dominant",
    "type",
    "label",
    "context",
    "context_rel",
    "coherence",
    "partial",
    "is_person",
    "is_location",
    "is_org",
)
WEIGHTS = {
    "nil": 6.06,
    "popularity": 3.06,
    "top_prior": 1.22,
    "dominant": 1.12,
    "type": 2.42,
    "label": 0.77,
    "context": 5.94,
    "context_rel": 10.3,
    "coherence": 1.78,
    "partial": -0.78,
    "is_person": -1.57,
    "is_location": -0.24,
    "is_org": 0.62,
}
CONTEXT_LOW, CONTEXT_HIGH = 0.30, 0.70  # bge-m3 cosines between related texts

# AP-style state abbreviations as they appear in datelines and parentheticals.
STATE_ABBREVIATIONS = {
    "ala": "alabama",
    "ariz": "arizona",
    "ark": "arkansas",
    "calif": "california",
    "colo": "colorado",
    "conn": "connecticut",
    "del": "delaware",
    "fla": "florida",
    "ga": "georgia",
    "ill": "illinois",
    "ind": "indiana",
    "kan": "kansas",
    "ky": "kentucky",
    "la": "louisiana",
    "md": "maryland",
    "mass": "massachusetts",
    "mich": "michigan",
    "minn": "minnesota",
    "miss": "mississippi",
    "mo": "missouri",
    "mont": "montana",
    "neb": "nebraska",
    "nev": "nevada",
    "n.h": "new hampshire",
    "n.j": "new jersey",
    "n.m": "new mexico",
    "n.y": "new york",
    "n.c": "north carolina",
    "n.d": "north dakota",
    "okla": "oklahoma",
    "ore": "oregon",
    "pa": "pennsylvania",
    "r.i": "rhode island",
    "s.c": "south carolina",
    "s.d": "south dakota",
    "tenn": "tennessee",
    "tex": "texas",
    "vt": "vermont",
    "va": "virginia",
    "wash": "washington",
    "w.va": "west virginia",
    "wis": "wisconsin",
    "wyo": "wyoming",
}
SURNAME_PREFIX = "surname:"

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


def variants(name: str, type_: EntityType | None = None) -> list[str]:
    """Normalized lookup keys for a name: itself; with straight apostrophes; without a
    leading "the", a possessive, a parenthetical or a plural "s"; a state's full name for
    its abbreviation."""
    base = normalize_alias(name.replace("’", "'"))
    out = [base]

    def add(v: str) -> None:
        v = " ".join(v.split()).strip(" ,.;:-")
        if v and v not in out:
            out.append(v)

    add(base.removeprefix("the "))
    add(base.removesuffix("'s"))
    add(re.sub(r"\s*\([^)]*\)?", " ", base))  # "talladega (ala.) superspeedway", "doge ("
    if base.rstrip(".") in STATE_ABBREVIATIONS:
        add(STATE_ABBREVIATIONS[base.rstrip(".")])
    plural = base.endswith("s") and not base.endswith("ss") and len(base) > 4
    if plural and type_ in (EntityType.ORGANIZATION, EntityType.PRODUCT):
        add(base[:-1])  # "fords"
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
            _join(groups, g, hosts[0])
    # Short organization names join the longer name they start: "Zero" -> "Zero
    # Motorcycles", "MultiCare" -> "MultiCare Health System".
    for t in (EntityType.ORGANIZATION, EntityType.PRODUCT):
        orgs = [g for (k, gt), g in groups.items() if gt is t]
        for g in orgs:
            hosts = [h for h in orgs if h is not g and h.key.startswith(g.key + " ")]
            if len(hosts) == 1 and (g.key, t) in groups:
                _join(groups, g, hosts[0])
    return list(groups.values())


def _join(groups: dict[tuple[str, EntityType], Group], g: Group, host: Group) -> None:
    host.surfaces.extend(g.surfaces)
    host.mention_ids.extend(g.mention_ids)
    host.passages |= g.passages
    del groups[(g.key, g.type)]


def candidate_keys(group: Group) -> list[str]:
    keys: list[str] = []
    for s in [group.key, *sorted({normalize_alias(x) for x in group.surfaces})]:
        for v in variants(s, group.type):
            if v not in keys:
                keys.append(v)
    # A lone surname ("Trump", "Hughes") with no full name in the document: notable
    # people with that surname (KbIndex's surname index).
    if group.type is EntityType.PERSON and " " not in group.key:
        keys.append(SURNAME_PREFIX + keys[0])
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


def score(features: dict[str, float], weights: dict[str, float] | None = None) -> float:
    w = weights or WEIGHTS
    return sum(w[k] * features.get(k, 0.0) for k in FEATURES)


def probabilities(
    features: Sequence[dict[str, float]], weights: dict[str, float] | None = None
) -> list[float]:
    """Softmax over the candidates plus the "not in the KB" option."""
    w = weights or WEIGHTS
    logits = [score(f, w) for f in features]
    top = max([w["nil"], *logits])
    exps = [math.exp(z - top) for z in logits]
    total = math.exp(w["nil"] - top) + sum(exps)
    return [e / total for e in exps]


def base_features(
    group: Group, cands: Sequence[Candidate], cosines: Sequence[float]
) -> list[dict[str, float]]:
    """Every feature but coherence, for one group's candidates. Popularity and context
    are also given relative to the other candidates: a state's description matches any
    article only weakly, but better than its namesake hamlets' do."""
    links = [c.entity.sitelinks for c in cands]
    top = max(links)
    second = sorted(links)[-2] if len(links) > 1 else 0
    ctx = [rescale(c) for c in cosines]
    best_ctx = max(ctx)
    out = []
    for c, sl, cx in zip(cands, links, ctx, strict=True):
        out.append(
            {
                "popularity": min(1.0, math.log1p(sl) / math.log1p(300)),
                "top_prior": 1.0 if sl == top else 0.0,
                "dominant": 1.0 if sl == top and top >= 3 * max(second, 1) else 0.0,
                "type": type_score(group.type, c.entity.type),
                "label": 1.0 if c.kind == "label" else 0.0,
                "context": cx,
                "context_rel": cx - best_ctx,
                "partial": 1.0 if c.kind == "surname" else 0.0,
                "is_person": 1.0 if group.type is EntityType.PERSON else 0.0,
                "is_location": 1.0 if group.type is EntityType.LOCATION else 0.0,
                "is_org": 1.0 if group.type is EntityType.ORGANIZATION else 0.0,
            }
        )
    return out


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
        first[id(g)] = list(zip(cands, base_features(g, cands, context(g, cands)), strict=True))

    leaders: dict[int, list[str]] = {
        gid: [c.entity.entity_id for c, f in sorted(fs, key=lambda x: -score(x[1]))[:2]]
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
        feats = [
            {**f, "coherence": min(1.0, len(neighbours[c.entity.entity_id] & others) / 2)}
            for c, f in first[id(g)]
        ]
        probs = probabilities(feats)
        scored = [
            Scored(c.entity.entity_id, p, f)
            for (c, _), f, p in zip(first[id(g)], feats, probs, strict=True)
        ]
        scored.sort(key=lambda s: -s.confidence)
        kept = [s for s in scored if s.confidence >= KEEP_THRESHOLD][:3]
        gl = GroupLinks(g, kept, scored=scored)
        if not kept or kept[0].confidence < LINK_THRESHOLD:
            gl.local_id = local_entity_id(g.type, g.key)
        out.append(gl)
    return out
