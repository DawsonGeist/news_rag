"""The entity stage for a batch of documents: NER, coreference, linking, snippets.

Runs inside the enricher after a revision's vectors exist (the passage vectors are the
linker's context, and the stage tags the revision's points with its entities for gated
search). Produces contract records; the caller commits them.
"""

from __future__ import annotations

import bisect
from collections import Counter, OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np

from newsrag import ids
from newsrag.contracts.v1 import (
    Document,
    Entity,
    EntityType,
    Mention,
    MentionLink,
    Segment,
    SegmentLevel,
    Snippet,
    TextLocator,
)
from newsrag.enrich.embedder import Embedder, Vectors
from newsrag.kb.build import normalize_alias
from newsrag.kb.index import Candidate, KbIndex
from newsrag.link.coref import CorefModel
from newsrag.link.linker import (
    LINK_THRESHOLD,
    LINKER_VERSION,
    Group,
    GroupLinks,
    NamedMention,
    candidate_keys,
    group_mentions,
    link_document,
)
from newsrag.link.ner import LABELS, NerModel, clean

COREF_MAX_WORDS = 6  # longer coreferent phrases are descriptions, not references
SNIPPET_NEIGHBOURS = 1  # sentences either side of the mention's sentence


@dataclass
class DocEntities:
    """One revision's output."""

    mentions: list[Mention] = field(default_factory=list)
    links: list[MentionLink] = field(default_factory=list)
    snippets: list[Snippet] = field(default_factory=list)
    entities: list[Entity] = field(default_factory=list)  # local entities only
    entity_ids: list[str] = field(default_factory=list)  # linked or local, for the payload
    groups: list[GroupLinks] = field(default_factory=list)  # in memory only (evaluation)
    named: dict[str, NamedMention] = field(default_factory=dict)  # mention_id -> its name


@dataclass(frozen=True)
class _Unit:
    doc: int
    field: str
    offset: int
    text: str
    passage: int  # passage position, -1 for the title


class VectorSource:
    """Passage vectors for context: from the vector store, else embedded here."""

    def __init__(self, embedder: Embedder, fetch: object | None = None) -> None:
        self.embedder = embedder
        self.fetch = fetch  # callable(segment_ids) -> dict[segment_id, vector] or None

    def get(self, segs: Sequence[Segment]) -> dict[str, Vectors]:
        out: dict[str, Vectors] = {}
        if self.fetch is not None:
            out.update(self.fetch([s.segment_id for s in segs]))  # type: ignore[operator]
        missing = [s for s in segs if s.segment_id not in out]
        if missing:
            vecs = self.embedder.encode([s.text for s in missing])
            out.update({s.segment_id: v for s, v in zip(missing, vecs, strict=True)})
        return out


class EntityStage:
    def __init__(
        self,
        ner: NerModel,
        coref: CorefModel,
        kb: KbIndex,
        embedder: Embedder,
        vectors: VectorSource,
        pipeline_version: str,
        cache_size: int = 100_000,
    ) -> None:
        self.ner = ner
        self.coref = coref
        self.kb = kb
        self.embedder = embedder
        self.vectors = vectors
        self.pipeline_version = pipeline_version
        self.cache: OrderedDict[str, np.ndarray] = OrderedDict()
        self.cache_size = cache_size

    # --- candidate description vectors (LRU) -------------------------------------------

    def _entity_vectors(self, cands: Sequence[Candidate]) -> dict[str, np.ndarray]:
        out: dict[str, np.ndarray] = {}
        todo: list[Candidate] = []
        for c in cands:
            eid = c.entity.entity_id
            if eid in self.cache:
                self.cache.move_to_end(eid)
                out[eid] = self.cache[eid]
            elif eid not in out:
                todo.append(c)
        todo = list({c.entity.entity_id: c for c in todo}.values())
        if todo:
            texts = [
                f"{c.entity.label}: {c.entity.description}"
                if c.entity.description
                else c.entity.label
                for c in todo
            ]
            for c, v in zip(todo, self.embedder.encode(texts), strict=True):
                vec = v.astype(np.float16)
                out[c.entity.entity_id] = vec
                self.cache[c.entity.entity_id] = vec
            while len(self.cache) > self.cache_size:
                self.cache.popitem(last=False)
        return out

    # --- the batch ---------------------------------------------------------------------

    def run(
        self, docs: Sequence[Document], segments: Sequence[Sequence[Segment]], now: datetime
    ) -> list[DocEntities]:
        units: list[_Unit] = []
        for i, segs in enumerate(segments):
            for s in segs:
                if s.level is SegmentLevel.PASSAGE:
                    units.append(_Unit(i, "body", _start(s), s.text, s.position))
                elif s.level is SegmentLevel.TITLE:
                    units.append(_Unit(i, "title", 0, s.text, -1))
        raw = self.ner.predict([u.text for u in units])

        named: list[list[tuple[Mention, NamedMention]]] = [[] for _ in docs]
        for u, spans in zip(units, raw, strict=True):
            doc = docs[u.doc]
            sentences = _sentences(segments[u.doc])
            for sp in clean(u.text, spans):
                start, end = u.offset + sp.start, u.offset + sp.end
                mid = ids.hash32(
                    f"{doc.doc_id}:{doc.revision}:{u.field}:{start}:{end}:{self.ner.model_id}"
                )
                m = Mention(
                    mention_id=mid,
                    doc_id=doc.doc_id,
                    revision=doc.revision,
                    segment_id=_sentence_at(sentences, start) if u.field == "body" else None,
                    surface=u.text[sp.start : sp.end],
                    locator=TextLocator(field=u.field, start=start, end=end),
                    ner_label=sp.label,
                    ner_confidence=round(sp.score, 4),
                    ner_model_id=self.ner.model_id,
                    pipeline_version=self.pipeline_version,
                )
                named[u.doc].append((m, NamedMention(mid, m.surface, LABELS[sp.label], u.passage)))

        groups = [group_mentions([nm for _, nm in ms]) for ms in named]
        keys = {k for gs in groups for g in gs for k in candidate_keys(g)}
        by_alias = self.kb.candidates(keys)
        clusters = self.coref.clusters([d.extracted_text for d in docs])

        out: list[DocEntities] = []
        for i, doc in enumerate(docs):
            out.append(
                self._finish(doc, segments[i], named[i], groups[i], by_alias, clusters[i], now)
            )
        return out

    def _finish(
        self,
        doc: Document,
        segs: Sequence[Segment],
        named: list[tuple[Mention, NamedMention]],
        groups: list[Group],
        by_alias: dict[str, list[Candidate]],
        clusters: list[list[tuple[int, int]]],
        now: datetime,
    ) -> DocEntities:
        passages = {s.position: s for s in segs if s.level is SegmentLevel.PASSAGE}
        title = next((s for s in segs if s.level is SegmentLevel.TITLE), None)

        def context(g: Group, cands: Sequence[Candidate]) -> list[float]:
            ctx = [passages[p] for p in sorted(g.passages) if p in passages]
            if not ctx and title is not None:
                ctx = [title]
            if not ctx:
                return [0.0] * len(cands)
            vecs = self.vectors.get(ctx)
            q = np.mean([vecs[s.segment_id] for s in ctx], axis=0).astype(np.float32)
            q /= np.linalg.norm(q) or 1.0
            ev = self._entity_vectors(cands)
            return [float(np.dot(q, ev[c.entity.entity_id].astype(np.float32))) for c in cands]

        linked = link_document(groups, by_alias, context, self.kb.relations_among)
        result = DocEntities(
            mentions=[m for m, _ in named],
            groups=linked,
            named={nm.mention_id: nm for _, nm in named},
        )
        by_mention: dict[str, GroupLinks] = {}
        for gl in linked:
            for mid in gl.group.mention_ids:
                by_mention[mid] = gl
                result.links.extend(self._links(mid, gl, now))
            if gl.local_id is not None and not (
                gl.links and gl.links[0].confidence >= LINK_THRESHOLD
            ):
                result.entities.append(_local_entity(gl, now))

        coref_mentions = self._coref(doc, segs, named, by_mention, clusters, now, result)
        result.mentions.extend(coref_mentions)
        result.snippets = self._snippets(doc, segs, result, by_mention)
        result.entity_ids = sorted({gl.entity_id for gl in linked})
        return result

    def _links(self, mention_id: str, gl: GroupLinks, now: datetime) -> list[MentionLink]:
        out = [
            MentionLink(
                mention_id=mention_id,
                entity_id=s.entity_id,
                confidence=round(s.confidence, 4),
                method="kb_scored",
                linker_version=LINKER_VERSION,
                kb_snapshot=self.kb.snapshot,
                features={k: round(v, 4) for k, v in s.features.items()},
                created_at=now,
            )
            for s in gl.links
        ]
        if gl.local_id is not None and gl.entity_id == gl.local_id:
            out.append(
                MentionLink(
                    mention_id=mention_id,
                    entity_id=gl.local_id,
                    confidence=1.0,
                    method="unresolved_self",
                    linker_version=LINKER_VERSION,
                    created_at=now,
                )
            )
        return out

    def _coref(
        self,
        doc: Document,
        segs: Sequence[Segment],
        named: list[tuple[Mention, NamedMention]],
        by_mention: dict[str, GroupLinks],
        clusters: list[list[tuple[int, int]]],
        now: datetime,
        result: DocEntities,
    ) -> list[Mention]:
        """Cluster members without a name of their own inherit the cluster's entity,
        when the cluster's named mentions agree (at least two thirds of them)."""
        body = [
            (m.locator.start, m.locator.end, m.mention_id)
            for m, _ in named
            if m.locator.field == "body"
        ]
        sentences = _sentences(segs)
        out: list[Mention] = []
        text = doc.extracted_text
        for cluster in clusters:
            votes: Counter[str] = Counter()
            unnamed: list[tuple[int, int]] = []
            for s, e in cluster:
                hits = [mid for ms, me, mid in body if ms < e and s < me]
                if hits:
                    for mid in hits:
                        if mid in by_mention:
                            votes[by_mention[mid].entity_id] += 1
                elif len(text[s:e].split()) <= COREF_MAX_WORDS:
                    unnamed.append((s, e))
            if not votes or not unnamed:
                continue
            entity_id, n = votes.most_common(1)[0]
            if n * 3 < sum(votes.values()) * 2:
                continue
            gl = next(g for g in by_mention.values() if g.entity_id == entity_id)
            for s, e in unnamed:
                mid = ids.hash32(f"{doc.doc_id}:{doc.revision}:body:{s}:{e}:{self.coref.model_id}")
                out.append(
                    Mention(
                        mention_id=mid,
                        doc_id=doc.doc_id,
                        revision=doc.revision,
                        segment_id=_sentence_at(sentences, s),
                        surface=text[s:e],
                        locator=TextLocator(field="body", start=s, end=e),
                        ner_label="coref",
                        ner_model_id=self.coref.model_id,
                        pipeline_version=self.pipeline_version,
                    )
                )
                result.links.append(
                    MentionLink(
                        mention_id=mid,
                        entity_id=entity_id,
                        confidence=round(gl.confidence * 0.9, 4),
                        method="coref",
                        linker_version=LINKER_VERSION,
                        kb_snapshot=self.kb.snapshot if entity_id.startswith("wd:") else None,
                        created_at=now,
                    )
                )
                by_mention[mid] = gl
        return out

    def _snippets(
        self,
        doc: Document,
        segs: Sequence[Segment],
        result: DocEntities,
        by_mention: dict[str, GroupLinks],
    ) -> list[Snippet]:
        """The mention's sentence plus a neighbour either side, once per entity and window."""
        sentences = _sentences(segs)
        starts = [s.locator.start for s in sentences]  # type: ignore[union-attr]
        seen: set[tuple[str, str, int, int]] = set()
        out: list[Snippet] = []
        for m in result.mentions:
            gl = by_mention.get(m.mention_id)
            if gl is None:
                continue
            entity_id = gl.entity_id
            loc = m.locator
            assert isinstance(loc, TextLocator)
            if loc.field == "title":
                f, start, end, text = "title", 0, len(doc.title or ""), doc.title or ""
            else:
                i = max(0, bisect.bisect_right(starts, loc.start) - 1)
                lo = sentences[max(0, i - SNIPPET_NEIGHBOURS)].locator
                hi = sentences[min(len(sentences) - 1, i + SNIPPET_NEIGHBOURS)].locator
                f, start, end = "body", lo.start, hi.end  # type: ignore[union-attr]
                text = doc.extracted_text[start:end]
            key = (entity_id, f, start, end)
            if key in seen or not text:
                continue
            seen.add(key)
            out.append(
                Snippet(
                    snippet_id=ids.hash32(
                        f"{doc.doc_id}:{doc.revision}:{entity_id}:{f}:{start}:{end}:"
                        f"{self.pipeline_version}"
                    ),
                    doc_id=doc.doc_id,
                    revision=doc.revision,
                    entity_id=entity_id,
                    mention_id=m.mention_id,
                    locator=TextLocator(field=f, start=start, end=end),
                    window_words=max(1, len(text.split())),
                    pipeline_version=self.pipeline_version,
                )
            )
        return out


def _start(s: Segment) -> int:
    loc = s.locator
    assert isinstance(loc, TextLocator)
    return loc.start


def _sentences(segs: Sequence[Segment]) -> list[Segment]:
    return sorted((s for s in segs if s.level is SegmentLevel.SENTENCE), key=_start)


def _sentence_at(sentences: list[Segment], offset: int) -> str | None:
    starts = [_start(s) for s in sentences]
    i = bisect.bisect_right(starts, offset) - 1
    return sentences[i].segment_id if i >= 0 else None


def _local_entity(gl: GroupLinks, now: datetime) -> Entity:
    names = Counter(gl.group.surfaces)
    canonical = names.most_common(1)[0][0]
    assert gl.local_id is not None
    return Entity(
        entity_id=gl.local_id,
        canonical_name=canonical,
        type=gl.group.type,
        aliases=sorted({normalize_alias(s) for s in names} - {normalize_alias(canonical)}),
        created_at=now,
    )


__all__ = ["DocEntities", "EntityStage", "EntityType", "VectorSource"]
