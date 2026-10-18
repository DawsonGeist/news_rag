"""Derived records (Phase 2+). PROVISIONAL: may change without a major bump until the
component that produces them ships; see contracts/v1/README.md "Stability".

All of these are rebuildable from Documents. Every record names the Document
revision it was derived from, because locators index that revision's text.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from newsrag.contracts.v1.common import (
    SCHEMA_VERSION,
    Confidence,
    ContractModel,
    Hex32,
    Locator,
    SchemaVersion,
    TextLocator,
    UtcDatetime,
)


class SegmentLevel(StrEnum):
    PARAGRAPH = "paragraph"
    SENTENCE = "sentence"


class Segment(ContractModel):
    """A paragraph or sentence of a Document revision.

    ``segment_id = hash32(f"{doc_id}:{revision}:{level}:{start}:{end}:{chunker_version}")``,
    so re-running the same chunker is idempotent. Sentences point to their paragraph.
    For text, ``text == extracted_text[locator.start:locator.end]``.
    """

    schema_version: SchemaVersion = SCHEMA_VERSION
    segment_id: Hex32
    doc_id: Hex32
    revision: int = Field(ge=1)
    level: SegmentLevel
    parent_id: Hex32 | None = None
    position: int = Field(ge=0, description="Order within (doc, revision, level).")
    text: str
    locator: Locator
    chunker_version: str
    pipeline_version: str


class EmbeddingTarget(StrEnum):
    PARAGRAPH = "paragraph"
    SENTENCE = "sentence"
    SNIPPET = "snippet"


class Embedding(ContractModel):
    """Interchange form of a vector. At rest, vectors live in the ANN engine's own format,
    keyed by (target_kind, target_id, model_id). Article vectors are derived (mean of
    paragraph vectors) and never stored as Embedding rows."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    target_kind: EmbeddingTarget
    target_id: Hex32
    model_id: str = Field(
        description="Exact model name and revision, e.g. 'BAAI/bge-small-en-v1.5@<sha>'."
    )
    dims: int = Field(gt=0)
    vector: list[float]
    normalized: bool = True
    embedded_at: UtcDatetime
    pipeline_version: str


class Mention(ContractModel):
    """An NER span. ``locator.field == "title"`` means the article is *about* the entity
    rather than merely mentioning it (the overview's ``in_title``)."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    mention_id: Hex32
    doc_id: Hex32
    revision: int = Field(ge=1)
    segment_id: Hex32 | None = Field(default=None, description="Sentence segment; None for title.")
    surface: str
    locator: TextLocator
    ner_label: str
    ner_confidence: Confidence | None = None
    ner_model_id: str
    pipeline_version: str


class EntityType(StrEnum):
    PERSON = "person"
    ORGANIZATION = "organization"
    LOCATION = "location"
    EVENT = "event"
    LAW = "law"  # statutes, bills, court cases
    PRODUCT = "product"
    WORK = "work"  # books, films, reports
    OTHER = "other"


class Entity(ContractModel):
    """A node in the entity map. ``entity_id`` is ``"wd:<QID>"`` when linked to Wikidata,
    else ``"local:<hash32(type + ':' + normalized surface)>"`` for unresolved mentions,
    which stay as their own nodes rather than being forced onto a KB entry."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    entity_id: str
    canonical_name: str
    type: EntityType
    aliases: list[str] = Field(default_factory=list)
    kb_id: str | None = None
    description: str | None = None
    created_at: UtcDatetime


class MentionLink(ContractModel):
    """Scored edge from a Mention to a candidate Entity. Never a hard merge: one mention
    may have several candidate links ("Paris" the city vs. the person)."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    mention_id: Hex32
    entity_id: str
    confidence: Confidence
    method: str = Field(
        description="e.g. 'exact_alias', 'context_model', 'coref', 'unresolved_self'."
    )
    linker_version: str
    features: dict[str, float] = Field(default_factory=dict)
    created_at: UtcDatetime


class StanceLabel(StrEnum):
    SUPPORTS = "supports"
    NEUTRAL = "neutral"
    OPPOSES = "opposes"


class Snippet(ContractModel):
    """N words either side of a resolved mention: the unit stance is judged on, and the join
    point between the entity map and the vector index."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    snippet_id: Hex32
    doc_id: Hex32
    revision: int = Field(ge=1)
    entity_id: str
    mention_id: Hex32
    text: str
    locator: TextLocator
    window_words: int = Field(gt=0)
    stance_score: float | None = Field(default=None, ge=-1.0, le=1.0)
    stance_label: StanceLabel | None = None
    stance_model_id: str | None = None
    pipeline_version: str


class RelationType(StrEnum):
    DUP_OF = "dup_of"  # identical content_hash
    NEAR_DUP_OF = "near_dup_of"  # e.g. wire story republished with edits
    PUBLISHED_BY = "published_by"


class DocRelation(ContractModel):
    """Typed, scored edge between documents (or document -> source). Only relation types
    with measured high accuracy are written."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    relation: RelationType
    src_doc_id: Hex32
    dst_id: str = Field(description="doc_id for dup relations, source_id for published_by.")
    score: Confidence
    method: str
    created_at: UtcDatetime
