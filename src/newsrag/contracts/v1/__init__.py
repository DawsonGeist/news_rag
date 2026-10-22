"""Contract v1. See contracts/v1/README.md for the normative rules."""

from newsrag.contracts.v1.common import (
    SCHEMA_VERSION,
    Locator,
    PageLocator,
    TextLocator,
    TimeLocator,
)
from newsrag.contracts.v1.document import ContentKind, DateSource, Document, Pipeline
from newsrag.contracts.v1.enrichment import (
    DocRelation,
    Embedding,
    EmbeddingTarget,
    Entity,
    EntityRelation,
    EntityType,
    Mention,
    MentionLink,
    RelationSource,
    RelationType,
    Segment,
    SegmentLevel,
    Snippet,
    StanceLabel,
)
from newsrag.contracts.v1.events import Stage, StageEvent, StageStatus
from newsrag.contracts.v1.feeds import (
    FeedCandidate,
    FeedHealthReport,
    FeedIssue,
    FeedMedium,
    FeedOrigin,
    FeedVerdict,
)

# Records with an exported JSON Schema in contracts/v1/schemas/. Stable unless noted.
STABLE_RECORDS = (Document, StageEvent, FeedCandidate, FeedHealthReport)
PROVISIONAL_RECORDS = (
    Segment,
    Embedding,
    Mention,
    Entity,
    MentionLink,
    EntityRelation,
    Snippet,
    DocRelation,
)

__all__ = [
    "SCHEMA_VERSION",
    "STABLE_RECORDS",
    "PROVISIONAL_RECORDS",
    "ContentKind",
    "DateSource",
    "DocRelation",
    "Document",
    "Embedding",
    "EmbeddingTarget",
    "Entity",
    "EntityRelation",
    "EntityType",
    "FeedCandidate",
    "FeedHealthReport",
    "FeedIssue",
    "FeedMedium",
    "FeedOrigin",
    "FeedVerdict",
    "Locator",
    "Mention",
    "MentionLink",
    "PageLocator",
    "Pipeline",
    "RelationSource",
    "RelationType",
    "Segment",
    "SegmentLevel",
    "Snippet",
    "Stage",
    "StageEvent",
    "StageStatus",
    "StanceLabel",
    "TextLocator",
    "TimeLocator",
]
