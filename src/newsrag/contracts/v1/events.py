"""StageEvent: one row per stage execution. Drives every lag metric."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from newsrag.contracts.v1.common import (
    SCHEMA_VERSION,
    ContractModel,
    Hex32,
    SchemaVersion,
    UtcDatetime,
)


class Stage(StrEnum):
    DISCOVER = "discover"  # URL first seen in a feed
    FETCH = "fetch"
    EXTRACT = "extract"
    STORE = "store"  # Document committed. "Searchable" in Phase 1.
    SEGMENT = "segment"
    EMBED = "embed"
    NER = "ner"
    LINK = "link"
    INDEX = "index"  # visible to the query service. "Searchable" from Phase 2.


class StageStatus(StrEnum):
    OK = "ok"
    RETRY = "retry"  # failed, will be retried
    ERROR = "error"  # failed permanently
    SKIPPED = "skipped"  # deliberately not processed (robots, non-HTML, duplicate)


class StageEvent(ContractModel):
    """``enqueued_at`` is when the item became ready for this stage (usually the previous
    stage's ``finished_at``); the gap to ``started_at`` is queueing time."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    event_id: str = Field(description="ULID; sortable by creation time.")
    stage: Stage
    status: StageStatus
    url_id: Hex32 | None = None
    doc_id: Hex32 | None = None
    revision: int | None = Field(default=None, ge=1)
    feed_id: Hex32 | None = None
    worker_id: str = Field(description="host:process:task, e.g. 'laptop:fetch-1234:7'.")
    attempt: int = Field(default=1, ge=1)
    enqueued_at: UtcDatetime | None = None
    started_at: UtcDatetime
    finished_at: UtcDatetime
    error_code: str | None = None
    detail: str | None = Field(default=None, max_length=500)
    pipeline_version: str
