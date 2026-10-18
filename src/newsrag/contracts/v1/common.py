"""Shared types for v1 contracts: base model, ID and time types, locators."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from newsrag.timeutil import ensure_utc

SCHEMA_VERSION = "1.0"
"""Major.minor. Minor bumps are additive (new optional fields). Major bumps live in contracts/v2."""


class ContractModel(BaseModel):
    """Base for every contract record: immutable, unknown fields rejected."""

    model_config = ConfigDict(extra="forbid", frozen=True)


Hex32 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{32}$")]
"""32 lowercase hex characters (truncated sha256), used for doc/url/feed/segment IDs."""

Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
"""Full sha256, used for content hashes and blob addresses."""

UtcDatetime = Annotated[datetime, AfterValidator(ensure_utc)]
"""Timezone-aware datetime, normalized to UTC. Naive datetimes are rejected."""

Confidence = Annotated[float, Field(ge=0.0, le=1.0)]

SchemaVersion = Annotated[str, StringConstraints(pattern=r"^1\.\d+$")]


# Locators point from a derived record back into its source.
# Offsets are Unicode code points (Python str indices) into the referenced text,
# half-open [start, end). JavaScript clients must convert from UTF-16 indices.


class TextLocator(ContractModel):
    """Span of a Document's ``extracted_text`` (field="body") or ``title`` (field="title")."""

    kind: Literal["text"] = "text"
    field: Literal["body", "title"] = "body"
    start: int = Field(ge=0)
    end: int = Field(ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.end < self.start:
            raise ValueError("end < start")
        return self


class TimeLocator(ContractModel):
    """Time range in an audio or video source, milliseconds from the start."""

    kind: Literal["time"] = "time"
    start_ms: int = Field(ge=0)
    end_ms: int = Field(ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.end_ms < self.start_ms:
            raise ValueError("end_ms < start_ms")
        return self


class PageLocator(ContractModel):
    """Span on one page of a paged source (court PDFs). Offsets index that page's text."""

    kind: Literal["page"] = "page"
    page: int = Field(ge=1)
    start: int = Field(ge=0)
    end: int = Field(ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.end < self.start:
            raise ValueError("end < start")
        return self


Locator = Annotated[TextLocator | TimeLocator | PageLocator, Field(discriminator="kind")]
