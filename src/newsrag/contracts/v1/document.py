"""Document: the single record every pipeline emits. One row per (doc_id, revision)."""

from __future__ import annotations

import unicodedata
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, model_validator

from newsrag.contracts.v1.common import (
    SCHEMA_VERSION,
    Confidence,
    ContractModel,
    Hex32,
    SchemaVersion,
    Sha256Hex,
    UtcDatetime,
)


class Pipeline(StrEnum):
    RSS = "rss"
    YOUTUBE = "youtube"
    COURT = "court"


class ContentKind(StrEnum):
    FULL_TEXT = "full_text"
    """The page's main text was extracted."""
    SUMMARY_ONLY = "summary_only"
    """Only a teaser or feed summary was available; extracted_text is that summary."""
    SUSPECTED_PAYWALL = "suspected_paywall"
    """Paywall markers found or text truncated; extracted_text is whatever was visible."""
    NON_ARTICLE = "non_article"
    """Video page, gallery, live-blog shell, index page: no article body."""


class DateSource(StrEnum):
    """Where ``published_at`` came from, most to least trustworthy."""

    JSON_LD = "json_ld"
    META_TAG = "meta_tag"
    FEED = "feed"
    URL_PATH = "url_path"
    PAGE_TEXT = "page_text"
    PLATFORM = "platform"  # API-provided (YouTube upload time, CourtListener filing date)
    NONE = "none"


class Document(ContractModel):
    """One revision of one piece of content.

    Text rules for ``extracted_text`` (checked on construction): NFC-normalized,
    paragraphs separated by exactly one blank line ("\\n\\n"), no other newlines,
    no carriage returns, no leading or trailing whitespace. Downstream locators
    index this exact string, so it is never modified after storage; a changed
    text is a new revision.
    """

    schema_version: SchemaVersion = SCHEMA_VERSION

    # Identity
    doc_id: Hex32
    revision: int = Field(ge=1)
    pipeline: Pipeline
    source_id: str = Field(min_length=1, description="Publisher key, e.g. 'foxnews.com'.")
    canonical_url: str
    url_canonicalizer_version: str
    final_url: str = Field(description="URL actually fetched after redirects, normalized.")

    # Content
    title: str | None = None
    authors: list[str] = Field(default_factory=list)
    extracted_text: str
    content_kind: ContentKind
    language: str | None = Field(default=None, description="ISO 639-1, e.g. 'en'.")
    language_confidence: Confidence | None = None
    word_count: int = Field(ge=0)

    # Time (all UTC; published_at is never back-filled from ingest times)
    published_at: UtcDatetime | None = None
    published_at_source: DateSource = DateSource.NONE
    modified_at: UtcDatetime | None = None
    discovered_at: UtcDatetime
    fetched_at: UtcDatetime
    ingested_at: UtcDatetime

    # Provenance
    feed_id: Hex32 | None = Field(default=None, description="First feed that surfaced this URL.")
    feed_title: str | None = None
    feed_summary: str | None = None
    content_hash: Sha256Hex
    raw_blob_hash: Sha256Hex
    extractor_version: str
    pipeline_version: str
    pipeline_meta: dict[str, Any] = Field(
        default_factory=dict,
        description="Pipeline-specific extras (video_id, docket_id, ...). JSON-serializable.",
    )

    @model_validator(mode="after")
    def _check(self) -> Self:
        text = self.extracted_text
        if text != text.strip():
            raise ValueError("extracted_text has leading/trailing whitespace")
        if "\r" in text:
            raise ValueError("extracted_text contains \\r")
        if text.replace("\n\n", "").count("\n") or "\n\n\n" in text:
            raise ValueError("extracted_text paragraphs must be separated by exactly '\\n\\n'")
        if not unicodedata.is_normalized("NFC", text):
            raise ValueError("extracted_text is not NFC-normalized")
        if not text and self.content_kind in (ContentKind.FULL_TEXT, ContentKind.SUMMARY_ONLY):
            raise ValueError(f"empty extracted_text not allowed for {self.content_kind}")
        if (self.published_at is None) != (self.published_at_source is DateSource.NONE):
            raise ValueError("published_at and published_at_source must be set together")
        if not self.discovered_at <= self.fetched_at <= self.ingested_at:
            raise ValueError("expected discovered_at <= fetched_at <= ingested_at")
        return self
