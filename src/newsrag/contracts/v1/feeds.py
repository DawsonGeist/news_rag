"""Feed list and feed vetting contracts (components 1 and 2)."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from newsrag.contracts.v1.common import (
    SCHEMA_VERSION,
    Confidence,
    ContractModel,
    Hex32,
    SchemaVersion,
    UtcDatetime,
)


class FeedMedium(StrEnum):
    TEXT = "text"
    AUDIO = "audio"  # podcasts: future transcription pipeline
    VIDEO = "video"


class FeedOrigin(StrEnum):
    FEEDS_YAML = "feeds_yaml"
    SUPPLEMENT = "supplement"
    MANUAL = "manual"


class FeedCandidate(ContractModel):
    """A feed from a curated list, before vetting."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    feed_id: Hex32
    feed_url: str
    source_id: str = Field(description="Registered domain of the listed publisher.")
    listed_domain: str = Field(description="Domain key exactly as it appears in the list.")
    country: str
    categories: list[str] = Field(min_length=1, description="First entry is the primary category.")
    medium: FeedMedium
    origin: FeedOrigin
    listed_languages: list[str] = Field(default_factory=list)
    listed_rank: int | None = None
    listed_avg_article_length: int | None = None
    display_name: str | None = None
    notes: str | None = None


class FeedVerdict(StrEnum):
    ACCEPT = "accept"
    REJECT = "reject"
    REVIEW = "review"  # borderline: a human decides


class FeedIssue(StrEnum):
    """Reason codes attached to a FeedHealthReport."""

    UNREACHABLE = "unreachable"
    HTTP_ERROR = "http_error"
    UNPARSEABLE = "unparseable"
    EMPTY = "empty"
    STALE = "stale"  # newest item older than the staleness threshold
    AUDIO_FEED = "audio_feed"
    ROBOTS_DISALLOWED = "robots_disallowed"
    PAYWALLED = "paywalled"
    SUMMARY_ONLY = "summary_only"  # article pages yield no more than the feed summary
    NON_ENGLISH = "non_english"
    NO_DATES = "no_dates"
    LOW_WORD_COUNT = "low_word_count"
    ARTICLE_FETCH_FAILED = "article_fetch_failed"
    DUPLICATE_FEED = "duplicate_feed"  # items fully covered by another accepted feed


class FeedHealthReport(ContractModel):
    """Result of vetting one feed. Ratios are over the items or sampled articles checked."""

    schema_version: SchemaVersion = SCHEMA_VERSION
    feed_id: Hex32
    feed_url: str
    checked_at: UtcDatetime
    vetter_version: str

    # Feed document
    http_status: int | None = None
    final_feed_url: str | None = None
    feed_format: str | None = Field(default=None, description="rss, atom, json or unknown.")
    item_count: int = Field(default=0, ge=0)
    items_with_date_ratio: Confidence = 0.0
    newest_item_at: UtcDatetime | None = None
    est_items_per_day: float | None = Field(default=None, ge=0.0)
    audio_enclosure_ratio: Confidence = 0.0

    # Politeness
    robots_allows_feed: bool | None = None
    robots_allows_articles: bool | None = None
    crawl_delay_s: float | None = Field(default=None, ge=0.0)

    # Sampled articles
    sampled_articles: int = Field(default=0, ge=0)
    sample_fetch_ok: int = Field(default=0, ge=0)
    full_text_ratio: Confidence = 0.0
    paywall_ratio: Confidence = 0.0
    english_ratio: Confidence = 0.0
    published_at_found_ratio: Confidence = 0.0
    median_word_count: int | None = Field(default=None, ge=0)

    verdict: FeedVerdict
    issues: list[FeedIssue] = Field(default_factory=list)
    detail: str | None = None
