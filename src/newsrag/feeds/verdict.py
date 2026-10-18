"""Feed vetting rules (phase1-design.md, component 1): pure functions from what was
observed to a FeedHealthReport and a verdict. No I/O, so every rule is unit-tested.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from statistics import median

from newsrag.config import VetSettings
from newsrag.contracts.v1 import ContentKind, FeedHealthReport, FeedIssue, FeedVerdict

VETTER_VERSION = "vet-2"

# Verdicts map onto feeds.status.
STATUS_FOR = {
    FeedVerdict.ACCEPT: "active",
    FeedVerdict.REVIEW: "review",
    FeedVerdict.REJECT: "rejected",
}


@dataclass
class FeedCheck:
    """Stage B: the feed document itself."""

    http_status: int | None = None
    final_feed_url: str | None = None
    reachable: bool = False
    robots_allows_feed: bool | None = None
    parse_ok: bool = False
    feed_format: str | None = None
    item_dates: list[datetime | None] = field(default_factory=list)
    audio_items: int = 0
    crawl_delay_s: float | None = None
    error: str | None = None

    @property
    def item_count(self) -> int:
        return len(self.item_dates)


@dataclass(frozen=True)
class Sample:
    """Stage C: one sampled article. ``kind`` is None when the fetch failed."""

    url: str
    kind: ContentKind | None
    language: str | None = None
    has_date: bool = False
    words: int = 0
    robots_disallowed: bool = False
    error: str | None = None  # why the fetch or extraction failed, for reviewers


def items_per_day(dates: Sequence[datetime | None]) -> float | None:
    """Rate from the span of dated items; None with fewer than two dates."""
    dated = sorted(d for d in dates if d is not None)
    if len(dated) < 2:
        return None
    span_days = (dated[-1] - dated[0]).total_seconds() / 86400
    # Items stamped the same minute (bulk imports) would give an absurd rate.
    return round((len(dated) - 1) / max(span_days, 1 / 24), 2)


# Which issue explains a low full-text ratio, by the most common non-full kind.
_KIND_ISSUE = {
    ContentKind.SUMMARY_ONLY: FeedIssue.SUMMARY_ONLY,
    ContentKind.SUSPECTED_PAYWALL: FeedIssue.PAYWALLED,
    ContentKind.NON_ARTICLE: FeedIssue.LOW_WORD_COUNT,
}


def _why_not_full(ok: Sequence[Sample]) -> FeedIssue:
    counts = Counter(s.kind for s in ok if s.kind is not ContentKind.FULL_TEXT)
    kind = counts.most_common(1)[0][0] if counts else None
    return _KIND_ISSUE.get(kind, FeedIssue.LOW_WORD_COUNT) if kind else FeedIssue.LOW_WORD_COUNT


def _ratio(n: int, d: int) -> float:
    return round(n / d, 4) if d else 0.0


def build_report(
    feed_id: str,
    feed_url: str,
    check: FeedCheck,
    samples: Sequence[Sample],
    checked_at: datetime,
    cfg: VetSettings,
    duplicate_of: str | None = None,
) -> FeedHealthReport:
    """Assemble the report and decide the verdict. ``duplicate_of`` names an earlier feed
    of the same publisher with exactly the same items; polling both is wasted traffic."""
    dates = check.item_dates
    newest = max((d for d in dates if d is not None), default=None)
    ok = [s for s in samples if s.kind is not None]
    issues: list[FeedIssue] = []
    reject = False

    def flag(issue: FeedIssue, is_reject: bool) -> None:
        nonlocal reject
        if issue not in issues:
            issues.append(issue)
        reject = reject or is_reject

    full = _ratio(sum(s.kind is ContentKind.FULL_TEXT for s in ok), len(ok))
    paywall = _ratio(sum(s.kind is ContentKind.SUSPECTED_PAYWALL for s in ok), len(ok))
    english = _ratio(sum(s.language == "en" for s in ok), len(ok))
    dated = _ratio(sum(s.has_date for s in ok), len(ok))
    words = int(median(s.words for s in ok)) if ok else None

    # Rejections: the feed is unusable or out of scope.
    if check.robots_allows_feed is False:
        flag(FeedIssue.ROBOTS_DISALLOWED, True)
    elif not check.reachable:
        flag(FeedIssue.HTTP_ERROR if check.http_status else FeedIssue.UNREACHABLE, True)
    elif not check.parse_ok:
        flag(FeedIssue.UNPARSEABLE, True)
    elif check.item_count == 0:
        flag(FeedIssue.EMPTY, True)
    else:
        if newest is not None and checked_at - newest > timedelta(days=cfg.stale_days):
            flag(FeedIssue.STALE, True)
        if check.audio_items and check.audio_items >= check.item_count / 2:
            flag(FeedIssue.AUDIO_FEED, True)
        if duplicate_of is not None:
            flag(FeedIssue.DUPLICATE_FEED, True)
        if samples and all(s.robots_disallowed for s in samples):
            flag(FeedIssue.ROBOTS_DISALLOWED, True)
        elif samples and not ok:
            flag(FeedIssue.ARTICLE_FETCH_FAILED, True)
        if ok:
            if paywall >= cfg.reject_paywall_ratio:
                flag(FeedIssue.PAYWALLED, True)
            if english < cfg.reject_english_ratio:
                flag(FeedIssue.NON_ENGLISH, True)
            # Review: usable, but a human should look.
            low_full = full < cfg.review_full_text_ratio
            if low_full:
                flag(_why_not_full(ok), False)
            if dated < cfg.review_date_ratio:
                flag(FeedIssue.NO_DATES, False)
            # A low median that the non-full samples already explain is not a second issue.
            if not low_full and words is not None and words < cfg.review_median_words:
                flag(FeedIssue.LOW_WORD_COUNT, False)

    if reject:
        verdict = FeedVerdict.REJECT
    elif issues:
        verdict = FeedVerdict.REVIEW
    else:
        verdict = FeedVerdict.ACCEPT
    return FeedHealthReport(
        feed_id=feed_id,
        feed_url=feed_url,
        checked_at=checked_at,
        vetter_version=VETTER_VERSION,
        http_status=check.http_status,
        final_feed_url=check.final_feed_url,
        feed_format=check.feed_format,
        item_count=check.item_count,
        items_with_date_ratio=_ratio(sum(d is not None for d in dates), len(dates)),
        newest_item_at=newest,
        est_items_per_day=items_per_day(dates),
        audio_enclosure_ratio=_ratio(check.audio_items, check.item_count),
        robots_allows_feed=check.robots_allows_feed,
        robots_allows_articles=(
            None if not samples else not all(s.robots_disallowed for s in samples)
        ),
        crawl_delay_s=check.crawl_delay_s,
        sampled_articles=len(samples),
        sample_fetch_ok=len(ok),
        full_text_ratio=full,
        paywall_ratio=paywall,
        english_ratio=english,
        published_at_found_ratio=dated,
        median_word_count=words,
        verdict=verdict,
        issues=issues,
        detail=_detail(check, samples, duplicate_of),
    )


def _detail(check: FeedCheck, samples: Sequence[Sample], duplicate_of: str | None) -> str | None:
    """Human-readable context for a reviewer: errors, sample kinds and sample errors."""
    parts = [check.error] if check.error else []
    if duplicate_of:
        parts.append(f"same items as {duplicate_of}")
    kinds = Counter(s.kind.value for s in samples if s.kind is not None)
    if kinds:
        parts.append("samples: " + ", ".join(f"{k} {n}" for k, n in sorted(kinds.items())))
    errors = Counter(s.error for s in samples if s.kind is None and s.error)
    if errors:
        parts.append("sample errors: " + ", ".join(f"{k} {n}" for k, n in sorted(errors.items())))
    return "; ".join(parts)[:1000] or None


def audio_report(feed_id: str, feed_url: str, checked_at: datetime) -> FeedHealthReport:
    """Podcast candidates get a verdict without any network traffic: out of scope for
    the text pipeline, kept as candidates for a future transcription pipeline."""
    return FeedHealthReport(
        feed_id=feed_id,
        feed_url=feed_url,
        checked_at=checked_at,
        vetter_version=VETTER_VERSION,
        verdict=FeedVerdict.REJECT,
        issues=[FeedIssue.AUDIO_FEED],
        detail="medium=audio: not vetted in Phase 1",
    )
