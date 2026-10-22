"""Component 4, extractor: raw page bytes + frontier hints -> Document.

A pure, deterministic function, so it can run in a process pool, on another machine,
or again over the archive (G1.6: the same blob and version give the same
``content_hash``). Any change to its output for an existing input must bump
``EXTRACTOR_VERSION``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from urllib.parse import urlsplit

import trafilatura
from py3langid.langid import MODEL_FILE, LanguageIdentifier
from trafilatura.utils import decode_file

from newsrag import ids
from newsrag.contracts.v1 import ContentKind, DateSource, Document, Pipeline
from newsrag.extract.dates import parse_datetime, plausible, url_path_date
from newsrag.extract.page import (
    PageMeta,
    UnparseableHTML,
    jsonld_authors,
    jsonld_is_free,
    parse_page,
)
from newsrag.extract.text import (
    html_fragment_to_paragraphs,
    join_paragraphs,
    normalize_paragraph,
    normalize_title,
    word_count,
)

EXTRACTOR_VERSION = "extract-3"
"""Hand-bumped whenever output changes for an existing input."""

NON_ARTICLE_MAX_WORDS = 80
# A feed summary at least this long is the article itself (a full-content feed), not a
# teaser. extract-2: before this, such feeds were misread as summary_only.
TEASER_MAX_WORDS = 150
PAYWALL_SHORT_WORDS = 250
LANGUAGE_SAMPLE_CHARS = 2000
# Below this, py3langid is guessing (a bare English headline can come out as 'nl' at
# 0.04). An unknown language is better than a wrong one that downstream trusts.
LANGUAGE_MIN_CONFIDENCE = 0.5

# Meta keys (lowercased) that carry a publication time, most specific first.
PUBLISHED_META = (
    "article:published_time",
    "og:article:published_time",
    "datepublished",
    "parsely-pub-date",
    "sailthru.date",
    "pubdate",
    "publishdate",
    "publish-date",
    "dc.date.issued",
    "dcterms.created",
    "date",
)
MODIFIED_META = ("article:modified_time", "og:updated_time", "datemodified")

# Class/id markup that subscription walls leave behind, matched only in element
# attributes, and only counted together with short text. Vendor names (Piano, Tinypass)
# are deliberately absent: free sites such as NPR use them for donation prompts.
PAYWALL_MARKERS = re.compile(
    r"paywall|regwall|subscriber[-_ ]?only|subscribers[-_ ]?only|premium[-_]content|"
    r"metered[-_]?content|article[-_]locked|subscription[-_]required|"
    # Ghost's members-only gate (extract-3; foodsafetynews.com previews read as full text).
    r"post-upgrade-cta|members[-_]?only",
    re.IGNORECASE,
)

# Markup that is never visible article text, pruned before extraction (extract-3).
# <template> is inert until a script clones it, but trafilatura reads it; notus.org's
# sign-in modal ("New code will be available in 1:00") leaked into article bodies.
INVISIBLE_XPATH = [
    "//template",
    "//dialog",
    "//*[@role='dialog']",
    "//*[@aria-modal='true']",
]

_CHARSET = re.compile(r"charset\s*=\s*[\"']?([\w.:-]+)", re.IGNORECASE)
_META_CHARSET = re.compile(rb"<meta[^>]+charset\s*=\s*[\"']?([\w.:-]+)", re.IGNORECASE)
META_SNIFF_BYTES = 4096


@dataclass(frozen=True)
class ExtractContext:
    """Everything the extractor knows besides the page bytes (frontier hints)."""

    final_url: str
    source_id: str
    discovered_at: datetime
    fetched_at: datetime
    pipeline_version: str
    content_type: str | None = None
    feed_id: str | None = None
    feed_title: str | None = None
    feed_summary: str | None = None
    feed_published_at: datetime | None = None


@dataclass(frozen=True)
class ExtractFailure:
    error_code: str
    detail: str = ""


def extractor_version() -> str:
    return f"{EXTRACTOR_VERSION}+trafilatura-{trafilatura.__version__}"


@lru_cache(maxsize=1)
def _langid() -> LanguageIdentifier:
    return LanguageIdentifier.from_model_file(MODEL_FILE, norm_probs=True)


def _try_decode(body: bytes, charset: str | bytes | None) -> str | None:
    if not charset:
        return None
    name = charset.decode("ascii", "ignore") if isinstance(charset, bytes) else charset
    try:
        return body.decode(name)
    except (LookupError, UnicodeDecodeError):
        return None


def decode_html(body: bytes, content_type: str | None) -> str:
    """Declared charsets first (HTTP header, then <meta>), each only if it decodes
    cleanly; then strict UTF-8; statistical detection only as a last resort, because
    it is unreliable on short or mostly-ASCII pages."""
    http = _CHARSET.search(content_type or "")
    meta = _META_CHARSET.search(body[:META_SNIFF_BYTES])
    for charset in (http.group(1) if http else None, meta.group(1) if meta else None, "utf-8"):
        if (text := _try_decode(body, charset)) is not None:
            return text
    return decode_file(body)


def detect_language(text: str) -> tuple[str | None, float | None]:
    sample = text[:LANGUAGE_SAMPLE_CHARS]
    if not sample.strip():
        return None, None
    lang, prob = _langid().classify(sample)
    if prob < LANGUAGE_MIN_CONFIDENCE:
        return None, None
    return str(lang), round(float(prob), 4)


def choose_published(
    page: PageMeta, ctx: ExtractContext, page_text_date: str | None
) -> tuple[datetime | None, DateSource]:
    candidates: list[tuple[DateSource, datetime | None]] = [
        (DateSource.JSON_LD, parse_datetime(page.first_article("datePublished"))),
        *[(DateSource.META_TAG, parse_datetime(page.meta.get(k))) for k in PUBLISHED_META],
        (DateSource.FEED, ctx.feed_published_at),
        (DateSource.URL_PATH, url_path_date(ctx.final_url)),
        (DateSource.PAGE_TEXT, parse_datetime(page_text_date)),
    ]
    for source, dt in candidates:
        if (ok := plausible(dt, ctx.fetched_at)) is not None:
            return ok, source
    return None, DateSource.NONE


def choose_modified(page: PageMeta, ctx: ExtractContext) -> datetime | None:
    for raw in (page.first_article("dateModified"), *(page.meta.get(k) for k in MODIFIED_META)):
        if (dt := plausible(parse_datetime(raw), ctx.fetched_at)) is not None:
            return dt
    return None


def body_paragraphs(text: str | None, title: str | None) -> list[str]:
    """trafilatura's text is one block per line. Its first line often repeats the
    headline, which is already in ``title``, either alone or fused to the next block
    ("...shootingNPR's Joel Rose talks"); list items carry a "- " marker."""
    lines = [normalize_paragraph(x) for x in (text or "").split("\n")]
    lines = [x.removeprefix("- ") for x in lines if x]
    if lines and title and lines[0].casefold().startswith(title.casefold()):
        rest = lines[0][len(title) :].strip()
        lines = ([rest] if rest else []) + lines[1:]
    return lines


def declares_non_article(page: PageMeta, url: str) -> bool:
    """Index-like pages: no article JSON-LD, and either the site root or an Open Graph
    type other than article (homepages say "website"). Long headline lists on such
    pages would otherwise pass the word-count test."""
    if page.articles:
        return False
    og_type = (page.meta.get("og:type") or "").strip().lower()
    return urlsplit(url).path in ("", "/") or og_type not in ("", "article")


def classify(text: str, summary: str, page: PageMeta, url: str) -> tuple[ContentKind, str]:
    """Pick the content kind. When the feed summary is at least as long as the page
    text, the summary becomes the text (summary_only, or full_text for full-content
    feeds)."""
    words = word_count(text)
    if jsonld_is_free(page) is False:
        return ContentKind.SUSPECTED_PAYWALL, text
    if PAYWALL_MARKERS.search(page.attr_tokens) and words < PAYWALL_SHORT_WORDS and page.articles:
        return ContentKind.SUSPECTED_PAYWALL, text
    if (words < NON_ARTICLE_MAX_WORDS and not page.articles) or declares_non_article(page, url):
        return ContentKind.NON_ARTICLE, text
    summary_words = word_count(summary)
    if summary and words <= summary_words:
        # The page gave no more than the feed. A long summary is the full article
        # carried in the feed; a short one is a teaser.
        if summary_words >= TEASER_MAX_WORDS:
            return ContentKind.FULL_TEXT, summary
        return ContentKind.SUMMARY_ONLY, summary
    if not text:
        # An article page with no extractable body: most likely rendered or walled.
        return ContentKind.SUSPECTED_PAYWALL, text
    return ContentKind.FULL_TEXT, text


def extract(body: bytes, ctx: ExtractContext) -> Document | ExtractFailure:
    try:
        html = decode_html(body, ctx.content_type)
        page = parse_page(html)
    except (UnparseableHTML, UnicodeDecodeError) as e:
        return ExtractFailure("unparseable", str(e)[:200])

    final_url = ids.normalize_url(ctx.final_url)
    canonical = ids.choose_canonical(final_url, page.canonical)
    # Default (balanced) mode, not favor_precision: precision mode drops the text of
    # inline links ("During a [CBS News town hall], a man...") and leaves broken
    # sentences, which is worse for stance search than a little extra boilerplate.
    result = trafilatura.bare_extraction(
        html,
        url=final_url,
        prune_xpath=INVISIBLE_XPATH,
        include_comments=False,
        include_tables=True,
        with_metadata=True,
    )
    t_title = getattr(result, "title", None)
    t_text = getattr(result, "text", None)
    t_author = getattr(result, "author", None)
    t_date = getattr(result, "date", None)

    headline = page.first_article("headline")
    title = normalize_title(
        (headline if isinstance(headline, str) else None)
        or page.meta.get("og:title")
        or t_title
        or page.title_tag
    )
    text = join_paragraphs(body_paragraphs(t_text, title))
    summary = join_paragraphs(html_fragment_to_paragraphs(ctx.feed_summary or ""))
    kind, text = classify(text, summary, page, final_url)

    authors = jsonld_authors(page.first_article("author"))
    if not authors and t_author:
        authors = [a.strip() for a in t_author.split(";") if a.strip()]

    published_at, published_source = choose_published(page, ctx, t_date)
    language, language_conf = detect_language(f"{title or ''}\n{text}")

    return Document(
        doc_id=ids.doc_id(canonical),
        revision=1,  # provisional; the storage writer assigns the real revision
        pipeline=Pipeline.RSS,
        source_id=ctx.source_id,
        canonical_url=canonical,
        url_canonicalizer_version=ids.CANONICALIZER_VERSION,
        final_url=final_url,
        title=title,
        authors=authors,
        extracted_text=text,
        content_kind=kind,
        language=language,
        language_confidence=language_conf,
        word_count=word_count(text),
        published_at=published_at,
        published_at_source=published_source,
        modified_at=choose_modified(page, ctx),
        discovered_at=ctx.discovered_at,
        fetched_at=ctx.fetched_at,
        # Provisional (the writer sets the flush time); fetched_at keeps this deterministic.
        ingested_at=ctx.fetched_at,
        feed_id=ctx.feed_id,
        feed_title=ctx.feed_title,
        feed_summary=ctx.feed_summary,
        content_hash=ids.content_hash(title, text),
        raw_blob_hash=ids.sha256_hex(body),
        extractor_version=extractor_version(),
        pipeline_version=ctx.pipeline_version,
    )
