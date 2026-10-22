from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from newsrag import ids
from newsrag.contracts.v1 import ContentKind, Document
from newsrag.extract.dates import parse_datetime, plausible, url_path_date
from newsrag.extract.extractor import (
    ExtractContext,
    ExtractFailure,
    body_paragraphs,
    decode_html,
    detect_language,
    extract,
)
from newsrag.extract.page import jsonld_authors, jsonld_is_free, parse_page
from newsrag.extract.text import html_fragment_to_paragraphs, join_paragraphs

FIXTURES = Path(__file__).parent / "fixtures" / "synthetic" / "articles"
SPEC = yaml.safe_load((FIXTURES / "expected.yaml").read_text(encoding="utf-8"))
T = datetime(2026, 10, 17, 14, 0, tzinfo=UTC)


def utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def context(name: str, overrides: dict[str, Any]) -> ExtractContext:
    o = dict(overrides)
    for k in ("feed_published_at",):
        if k in o:
            o[k] = utc(o[k])
    fetched = utc(SPEC["defaults"]["fetched_at"])
    return ExtractContext(
        final_url=o.pop("final_url", f"https://news.example/{name}"),
        source_id="news.example",
        discovered_at=fetched,
        fetched_at=fetched,
        pipeline_version="test",
        **o,
    )


def run_case(name: str) -> Document:
    case = SPEC["cases"][name]
    doc = extract((FIXTURES / f"{name}.html").read_bytes(), context(name, case.get("context", {})))
    assert isinstance(doc, Document), doc
    return doc


@pytest.mark.parametrize("name", sorted(SPEC["cases"]))
def test_synthetic_fixture(name: str) -> None:
    doc = run_case(name)
    exp = SPEC["cases"][name]["expect"]
    paras = doc.extracted_text.split("\n\n")
    for key, want in exp.items():
        if key == "first":
            assert paras[0].startswith(want), paras[0]
        elif key == "last":
            assert paras[-1].endswith(want), paras[-1]
        elif key == "paragraphs":
            assert len(paras) == want
        elif key == "contains":
            for snippet in want:
                assert snippet in doc.extracted_text, snippet
        elif key in ("published_at", "modified_at"):
            assert getattr(doc, key) == utc(want), key
        else:
            got = getattr(doc, key)
            assert (got.value if hasattr(got, "value") else got) == want, key


def test_every_fixture_has_expectations() -> None:
    assert {p.stem for p in FIXTURES.glob("*.html")} == set(SPEC["cases"])


@pytest.mark.parametrize("name", sorted(SPEC["cases"]))
def test_extraction_is_deterministic(name: str) -> None:
    """G1.6 depends on this: the same bytes and version give the same content_hash."""
    a, b = run_case(name), run_case(name)
    assert a == b


def test_document_identity_fields() -> None:
    doc = run_case("full_jsonld")
    body = (FIXTURES / "full_jsonld.html").read_bytes()
    assert doc.doc_id == ids.doc_id("https://millbrookledger.example/news/solar-farm-approved")
    assert doc.raw_blob_hash == ids.sha256_hex(body)
    assert doc.content_hash == ids.content_hash(doc.title, doc.extracted_text)
    assert doc.final_url == "https://millbrookledger.example/news/solar-farm-approved"
    assert doc.extractor_version.startswith("extract-3+trafilatura-")
    assert doc.ingested_at == doc.fetched_at  # provisional until the writer flushes


def test_language_unknown_when_unsure() -> None:
    assert detect_language("Latest videos") == (None, None)  # py3langid guesses 'nl'
    assert detect_language("") == (None, None)
    lang, conf = detect_language("The council voted on Thursday to approve the new budget.")
    assert lang == "en" and conf is not None and conf >= 0.5


def test_paywall_markers_only_count_in_attributes() -> None:
    html = (
        b'<html><head><script type="application/ld+json">{"@type": "NewsArticle"}</script>'
        b'<script id="x">var paywall = "piano-wrap";</script></head>'
        b"<body><article><p>A short free story about the town fair and its winners.</p>"
        b"</article></body></html>"
    )
    doc = extract(html, context("free", {}))
    assert isinstance(doc, Document) and doc.content_kind is not ContentKind.SUSPECTED_PAYWALL
    walled = html.replace(b"<article>", b'<article class="article-locked">')
    doc = extract(walled, context("walled", {}))
    assert isinstance(doc, Document) and doc.content_kind is ContentKind.SUSPECTED_PAYWALL


def test_index_pages_are_non_articles() -> None:
    headlines = "".join(
        f"<p>Headline number {i} about something that happened.</p>" for i in range(40)
    )
    head = "<head><meta property='og:type' content='website'></head>"
    home = f"<html>{head}<body>{headlines}</body></html>"
    doc = extract(home.encode(), context("section", {}))
    assert isinstance(doc, Document) and doc.content_kind is ContentKind.NON_ARTICLE
    root = home.replace("website", "article").encode()
    doc = extract(root, context("x", {"final_url": "https://news.example/"}))
    assert isinstance(doc, Document) and doc.content_kind is ContentKind.NON_ARTICLE
    # Long text with og:type=article and no JSON-LD is still an article.
    doc = extract(root, context("x", {"final_url": "https://news.example/story"}))
    assert isinstance(doc, Document) and doc.content_kind is ContentKind.FULL_TEXT


def test_full_content_feed_is_full_text_not_summary_only() -> None:
    html = (
        b'<html><head><script type="application/ld+json">{"@type": "NewsArticle"}</script>'
        b"</head><body><article><p>A short teaser paragraph on the page.</p></article>"
        b"</body></html>"
    )
    long_summary = "<p>" + " ".join(f"word{i}" for i in range(200)) + "</p>"
    doc = extract(html, context("fullfeed", {"feed_summary": long_summary}))
    assert isinstance(doc, Document)
    assert doc.content_kind is ContentKind.FULL_TEXT and doc.word_count == 200
    teaser = "<p>" + " ".join(f"word{i}" for i in range(40)) + "</p>"
    doc = extract(html, context("teaser", {"feed_summary": teaser}))
    assert isinstance(doc, Document) and doc.content_kind is ContentKind.SUMMARY_ONLY


def test_invisible_markup_is_not_article_text() -> None:
    article = "".join(
        f"<p>Real paragraph {i} of the story about the council budget vote.</p>" for i in range(8)
    )
    hidden = (
        "<template><p>New code will be available in 1:00</p></template>"
        "<div role='dialog'><p>Let us try this again.</p></div>"
        "<dialog><p>Please reenter your email.</p></dialog>"
        "<div aria-modal='true'><p>Sign in to keep reading.</p></div>"
    )
    html = (
        '<html><head><script type="application/ld+json">{"@type": "NewsArticle"}</script></head>'
        f"<body><article>{article}{hidden}</article></body></html>"
    ).encode()
    doc = extract(html, context("modal", {}))
    assert isinstance(doc, Document)
    for leaked in ("New code", "try this again", "reenter", "Sign in"):
        assert leaked not in doc.extracted_text
    assert doc.extracted_text.split("\n\n")[-1].startswith("Real paragraph 7")


def test_ghost_members_gate_is_paywall() -> None:
    html = (
        b'<html><head><script type="application/ld+json">{"@type": "Article"}</script></head>'
        b"<body><article><p>The first paragraph of a members-only post is a preview.</p>"
        b"<p>A second preview paragraph.</p>"
        b'<aside class="gh-post-upgrade-cta"><p>This post is for subscribers only</p></aside>'
        b"</article></body></html>"
    )
    doc = extract(html, context("ghost", {}))
    assert isinstance(doc, Document) and doc.content_kind is ContentKind.SUSPECTED_PAYWALL


def test_unparseable_is_a_failure_not_an_exception() -> None:
    out = extract(b"", context("empty", {}))
    assert isinstance(out, ExtractFailure) and out.error_code == "unparseable"


def test_empty_article_without_summary_is_suspected_paywall() -> None:
    html = (
        b'<html><head><script type="application/ld+json">'
        b'{"@type": "NewsArticle", "headline": "Rendered by JavaScript"}</script></head>'
        b'<body><div id="root"></div></body></html>'
    )
    doc = extract(html, context("js", {}))
    assert isinstance(doc, Document)
    assert (doc.content_kind, doc.extracted_text) == (ContentKind.SUSPECTED_PAYWALL, "")


# --- pieces ---------------------------------------------------------------------------


def test_join_paragraphs_meets_text_contract() -> None:
    text = join_paragraphs(["  Café au lait​. ", "", "\tTwo\nlines\r\nhere ", "\x07bell"])
    assert text == "Café au lait.\n\nTwo lines here\n\nbell"


def test_body_paragraphs_drops_repeated_headline_and_list_markers() -> None:
    raw = "Big News\nFirst para.\n- item one\n\nLast."
    assert body_paragraphs(raw, "big news") == ["First para.", "item one", "Last."]
    fused = "Big NewsReporter talks with the mayor.\nMore."
    assert body_paragraphs(fused, "Big News") == ["Reporter talks with the mayor.", "More."]
    assert body_paragraphs(raw, "Other") == ["Big News", "First para.", "item one", "Last."]


def test_feed_summary_html_to_paragraphs() -> None:
    frag = "<p>One <b>bold</b> line.</p><p>Second<br>line.</p>"
    assert join_paragraphs(html_fragment_to_paragraphs(frag)) == "One bold line.\n\nSecond line."
    assert html_fragment_to_paragraphs("plain text") == ["plain text"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-10-16T18:30:00-04:00", "2026-10-16T22:30:00+00:00"),
        ("2026-10-16T18:30:00Z", "2026-10-16T18:30:00+00:00"),
        ("2026-10-16T18:30:00.123+0000", "2026-10-16T18:30:00.123000+00:00"),
        ("2026-10-16 18:30:00+02:00", "2026-10-16T16:30:00+00:00"),
        ("2026-10-16", "2026-10-16T12:00:00+00:00"),
        ("2026-10-16T18:30:00", None),  # naive: never guessed
        ("October 16, 2026", None),
        ("", None),
        (None, None),
        (20261016, None),
    ],
)
def test_parse_datetime(raw: object, expected: str | None) -> None:
    got = parse_datetime(raw)
    assert (got.isoformat() if got else None) == expected


def test_url_path_date_and_plausibility() -> None:
    assert url_path_date("https://e.com/2026/10/5/story") == utc("2026-10-05T12:00:00Z")
    assert url_path_date("https://e.com/2026/13/40/story") is None
    assert url_path_date("https://e.com/story-2026") is None
    assert plausible(utc("2026-10-19T00:00:00Z"), T) is None  # after the fetch
    assert plausible(utc("1970-01-01T00:00:00Z"), T) is None


def test_page_meta() -> None:
    page = parse_page(
        """<?xml version="1.0" encoding="utf-8"?>
        <html><head>
        <link rel="Canonical" href=" https://e.com/a ">
        <meta property="og:title" content="OG">
        <meta property="OG:TITLE" content="second ignored">
        <script type="application/ld+json">{ broken </script>
        <script type="application/ld+json">[{"@type": ["NewsArticle", "Thing"],
          "author": [{"name": "A"}, "B", {"name": ""}], "isAccessibleForFree": "true"}]</script>
        </head><body><time itemprop="datePublished" datetime="2026-10-16"></time></body></html>"""
    )
    assert page.canonical == "https://e.com/a"
    assert page.meta["og:title"] == "OG"
    assert page.meta["datepublished"] == "2026-10-16"
    assert jsonld_authors(page.first_article("author")) == ["A", "B"]
    assert jsonld_is_free(page) is True


def test_decode_prefers_valid_http_charset() -> None:
    body = "<meta charset='windows-1252'><p>Café</p>".encode("cp1252")
    assert "Café" in decode_html(body, "text/html; charset=windows-1252")
    # A wrong HTTP charset falls through to the page's own declaration.
    assert "Café" in decode_html(body, "text/html; charset=utf-8")
    assert "Café" in decode_html(body, None)
    # No declaration anywhere: strict UTF-8 first.
    assert decode_html("<p>Café</p>".encode(), None) == "<p>Café</p>"
