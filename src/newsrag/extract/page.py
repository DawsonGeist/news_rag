"""Structured metadata from one lxml parse: rel=canonical, JSON-LD article objects,
meta tags and <title>."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import lxml.html

ARTICLE_TYPES = frozenset(
    {
        "Article",
        "NewsArticle",
        "AnalysisNewsArticle",
        "AskPublicNewsArticle",
        "BackgroundNewsArticle",
        "OpinionNewsArticle",
        "ReportageNewsArticle",
        "ReviewNewsArticle",
        "SatiricalArticle",
        "BlogPosting",
        "LiveBlogPosting",
        "Report",
        "TechArticle",
    }
)

_XML_DECL = re.compile(r"^\s*<\?xml[^>]*\?>", re.IGNORECASE)


class UnparseableHTML(ValueError):
    pass


@dataclass
class PageMeta:
    canonical: str | None = None
    title_tag: str | None = None
    meta: dict[str, str] = field(default_factory=dict)  # lowercased key -> first content
    # Every class and id value on the page, space-joined: where paywall markup shows.
    # Script bodies are excluded on purpose; vendor names there are not evidence.
    attr_tokens: str = ""
    articles: list[dict[str, Any]] = field(default_factory=list)  # JSON-LD, document order

    def first_article(self, key: str) -> Any:
        for a in self.articles:
            if a.get(key) not in (None, "", []):
                return a[key]
        return None


def _types(obj: dict[str, Any]) -> set[str]:
    t = obj.get("@type")
    if isinstance(t, str):
        return {t}
    if isinstance(t, list):
        return {x for x in t if isinstance(x, str)}
    return set()


def _walk(obj: Any) -> Iterator[dict[str, Any]]:
    """Every dict in a JSON-LD blob: top level, lists, @graph and nested values."""
    if isinstance(obj, list):
        for x in obj:
            yield from _walk(x)
    elif isinstance(obj, dict):
        yield obj
        for v in obj.values():
            if isinstance(v, dict | list):
                yield from _walk(v)


def parse_page(html: str) -> PageMeta:
    try:
        root = lxml.html.document_fromstring(_XML_DECL.sub("", html, count=1))
    except (ValueError, lxml.etree.ParserError) as e:
        raise UnparseableHTML(str(e)) from e
    meta = PageMeta()

    for link in root.xpath("//link[@href]"):
        rels = (link.get("rel") or "").lower().split()
        if "canonical" in rels:
            meta.canonical = link.get("href").strip()
            break

    meta.attr_tokens = " ".join(str(v) for v in root.xpath("//@class | //@id"))

    titles = root.xpath("//title")
    if titles:
        meta.title_tag = titles[0].text_content()

    for m in root.xpath("//meta[@content]"):
        key = m.get("property") or m.get("name") or m.get("itemprop")
        if key:
            meta.meta.setdefault(key.strip().lower(), m.get("content"))
    # Microdata dates on <time itemprop=datePublished datetime=...>.
    for t in root.xpath("//*[@itemprop and @datetime]"):
        meta.meta.setdefault(t.get("itemprop").strip().lower(), t.get("datetime"))

    for script in root.xpath('//script[@type="application/ld+json"]'):
        try:
            blob = json.loads(script.text_content(), strict=False)
        except ValueError:
            continue  # broken JSON-LD is common; the other sources still apply
        meta.articles.extend(o for o in _walk(blob) if _types(o) & ARTICLE_TYPES)
    return meta


def jsonld_authors(value: Any) -> list[str]:
    """Author names from a JSON-LD ``author`` value (string, Person, or a list)."""
    items = value if isinstance(value, list) else [value]
    names = []
    for it in items:
        name = it.get("name") if isinstance(it, dict) else it
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return names


def jsonld_is_free(meta: PageMeta) -> bool | None:
    """False if any article object (or one of its parts) says it is not free."""
    seen = None
    for a in meta.articles:
        objs = [a] + [p for p in _walk(a.get("hasPart")) if isinstance(p, dict)]
        for o in objs:
            v = o.get("isAccessibleForFree")
            if isinstance(v, str):
                v = {"true": True, "false": False}.get(v.strip().lower())
            if v is False:
                return False
            if v is True:
                seen = True
    return seen
