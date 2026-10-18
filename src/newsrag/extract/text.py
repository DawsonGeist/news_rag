"""Text normalization to the Document text contract (contracts/v1/README.md, "Text and
locators"): NFC, paragraphs joined by exactly "\\n\\n", no other newlines, no "\\r",
no leading or trailing whitespace.

Every downstream locator indexes the string this produces, so any change to it must
bump ``EXTRACTOR_VERSION``.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

import lxml.html

# Characters some sites inject that carry no text: zero-width space/joiners, word
# joiner, BOM, soft hyphen.
_INVISIBLE = dict.fromkeys(map(ord, "​‌‍⁠﻿­"), None)
# Non-breaking and fixed-width spaces become plain spaces.
_SPACES = dict.fromkeys(map(ord, "   "), " ")
_WS = re.compile(r"\s+")


def normalize_paragraph(text: str) -> str:
    text = unicodedata.normalize("NFC", text).translate(_INVISIBLE).translate(_SPACES)
    # Drop control characters (whitespace controls are collapsed below).
    text = "".join(ch for ch in text if ch.isspace() or unicodedata.category(ch) != "Cc")
    return _WS.sub(" ", text).strip()


def join_paragraphs(paragraphs: Iterable[str]) -> str:
    """Normalize each paragraph, drop empty ones, join with a blank line."""
    out = [p for p in (normalize_paragraph(x) for x in paragraphs) if p]
    # NFC of a concatenation can differ from concatenated NFC pieces only at the
    # boundaries, and "\n\n" is a starter, so the result stays NFC.
    return "\n\n".join(out)


def normalize_title(title: str | None) -> str | None:
    if title is None:
        return None
    return normalize_paragraph(title) or None


def html_fragment_to_paragraphs(fragment: str) -> list[str]:
    """Plain-text paragraphs of an HTML fragment (feed summaries are often HTML)."""
    if "<" not in fragment:
        return fragment.splitlines()
    try:
        root = lxml.html.fragment_fromstring(fragment, create_parent="div")
    except (ValueError, lxml.etree.ParserError):
        return fragment.splitlines()
    for br in root.iter("br"):
        br.tail = "\n" + (br.tail or "")
    blocks = root.xpath(".//p | .//li | .//h1 | .//h2 | .//h3 | .//blockquote")
    if blocks:
        return [b.text_content() for b in blocks]
    return str(root.text_content()).splitlines()


def word_count(text: str) -> int:
    return len(text.split())
