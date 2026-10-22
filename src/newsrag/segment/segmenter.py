"""Component 5, segmenter: Document -> title, paragraph, sentence and passage segments.

A pure function, CPU-only, run inside the enricher. Every segment's locator indexes the
Document's text in code points, and the invariant ``segment.text ==
extracted_text[start:end]`` (or ``title[start:end]``) holds by construction and is
tested. Any change to the output for an existing input must bump ``CHUNKER_VERSION``.

- **Paragraphs** are exact: ``extracted_text`` separates them with "\\n\\n" (contract).
- **Sentences** come from pySBD, tuned for English abbreviations ("Sen.", "U.S.",
  "3 p.m."). Its strings are mapped back to offsets by searching forward in the
  paragraph, so locators never depend on the splitter's own offset bookkeeping.
- **Passages**, the retrieval unit (decisions.md, 2026-10-22), greedily pack whole
  consecutive paragraphs up to ``PASSAGE_MAX_WORDS``. A paragraph longer than
  ``LONG_PARAGRAPH_WORDS`` is split into sentence groups instead. Passages never
  overlap and never cut a sentence.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from functools import lru_cache

import pysbd

from newsrag import ids
from newsrag.contracts.v1 import Document, Segment, SegmentLevel, TextLocator

CHUNKER_VERSION = "seg-1"
PASSAGE_MAX_WORDS = 200
LONG_PARAGRAPH_WORDS = 400


@dataclass(frozen=True)
class _Span:
    start: int
    end: int
    words: int


@lru_cache(maxsize=1)
def _splitter() -> pysbd.Segmenter:
    return pysbd.Segmenter(language="en", clean=False, char_span=False)


def paragraph_spans(text: str) -> list[_Span]:
    spans: list[_Span] = []
    start = 0
    if not text:
        return spans
    for para in text.split("\n\n"):
        spans.append(_Span(start, start + len(para), len(para.split())))
        start += len(para) + 2
    return spans


def sentence_spans(text: str, para: _Span) -> list[_Span]:
    """Sentences of one paragraph as absolute spans, whitespace trimmed.

    pySBD only chooses *where to cut*: each sentence runs from the previous cut to the
    end of the splitter's piece, and the last one runs to the paragraph's end. So the
    sentences always cover the whole paragraph, even when the splitter drops or alters
    text (it silently dropped a trailing run of hashtags on a real page).
    """
    body = text[para.start : para.end]
    cuts, cursor = [], 0
    for raw in _splitter().segment(body):
        piece = raw.strip()
        if not piece:
            continue
        at = body.find(piece, cursor)
        if at < 0:
            break  # the splitter altered the text; the rest becomes one sentence
        cursor = at + len(piece)
        cuts.append(cursor)
    if not cuts or cuts[-1] < len(body):
        cuts.append(len(body))
    out, prev = [], 0
    for cut in cuts:
        chunk = body[prev:cut]
        lead = len(chunk) - len(chunk.lstrip())
        stripped = chunk.strip()
        if stripped:
            begin = para.start + prev + lead
            out.append(_Span(begin, begin + len(stripped), len(stripped.split())))
        prev = cut
    return out


def passage_spans(text: str, paras: list[_Span]) -> Iterator[_Span]:
    """Pack whole paragraphs up to PASSAGE_MAX_WORDS; split overlong paragraphs by sentence."""
    group: list[_Span] = []

    def flush() -> Iterator[_Span]:
        if group:
            yield _Span(group[0].start, group[-1].end, sum(p.words for p in group))
            group.clear()

    for para in paras:
        if para.words > LONG_PARAGRAPH_WORDS:
            yield from flush()
            yield from _pack(sentence_spans(text, para))
            continue
        if group and sum(p.words for p in group) + para.words > PASSAGE_MAX_WORDS:
            yield from flush()
        group.append(para)
    yield from flush()


def _pack(sentences: list[_Span]) -> Iterator[_Span]:
    group: list[_Span] = []
    for s in sentences:
        if group and sum(x.words for x in group) + s.words > PASSAGE_MAX_WORDS:
            yield _Span(group[0].start, group[-1].end, sum(x.words for x in group))
            group = []
        group.append(s)
    if group:
        yield _Span(group[0].start, group[-1].end, sum(x.words for x in group))


def segment_id(doc: Document, level: SegmentLevel, start: int, end: int) -> str:
    return ids.hash32(f"{doc.doc_id}:{doc.revision}:{level.value}:{start}:{end}:{CHUNKER_VERSION}")


def segment(doc: Document, pipeline_version: str) -> list[Segment]:
    """All segments of one Document revision: title, paragraphs, sentences, passages."""
    text = doc.extracted_text
    out: list[Segment] = []

    def make(
        level: SegmentLevel,
        span: _Span,
        position: int,
        parent: str | None = None,
        field: str = "body",
    ) -> Segment:
        source = doc.title if field == "title" else text
        assert source is not None
        return Segment(
            segment_id=segment_id(doc, level, span.start, span.end),
            doc_id=doc.doc_id,
            revision=doc.revision,
            level=level,
            parent_id=parent,
            position=position,
            text=source[span.start : span.end],
            locator=TextLocator(
                field="title" if field == "title" else "body", start=span.start, end=span.end
            ),
            chunker_version=CHUNKER_VERSION,
            pipeline_version=pipeline_version,
        )

    if doc.title:
        out.append(make(SegmentLevel.TITLE, _Span(0, len(doc.title), 0), 0, field="title"))
    paras = paragraph_spans(text)
    sentence_pos = 0
    for i, para in enumerate(paras):
        p = make(SegmentLevel.PARAGRAPH, para, i)
        out.append(p)
        for s in sentence_spans(text, para):
            out.append(make(SegmentLevel.SENTENCE, s, sentence_pos, parent=p.segment_id))
            sentence_pos += 1
    for j, passage in enumerate(passage_spans(text, paras)):
        out.append(make(SegmentLevel.PASSAGE, passage, j))
    return out
