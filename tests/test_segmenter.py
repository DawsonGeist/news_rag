import random
from datetime import UTC, datetime
from pathlib import Path

import pytest
from tests.test_contracts import make_doc

from newsrag import ids
from newsrag.contracts.v1 import ContentKind, Document, Segment, SegmentLevel
from newsrag.extract.extractor import ExtractContext, extract
from newsrag.segment.segmenter import (
    CHUNKER_VERSION,
    LONG_PARAGRAPH_WORDS,
    PASSAGE_MAX_WORDS,
    segment,
)

ARTICLES = Path(__file__).parent / "fixtures" / "synthetic" / "articles"
T = datetime(2026, 10, 17, 14, 0, tzinfo=UTC)


def doc_with(text: str, title: str | None = "A story") -> Document:
    return make_doc(
        title=title,
        extracted_text=text,
        content_hash=ids.content_hash(title, text),
        content_kind=ContentKind.FULL_TEXT if text else ContentKind.NON_ARTICLE,
    )


def check_invariants(doc: Document, segs: list[Segment]) -> None:
    by_level: dict[SegmentLevel, list[Segment]] = {lvl: [] for lvl in SegmentLevel}
    for s in segs:
        by_level[s.level].append(s)
        src = doc.title if s.locator.field == "title" else doc.extracted_text
        assert src is not None
        assert s.text == src[s.locator.start : s.locator.end]  # the locator contract
        assert s.text == s.text.strip() and s.text
        assert s.segment_id == ids.hash32(
            f"{doc.doc_id}:{doc.revision}:{s.level.value}:{s.locator.start}:{s.locator.end}:{CHUNKER_VERSION}"
        )
    for lvl, items in by_level.items():
        assert [s.position for s in items] == list(range(len(items))), lvl
    paras = by_level[SegmentLevel.PARAGRAPH]
    assert "\n\n".join(p.text for p in paras) == doc.extracted_text
    para_ids = {p.segment_id for p in paras}
    assert all(s.parent_id in para_ids for s in by_level[SegmentLevel.SENTENCE])
    # Each paragraph's sentences cover it exactly: no text is ever dropped.
    for p in paras:
        kids = [x for x in by_level[SegmentLevel.SENTENCE] if x.parent_id == p.segment_id]
        assert kids and kids[0].locator.start == p.locator.start
        assert kids[-1].locator.end == p.locator.end
    # Passages tile the body in order, without overlap, never cutting a sentence.
    passages = by_level[SegmentLevel.PASSAGE]
    ends = [p.locator.end for p in passages]
    assert ends == sorted(ends)
    for a, b in zip(passages, passages[1:], strict=False):
        assert a.locator.end <= b.locator.start
    if doc.extracted_text:
        assert passages[0].locator.start == 0
        assert passages[-1].locator.end == len(doc.extracted_text)
    sentence_bounds = {(s.locator.start, s.locator.end) for s in by_level[SegmentLevel.SENTENCE]}
    starts = {a for a, _ in sentence_bounds}
    finishes = {b for _, b in sentence_bounds}
    for p in passages:
        assert p.locator.start in starts and p.locator.end in finishes


@pytest.mark.parametrize("name", sorted(p.stem for p in ARTICLES.glob("*.html")))
def test_synthetic_articles(name: str) -> None:
    ctx = ExtractContext(
        final_url=f"https://news.example/{name}",
        source_id="news.example",
        discovered_at=T,
        fetched_at=T,
        pipeline_version="t",
    )
    doc = extract((ARTICLES / f"{name}.html").read_bytes(), ctx)
    assert isinstance(doc, Document)
    check_invariants(doc, segment(doc, "t"))


def test_known_document() -> None:
    text = (
        "Sen. John Smith said the U.S. economy grew 2.5% in Q3. He spoke at 3 p.m.\n\n"
        "“We are not done,” he added.\n\n"
        "The vote was 5-2."
    )
    segs = segment(doc_with(text), "t")
    levels = [s.level for s in segs]
    assert levels.count(SegmentLevel.TITLE) == 1
    assert levels.count(SegmentLevel.PARAGRAPH) == 3
    assert [s.text for s in segs if s.level is SegmentLevel.SENTENCE] == [
        "Sen. John Smith said the U.S. economy grew 2.5% in Q3.",
        "He spoke at 3 p.m.",
        "“We are not done,” he added.",
        "The vote was 5-2.",
    ]
    (passage,) = [s for s in segs if s.level is SegmentLevel.PASSAGE]  # all fits in one
    assert passage.text == text


def para(words: int, tag: str) -> str:
    sentences = []
    while words > 0:
        n = min(words, 12)
        sentences.append(" ".join(f"{tag}{i}" for i in range(n)) + ".")
        words -= n
    return " ".join(sentences)


def test_passage_packing_and_long_paragraphs() -> None:
    paras = [
        para(60, "a"),
        para(60, "b"),
        para(60, "c"),
        para(60, "d"),
        para(900, "e"),
        para(30, "f"),
    ]
    doc = doc_with("\n\n".join(paras))
    segs = segment(doc, "t")
    check_invariants(doc, segs)
    passages = [s for s in segs if s.level is SegmentLevel.PASSAGE]
    words = [len(p.text.split()) for p in passages]
    assert words[0] == 180  # a+b+c: adding d would exceed 200
    assert all(w <= PASSAGE_MAX_WORDS for w in words)  # the 900-word paragraph was split
    assert LONG_PARAGRAPH_WORDS < 900 and sum(words) == 60 * 4 + 900 + 30


def test_unicode_offsets_are_code_points() -> None:
    text = "Café owner José 👩🏽‍🍳 cooked. Naïve critics loved it.\n\nNext paragraph — done."
    doc = doc_with(text)
    check_invariants(doc, segment(doc, "t"))


def test_text_the_splitter_drops_is_kept() -> None:
    # pySBD dropped this trailing run of hashtags on a real page.
    text = (
        '@freddierecords ‼️NOW AVAILABLE‼️ "Sin Ti" is the amazing new single. '
        "Check out this new feel-good summer song on @spotify and be sure to share. "
        "Go to the link below or in our bio to listen today. #tejano #norteño #newmusic #fyp"
    )
    doc = doc_with(text)
    segs = segment(doc, "t")
    check_invariants(doc, segs)
    sentences = [x.text for x in segs if x.level is SegmentLevel.SENTENCE]
    assert sentences[-1].endswith("#fyp")


def test_empty_body_has_only_a_title() -> None:
    segs = segment(doc_with("", title="Videos"), "t")
    assert [(s.level, s.text) for s in segs] == [(SegmentLevel.TITLE, "Videos")]
    assert segment(doc_with("", title=None), "t") == []


def test_random_documents_keep_invariants() -> None:
    rng = random.Random(7)
    vocab = [
        "Mr.",
        "the",
        "U.S.",
        "said",
        "council",
        "vote",
        "3.5%",
        "“quote”",
        "Inc.",
        "Dr.",
        "budget",
    ]
    for _ in range(50):
        paras = []
        for _ in range(rng.randint(1, 30)):
            sents = []
            for _ in range(rng.randint(1, 6)):
                sents.append(
                    " ".join(rng.choice(vocab) for _ in range(rng.randint(1, 40)))
                    + rng.choice([".", "?", "!"])
                )
            paras.append(" ".join(sents))
        doc = doc_with("\n\n".join(paras))
        check_invariants(doc, segment(doc, "t"))


def test_deterministic() -> None:
    doc = doc_with("One. Two.\n\nThree.")
    assert segment(doc, "t") == segment(doc, "t")
