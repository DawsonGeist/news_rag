"""Named-entity recognition (component 7, step 1): GLiNER on passages and titles.

GLiNER is zero-shot: it scores spans against label names. On news it finds names well,
but it also tags generic phrases ("conference call", "the bill", "We"). The rules here
keep names only, measured on a sample of the archive (prototypes/entities):

- a per-label score threshold, higher for the noisy labels (event, law, product, work);
- the surface must look like a name: an uppercase letter, and not a pronoun;
- leading honorifics ("Gov.", "Sen.", "President") are cut off the span, so the mention
  is the name itself.

Passages are the NER unit: at most ~200 words (a long paragraph is split by sentence),
so they fit GLiNER's 384-token window, and their offsets map back to the body.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from newsrag.contracts.v1 import EntityType

# GLiNER label prompt -> EntityType.
LABELS: dict[str, EntityType] = {
    "person": EntityType.PERSON,
    "organization": EntityType.ORGANIZATION,
    "location": EntityType.LOCATION,
    "event": EntityType.EVENT,
    "law": EntityType.LAW,
    "product": EntityType.PRODUCT,
    "work of art": EntityType.WORK,
}
THRESHOLDS: dict[str, float] = {
    "person": 0.5,
    "organization": 0.5,
    "location": 0.5,
    "event": 0.7,
    "law": 0.7,
    "product": 0.7,
    "work of art": 0.7,
}
MIN_THRESHOLD = min(THRESHOLDS.values())

NOT_NAMES = frozenset(
    [
        "i",
        "we",
        "you",
        "he",
        "she",
        "it",
        "they",
        "me",
        "us",
        "him",
        "her",
        "them",
        "my",
        "our",
        "your",
        "his",
        "its",
        "their",
        "mine",
        "ours",
        "yours",
        "hers",
        "theirs",
        "this",
        "that",
        "these",
        "those",
        "who",
        "whom",
    ]
)
# Honorifics and titles that GLiNER often includes in a person's span.
_TITLE = re.compile(
    r"^(?:(?:the\s+)?(?:gov|sen|rep|reps|del|lt|gen|col|maj|capt|sgt|cmdr|adm|dr|mr|mrs|ms|mx|"
    r"prof|rev|fr|hon|st|atty|supt|insp|det|ofc|cpl|pvt)\.?|"
    r"(?:the\s+)?(?:president|vice president|governor|senator|representative|congressman|"
    r"congresswoman|mayor|judge|justice|chief justice|secretary|attorney general|"
    r"sheriff|commissioner|councilman|councilwoman|councilmember|chairman|chairwoman|"
    r"chair|ceo|coach|pope|king|queen|prince|princess|bishop|cardinal|rabbi|pastor|"
    r"officer|deputy|detective|sergeant|lieutenant|captain|general|colonel|admiral|"
    r"former|late|u\.s\.|state))\s+",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Span:
    """An entity span in one NER unit's text."""

    start: int
    end: int
    label: str
    score: float


class NerModel(Protocol):
    model_id: str

    def predict(self, texts: Sequence[str]) -> list[list[Span]]:
        """Raw spans per text, at ``MIN_THRESHOLD`` or above."""
        ...


def strip_title(text: str, start: int, end: int) -> tuple[int, int]:
    """Cut leading honorifics ("Gov. ", "Former President ") off a person span."""
    while True:
        m = _TITLE.match(text[start:end])
        if not m or start + m.end() >= end:
            return start, end
        start += m.end()


def clean(text: str, spans: Sequence[Span]) -> list[Span]:
    """Apply the name rules to one unit's raw spans."""
    out: list[Span] = []
    for s in spans:
        if s.label not in LABELS or s.score < THRESHOLDS[s.label]:
            continue
        start, end = s.start, s.end
        if s.label == "person":
            start, end = strip_title(text, start, end)
        # Trim a possessive and stray punctuation at the edges.
        surface = text[start:end]
        trimmed = surface.rstrip(" ,.;:)]\"'’").removesuffix("’s").removesuffix("'s")
        end = start + len(trimmed)
        lead = len(trimmed) - len(trimmed.lstrip(" ,;:([\"'‘“"))
        start += lead
        surface = text[start:end]
        if not surface or not any(c.isupper() for c in surface):
            continue
        if surface.lower() in NOT_NAMES:
            continue
        out.append(Span(start, end, s.label, s.score))
    return out


class GlinerNer:
    """GLiNER large v2.1 on the GPU in half precision (~240 passages/s on the 5060 Ti)."""

    MODEL_NAME = "urchade/gliner_large-v2.1"

    def __init__(self, device: str = "cuda", batch_size: int = 16) -> None:
        from gliner import GLiNER

        self.model = GLiNER.from_pretrained(self.MODEL_NAME, local_files_only=True)
        self.model = self.model.to(device)
        if device == "cuda":
            self.model = self.model.half()
        self.model.eval()
        self.batch_size = batch_size
        self.model_id = f"{self.MODEL_NAME}@{_snapshot_revision(self.MODEL_NAME)}"

    def predict(self, texts: Sequence[str]) -> list[list[Span]]:
        if not texts:
            return []
        raw = self.model.inference(
            list(texts), list(LABELS), threshold=MIN_THRESHOLD, batch_size=self.batch_size
        )
        return [
            [Span(int(e["start"]), int(e["end"]), str(e["label"]), float(e["score"])) for e in es]
            for es in raw
        ]


def _snapshot_revision(repo: str) -> str:
    """The cached snapshot's commit, so ``model_id`` names exact weights."""
    from huggingface_hub import scan_cache_dir

    for r in scan_cache_dir().repos:
        if r.repo_id == repo:
            revs = sorted(r.revisions, key=lambda x: x.last_modified)
            if revs:
                return revs[-1].commit_hash[:12]
    return "unknown"
