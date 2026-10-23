"""Within-document coreference (component 7, step 2): fastcoref (F-Coref).

Resolves "she", "the company" and "the pontiff" to a named mention, so a mention with
no name of its own inherits that mention's link. That's the overview's "He signed
the order Tuesday" problem, and it gives stance snippets their target.

fastcoref 2.1.6 predates transformers 5: one attribute the new loader expects is
missing, so it's added here (``_shim``). The pinned version is checked by a test.
Long articles are split into paragraph-aligned chunks that fit the model's window;
coreference across chunks is not resolved (rare in practice; named mentions repeat).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

Cluster = list[tuple[int, int]]  # character spans in the text

CHUNK_WORDS = 2000  # comfortably inside F-Coref's 4096-token Longformer window


class CorefModel(Protocol):
    model_id: str

    def clusters(self, texts: Sequence[str]) -> list[list[Cluster]]: ...


def chunks(text: str, max_words: int = CHUNK_WORDS) -> list[tuple[int, str]]:
    """Split at paragraph breaks into (offset, chunk) pieces of at most ``max_words``
    words (a single longer paragraph stays whole)."""
    out: list[tuple[int, str]] = []
    start = pos = words = 0
    for para in text.split("\n\n"):
        n = len(para.split())
        if words and words + n > max_words:
            out.append((start, text[start : pos - 2]))
            start, words = pos, 0
        words += n
        pos += len(para) + 2
    if start < len(text):
        out.append((start, text[start:]))
    return out


def _shim() -> None:
    from fastcoref.coref_models.modeling_fcoref import FCorefModel

    if not hasattr(FCorefModel, "all_tied_weights_keys"):
        FCorefModel.all_tied_weights_keys = {}


class FastCoref:
    MODEL_NAME = "biu-nlp/f-coref"

    def __init__(self, device: str = "cuda") -> None:
        import logging

        _shim()
        from fastcoref import FCoref

        # fastcoref logs every batch at INFO, and checks the Hub unless offline.
        logging.getLogger("fastcoref").setLevel(logging.WARNING)
        self.model = FCoref(model_name_or_path=self.MODEL_NAME, device=device)
        self.model_id = self.MODEL_NAME

    def clusters(self, texts: Sequence[str]) -> list[list[Cluster]]:
        pieces: list[tuple[int, int, str]] = []  # (text index, offset, chunk)
        for i, text in enumerate(texts):
            pieces.extend((i, off, chunk) for off, chunk in chunks(text) if chunk.strip())
        out: list[list[Cluster]] = [[] for _ in texts]
        if not pieces:
            return out
        preds = self.model.predict(texts=[p[2] for p in pieces], max_tokens_in_batch=10000)
        for (i, off, _), pred in zip(pieces, preds, strict=True):
            for cluster in pred.get_clusters(as_strings=False):
                out[i].append([(off + s, off + e) for s, e in cluster])
        return out
