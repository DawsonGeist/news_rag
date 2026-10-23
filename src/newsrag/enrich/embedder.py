"""Passage and title embeddings (component 6).

The model is pinned in code, not config: a different model or revision produces
different vectors, which must be a new ``PIPELINE_VERSION`` and a new collection, never a
silent mix. It loads from the local Hugging Face cache only (local-first: no network at
runtime). Fetch it once with ``huggingface-cli download BAAI/bge-m3 --revision <rev>``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import numpy as np
import numpy.typing as npt

MODEL_NAME = "BAAI/bge-m3"
MODEL_REVISION = "5617a9f61b028005a4858fdac845db406aefb181"
MODEL_ID = f"{MODEL_NAME}@{MODEL_REVISION}"
DIMS = 1024
# Passages are at most ~200 words (~270 tokens); a long paragraph's sentence-packed
# passage can reach ~400 words. 512 tokens covers nearly all of them, and bge-m3's
# attention cost grows quadratically past that.
MAX_SEQ_LENGTH = 512

Vectors = npt.NDArray[np.float32]


class Embedder(Protocol):
    model_id: str
    dims: int

    def encode(self, texts: Sequence[str]) -> Vectors:
        """L2-normalized vectors, one row per text."""
        ...


class SentenceTransformerEmbedder:
    def __init__(self, device: str = "cuda", batch_size: int = 64) -> None:
        import torch
        from sentence_transformers import SentenceTransformer

        self.model_id = MODEL_ID
        self.dims = DIMS
        self.batch_size = batch_size
        self.model = SentenceTransformer(
            MODEL_NAME,
            revision=MODEL_REVISION,
            device=device,
            local_files_only=True,
            model_kwargs={"torch_dtype": torch.float16 if device == "cuda" else torch.float32},
        )
        self.model.max_seq_length = MAX_SEQ_LENGTH
        got = self.model.get_sentence_embedding_dimension()
        if got != DIMS:
            raise RuntimeError(f"{MODEL_ID} produces {got}-d vectors, expected {DIMS}")

    def encode(self, texts: Sequence[str]) -> Vectors:
        if not texts:
            return np.zeros((0, self.dims), dtype=np.float32)
        out = self.model.encode(
            list(texts),
            batch_size=self.batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return np.asarray(out, dtype=np.float32)
