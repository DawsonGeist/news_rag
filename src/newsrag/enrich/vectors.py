"""Passage vectors in Qdrant (decided 2026-10-23, docs/p2.1-vector-store.md).

Qdrant has its own durability model, outside the file-plus-manifest protocol, so the
enricher makes every write **idempotent** and the SQLite state the oracle:

- A point's ID is its ``segment_id`` (32 hex digits are exactly a UUID), so re-running a
  batch after a crash overwrites the same points instead of adding duplicates.
- Points are written (``wait=True``) *before* the transaction that marks the revision
  ``done``. A crash in between leaves points for a revision still ``pending``; the
  re-run rewrites them.
- ``enrichments.vectors`` records how many points each ``done`` revision has.
  ``reconcile`` compares that with the collection and re-queues anything lost (a power
  cut can drop Qdrant writes it had acknowledged but not yet flushed).
- Only the latest revision of a document is searchable: indexing revision r deletes
  the document's points with a lower revision.
"""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client import models as m

from newsrag.enrich.embedder import Vectors

# Payload fields with an index. Gated search filters on these (entity IDs join in P2.4).
KEYWORD_FIELDS = ("doc_id", "source_id", "level")
INTEGER_FIELDS = ("published", "revision")


def point_id(segment_id: str) -> str:
    return str(uuid.UUID(hex=segment_id))


def segment_id_of(pid: str | int) -> str:
    return uuid.UUID(str(pid)).hex


@dataclass(frozen=True)
class Point:
    segment_id: str
    payload: dict[str, Any]


class VectorIndex:
    def __init__(self, client: QdrantClient, collection: str, dims: int) -> None:
        self.c = client
        self.collection = collection
        self.dims = dims

    @classmethod
    def connect(cls, url: str, collection: str, dims: int, timeout_s: float = 60) -> VectorIndex:
        return cls(QdrantClient(url=url, timeout=int(timeout_s)), collection, dims)

    def ensure_collection(self) -> None:
        """Create the collection if missing; refuse one built for other vectors."""
        if self.c.collection_exists(self.collection):
            params = self.c.get_collection(self.collection).config.params.vectors
            size = params.size if isinstance(params, m.VectorParams) else None
            if size != self.dims:
                raise RuntimeError(
                    f"collection {self.collection} holds {size}-d vectors, expected {self.dims}"
                )
            return
        self.c.create_collection(
            self.collection,
            # float16 originals on disk halve the long-term cost (p2.1 finding 4); int8
            # copies in RAM serve the search, rescored against the originals.
            vectors_config=m.VectorParams(
                size=self.dims,
                distance=m.Distance.COSINE,
                on_disk=True,
                datatype=m.Datatype.FLOAT16,
            ),
            quantization_config=m.ScalarQuantization(
                scalar=m.ScalarQuantizationConfig(type=m.ScalarType.INT8, always_ram=True)
            ),
        )
        for name in KEYWORD_FIELDS:
            self.c.create_payload_index(self.collection, name, m.PayloadSchemaType.KEYWORD)
        for name in INTEGER_FIELDS:
            self.c.create_payload_index(self.collection, name, m.PayloadSchemaType.INTEGER)

    def upsert(self, points: Sequence[Point], vectors: Vectors, chunk: int = 512) -> None:
        """Write points and wait until Qdrant has applied them."""
        if len(points) != len(vectors):
            raise ValueError("points and vectors differ in length")
        for i in range(0, len(points), chunk):
            part = points[i : i + chunk]
            self.c.upsert(
                self.collection,
                m.Batch(
                    ids=[point_id(p.segment_id) for p in part],
                    vectors=vectors[i : i + chunk].tolist(),
                    payloads=[p.payload for p in part],
                ),
                wait=True,
            )

    def delete_older_revisions(self, latest: dict[str, int]) -> None:
        """Remove each document's points from revisions below ``latest[doc_id]``."""
        for doc_id, revision in latest.items():
            if revision <= 1:
                continue
            self.c.delete(
                self.collection,
                m.FilterSelector(
                    filter=m.Filter(
                        must=[
                            m.FieldCondition(key="doc_id", match=m.MatchValue(value=doc_id)),
                            m.FieldCondition(key="revision", range=m.Range(lt=revision)),
                        ]
                    )
                ),
                wait=True,
            )

    def delete_docs(self, keys: Iterable[tuple[str, int]]) -> None:
        """Remove every point of these (doc_id, revision) pairs."""
        for doc_id, revision in keys:
            self.c.delete(
                self.collection,
                m.FilterSelector(
                    filter=m.Filter(
                        must=[
                            m.FieldCondition(key="doc_id", match=m.MatchValue(value=doc_id)),
                            m.FieldCondition(key="revision", match=m.MatchValue(value=revision)),
                        ]
                    )
                ),
                wait=True,
            )

    def count(self) -> int:
        return self.c.count(self.collection, exact=True).count

    def counts_by_revision(self, page: int = 10_000) -> Counter[tuple[str, int]]:
        """Points per (doc_id, revision), by scrolling the payloads (no vectors)."""
        counts: Counter[tuple[str, int]] = Counter()
        offset: Any = None
        while True:
            records, offset = self.c.scroll(
                self.collection,
                limit=page,
                offset=offset,
                with_payload=["doc_id", "revision"],
                with_vectors=False,
            )
            for r in records:
                p = r.payload or {}
                counts[(str(p.get("doc_id")), int(p.get("revision", 0)))] += 1
            if offset is None:
                return counts
