"""Prototype P2.4: run the entity stage on a sample of enriched documents, without
committing anything, and write the results for review:

  $NEWSRAG_DATA_DIR/prototype/entities/link-sample.jsonl   one line per mention group
  stdout                                                   throughput and counts

Prototype code: not part of the package.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from collections import Counter

from newsrag.config import load_settings
from newsrag.enrich.embedder import DIMS, SentenceTransformerEmbedder
from newsrag.enrich.enricher import COLLECTION, PIPELINE_VERSION
from newsrag.enrich.vectors import VectorIndex
from newsrag.kb.index import KbIndex
from newsrag.link.coref import FastCoref
from newsrag.link.ner import GlinerNer
from newsrag.link.stage import EntityStage, VectorSource
from newsrag.segment.segmenter import segment
from newsrag.storage.state import connect
from newsrag.storage.tables import read_revisions
from newsrag.timeutil import utcnow


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=100)
    ap.add_argument("--seed", type=int, default=5)
    ap.add_argument("--snapshot", default="20261019")
    args = ap.parse_args()
    s = load_settings()
    conn = connect(s.state_db)
    keys = [(r[0], r[1]) for r in conn.execute(
        "SELECT doc_id, revision FROM enrichments WHERE state = 'done' AND pipeline_version = ?",
        (PIPELINE_VERSION,)).fetchall()]
    random.Random(args.seed).shuffle(keys)
    docs = list(read_revisions(conn, s.data_dir, keys[: args.docs]).values())
    segs = [segment(d, "probe") for d in docs]

    t0 = time.perf_counter()
    embedder = SentenceTransformerEmbedder()
    index = VectorIndex.connect(s.enricher.qdrant_url, COLLECTION, DIMS)
    stage = EntityStage(
        GlinerNer(), FastCoref(), KbIndex(s.data_dir / "kb" / args.snapshot), embedder,
        VectorSource(embedder, index.vectors), "probe",
    )
    print(f"models loaded in {time.perf_counter() - t0:.0f}s", flush=True)
    t0 = time.perf_counter()
    results = stage.run(docs, segs, utcnow())
    dt = time.perf_counter() - t0
    n = Counter()
    for r in results:
        n["mentions"] += len(r.mentions)
        n["coref"] += sum(m.ner_label == "coref" for m in r.mentions)
        n["links"] += len(r.links)
        n["snippets"] += len(r.snippets)
        n["snippet_bytes"] += sum(len(x.text.encode()) for x in r.snippets)
        n["locals"] += len(r.entities)
        n["entity_ids"] += len(r.entity_ids)
    print(f"{len(docs)} docs in {dt:.1f}s ({len(docs) / dt:.1f} docs/s): {dict(n)}", flush=True)

    out = s.data_dir / "prototype" / "entities" / "link-sample.jsonl"
    with out.open("w") as f:
        for d, r in zip(docs, results, strict=True):
            by_mention = {}
            for link in r.links:
                by_mention.setdefault(link.mention_id, []).append(link)
            for m in r.mentions:
                f.write(json.dumps({
                    "doc_id": d.doc_id, "source": d.source_id, "title": d.title,
                    "surface": m.surface, "label": m.ner_label,
                    "context": (d.extracted_text if m.locator.field == "body" else d.title or "")[
                        max(0, m.locator.start - 150): m.locator.end + 150],
                    "links": [(x.entity_id, x.confidence, x.method) for x in by_mention.get(m.mention_id, [])],
                }) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
