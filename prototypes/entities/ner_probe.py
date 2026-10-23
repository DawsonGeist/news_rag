"""Prototype P2.4: NER candidates on real archive paragraphs.

Runs GLiNER (large, medium) and spaCy en_core_web_lg over the paragraphs of a random
sample of enriched documents; reports throughput and writes every entity found to
$NEWSRAG_DATA_DIR/prototype/entities/ner-<model>.jsonl for side-by-side review.
Prototype code: not part of the package.
"""

from __future__ import annotations

import argparse
import json
import random
import time

import torch

from newsrag.config import load_settings
from newsrag.contracts.v1 import SegmentLevel
from newsrag.segment.segmenter import segment
from newsrag.storage.state import connect
from newsrag.storage.tables import read_revisions

LABELS = ["person", "organization", "location", "event", "law", "product", "work of art"]
SPACY_MAP = {
    "PERSON": "person", "ORG": "organization", "GPE": "location", "LOC": "location",
    "FAC": "location", "EVENT": "event", "LAW": "law", "PRODUCT": "product",
    "WORK_OF_ART": "work of art", "NORP": "group",
}


def sample_paragraphs(n_docs: int, seed: int) -> list[tuple[str, int, str]]:
    s = load_settings()
    conn = connect(s.state_db)
    keys = [(r[0], r[1]) for r in conn.execute(
        "SELECT doc_id, revision FROM enrichments WHERE state = 'done'").fetchall()]
    random.Random(seed).shuffle(keys)
    docs = read_revisions(conn, s.data_dir, keys[:n_docs])
    out = []
    for d in docs.values():
        for seg in segment(d, "probe"):
            if seg.level is SegmentLevel.PARAGRAPH:
                out.append((d.doc_id, seg.position, seg.text))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=200)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--threshold", type=float, default=0.5)
    args = ap.parse_args()
    s = load_settings()
    out_dir = s.data_dir / "prototype" / "entities"
    out_dir.mkdir(parents=True, exist_ok=True)
    paras = sample_paragraphs(args.docs, args.seed)
    texts = [p[2] for p in paras]
    words = sum(len(t.split()) for t in texts)
    print(f"{args.docs} docs -> {len(texts)} paragraphs, {words} words", flush=True)

    results: dict[str, list[list[dict]]] = {}
    from gliner import GLiNER
    for name in ("urchade/gliner_large-v2.1", "urchade/gliner_medium-v2.1"):
        m = GLiNER.from_pretrained(name, local_files_only=True).to("cuda").half()
        m.eval()
        m.inference(texts[:8], LABELS, threshold=args.threshold)  # warm up
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        ents = m.inference(texts, LABELS, threshold=args.threshold, batch_size=16)
        torch.cuda.synchronize()
        dt = time.perf_counter() - t0
        tag = name.split("/")[1]
        results[tag] = ents
        print(f"{tag}: {len(texts)/dt:.0f} paragraphs/s, {words/dt:.0f} words/s, "
              f"{sum(map(len, ents))} entities, peak VRAM "
              f"{torch.cuda.max_memory_allocated()/2**30:.1f} GiB", flush=True)
        del m
        torch.cuda.empty_cache()

    import spacy
    nlp = spacy.load("en_core_web_lg", disable=["lemmatizer"])
    t0 = time.perf_counter()
    sp = []
    for doc in nlp.pipe(texts, batch_size=64, n_process=4):
        sp.append([{"start": e.start_char, "end": e.end_char, "text": e.text,
                    "label": SPACY_MAP.get(e.label_, e.label_.lower()), "score": None}
                   for e in doc.ents if e.label_ in SPACY_MAP])
    dt = time.perf_counter() - t0
    results["spacy_lg"] = sp
    print(f"spacy_lg (4 CPU procs): {len(texts)/dt:.0f} paragraphs/s, "
          f"{sum(map(len, sp))} entities", flush=True)

    for tag, ents in results.items():
        with (out_dir / f"ner-{tag}.jsonl").open("w") as f:
            for (doc_id, pos, text), es in zip(paras, ents, strict=True):
                f.write(json.dumps({"doc_id": doc_id, "position": pos, "text": text,
                                    "entities": es}) + "\n")


if __name__ == "__main__":
    main()
