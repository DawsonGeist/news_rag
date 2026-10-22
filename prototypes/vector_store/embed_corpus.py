"""Prototype P2.1: segment the archive into passages and embed them with one model.

Writes, under $NEWSRAG_DATA_DIR/prototype/vector_store/:
  passages.parquet          passage_id, doc_id, revision, source_id, published_at,
                            ingested_at, position, text (one row per passage)
  vectors-<model>.npy       float16 [n_passages, dim], L2-normalized, row-aligned

Prototype code: not part of the package, not a contract. Resumable is not needed; it
writes to tmp names and renames at the end.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from sentence_transformers import SentenceTransformer

from newsrag.config import load_settings
from newsrag.contracts.v1 import SegmentLevel
from newsrag.segment.segmenter import segment
from newsrag.storage.state import connect
from newsrag.storage.tables import DOCUMENTS, committed_files, table_to_documents


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="BAAI/bge-m3")
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--limit-docs", type=int, default=None)
    args = ap.parse_args()

    s = load_settings()
    out = s.data_dir / "prototype" / "vector_store"
    out.mkdir(parents=True, exist_ok=True)
    conn = connect(s.state_db)
    files = [str(p) for p in committed_files(conn, s.data_dir, DOCUMENTS)]

    passages_path = out / "passages.parquet"
    if not passages_path.exists():
        t0 = time.perf_counter()
        rows: dict[str, list] = {k: [] for k in (
            "passage_id", "doc_id", "revision", "source_id", "published_at", "ingested_at", "position", "text")}
        limit = f"LIMIT {args.limit_docs}" if args.limit_docs else ""
        sql = (
            f"SELECT * FROM read_parquet({files}) "
            "WHERE content_kind = 'full_text' AND language = 'en' "
            f"ORDER BY doc_id, revision {limit}"
        )
        reader = duckdb.sql(sql).arrow()
        n_docs = 0
        for batch in reader:
            for d in table_to_documents(pa.Table.from_batches([batch])):
                n_docs += 1
                for seg in segment(d, "prototype"):
                    if seg.level is not SegmentLevel.PASSAGE:
                        continue
                    rows["passage_id"].append(seg.segment_id)
                    rows["doc_id"].append(d.doc_id)
                    rows["revision"].append(d.revision)
                    rows["source_id"].append(d.source_id)
                    rows["published_at"].append(d.published_at)
                    rows["ingested_at"].append(d.ingested_at)
                    rows["position"].append(seg.position)
                    rows["text"].append(seg.text)
        tmp = passages_path.with_suffix(".tmp")
        pq.write_table(pa.table(rows), tmp, compression="zstd")
        tmp.rename(passages_path)
        print(f"segmented {n_docs} docs -> {len(rows['text'])} passages in {time.perf_counter()-t0:.0f}s", flush=True)

    texts = pq.read_table(passages_path, columns=["text"])["text"].to_pylist()
    model = SentenceTransformer(args.model, device="cuda", model_kwargs={"torch_dtype": torch.float16})
    model.max_seq_length = 512
    name = args.model.replace("/", "__")
    vec_path = out / f"vectors-{name}.npy"
    t0 = time.perf_counter()
    chunks = []
    step = 50_000
    for i in range(0, len(texts), step):
        chunks.append(model.encode(texts[i : i + step], batch_size=args.batch,
                                   normalize_embeddings=True, convert_to_numpy=True).astype(np.float16))
        done = min(i + step, len(texts))
        rate = done / (time.perf_counter() - t0)
        print(f"embedded {done}/{len(texts)} ({rate:.0f}/s, eta {(len(texts)-done)/rate/60:.0f} min)", flush=True)
    vectors = np.concatenate(chunks)
    tmp = vec_path.with_suffix(".tmp.npy")
    np.save(tmp, vectors)
    tmp.rename(vec_path)
    print(f"done: {vectors.shape} in {(time.perf_counter()-t0)/60:.0f} min -> {vec_path}", flush=True)


if __name__ == "__main__":
    main()
