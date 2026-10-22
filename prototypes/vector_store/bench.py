"""Prototype P2.1: benchmark vector stores on the real archive's passages.

Same workload for every engine (adapters below):
  1. bulk-load all passages but the last APPEND_ROWS, then build the vector index
  2. append the rest in micro-batches (the enricher's write pattern), timing each
  3. search 200 real headline queries: unfiltered, by date range, and gated to doc
     sets of 100 / 1,000 / 10,000 documents (stand-in for "documents mentioning X")
  4. recall@50 of every search against exact search computed on the GPU
  5. disk size

Crash behaviour is tested separately (crash.py). Usage:
  python prototypes/vector_store/bench.py --engine lancedb [--model BAAI/bge-m3]
Results: $NEWSRAG_DATA_DIR/prototype/vector_store/results-<engine>.json
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch

from newsrag.config import load_settings
from newsrag.storage.state import connect
from newsrag.storage.tables import DOCUMENTS, committed_files

K = 50
N_QUERIES = 200
APPEND_ROWS = 40_000
APPEND_BATCH = 2_000
GATE_SIZES = (100, 1_000, 10_000)


class Engine(Protocol):
    name: str

    def load(self, rows: pa.Table) -> None: ...
    def build_index(self) -> None: ...
    def append(self, rows: pa.Table) -> None: ...
    def maintain(self) -> None: ...  # periodic optimize/compaction, if the engine needs it
    def search(
        self, q: np.ndarray, k: int, docs: Sequence[str] | None, date: tuple[int, int] | None
    ) -> list[int]: ...
    def disk_bytes(self) -> int: ...


# --- engines -----------------------------------------------------------------------------


class LanceEngine:
    name = "lancedb"

    def __init__(self, root: Path) -> None:
        import lancedb

        shutil.rmtree(root, ignore_errors=True)
        self.root = root
        self.db = lancedb.connect(root)
        self.tbl: Any = None

    def load(self, rows: pa.Table) -> None:
        self.tbl = self.db.create_table("passages", data=rows)

    def build_index(self) -> None:
        from lancedb.index import BTree, HnswSq

        self.tbl.create_index("vector", config=HnswSq(distance_type="cosine"))
        self.tbl.create_index("doc_id", config=BTree())
        self.tbl.create_index("published", config=BTree())

    def append(self, rows: pa.Table) -> None:
        self.tbl.add(rows)

    def maintain(self) -> None:
        self.tbl.optimize()  # folds appended rows into the indexes

    def search(self, q, k, docs, date):  # type: ignore[no-untyped-def]
        s = self.tbl.search(q).limit(k).select(["row"])
        clauses = []
        if docs is not None:
            clauses.append("doc_id IN (" + ",".join(f"'{d}'" for d in docs) + ")")
        if date is not None:
            clauses.append(f"published >= {date[0]} AND published < {date[1]}")
        if clauses:
            s = s.where(" AND ".join(clauses), prefilter=True)
        return s.to_arrow()["row"].to_pylist()

    def disk_bytes(self) -> int:
        return sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())


ENGINES = {"lancedb": LanceEngine}


# --- workload ------------------------------------------------------------------------------


def load_corpus(out: Path, model: str) -> tuple[pa.Table, np.ndarray]:
    meta = pq.read_table(out / "passages.parquet", columns=["doc_id", "published_at"])
    vectors = np.load(out / f"vectors-{model.replace('/', '__')}.npy", mmap_mode="r")
    published = meta["published_at"].cast(pa.int64()).to_numpy(zero_copy_only=False)
    # Microseconds -> seconds; undated passages get 0 so date filters exclude them.
    published = np.nan_to_num(published.astype("float64") / 1e6).astype("int64")
    dim = vectors.shape[1]
    table = pa.table(
        {
            "row": pa.array(np.arange(len(meta), dtype=np.int64)),
            "doc_id": meta["doc_id"],
            "published": pa.array(published),
            "vector": pa.FixedSizeListArray.from_arrays(
                pa.array(np.asarray(vectors, dtype=np.float32).ravel()), dim
            ),
        }
    )
    return table, np.asarray(vectors)


def query_vectors(model: str, n: int) -> np.ndarray:
    """Real headlines from the archive as queries (not themselves passages)."""
    from sentence_transformers import SentenceTransformer

    s = load_settings()
    files = [str(p) for p in committed_files(connect(s.state_db), s.data_dir, DOCUMENTS)]
    titles = [
        r[0]
        for r in duckdb.sql(
            f"SELECT title FROM read_parquet({files}) WHERE title IS NOT NULL "
            f"AND language = 'en' USING SAMPLE {n} ROWS (reservoir, 11)"
        ).fetchall()
    ]
    m = SentenceTransformer(model, device="cuda", model_kwargs={"torch_dtype": torch.float16})
    return m.encode(titles, normalize_embeddings=True, convert_to_numpy=True).astype(np.float32)


def exact(vectors_gpu: torch.Tensor, q: np.ndarray, k: int, mask: torch.Tensor | None) -> list[int]:
    scores = vectors_gpu @ torch.from_numpy(q).to(vectors_gpu.device, vectors_gpu.dtype)
    if mask is not None:
        scores = scores.masked_fill(~mask, float("-inf"))
    top = torch.topk(scores.float(), k)
    return [int(i) for i, v in zip(top.indices.tolist(), top.values.tolist(), strict=True) if v > float("-inf")]


def pct(values: list[float], p: float) -> float:
    return statistics.quantiles(values, n=100)[int(p) - 1] if len(values) > 1 else values[0]


def run(engine_name: str, model: str) -> dict[str, Any]:
    s = load_settings()
    out = s.data_dir / "prototype" / "vector_store"
    table, vectors = load_corpus(out, model)
    n = table.num_rows
    print(f"corpus: {n} passages, dim {vectors.shape[1]}", flush=True)
    queries = query_vectors(model, N_QUERIES)
    vg = torch.from_numpy(np.asarray(vectors)).to("cuda", torch.float16)

    engine: Engine = ENGINES[engine_name](out / f"store-{engine_name}")
    res: dict[str, Any] = {"engine": engine_name, "model": model, "passages": n}

    base = table.slice(0, n - APPEND_ROWS)
    t0 = time.perf_counter()
    engine.load(base)
    res["bulk_load_s"] = round(time.perf_counter() - t0, 1)
    t0 = time.perf_counter()
    engine.build_index()
    res["index_build_s"] = round(time.perf_counter() - t0, 1)
    print(f"load {res['bulk_load_s']} s, index {res['index_build_s']} s", flush=True)

    append_s = []
    for start in range(n - APPEND_ROWS, n, APPEND_BATCH):
        t0 = time.perf_counter()
        engine.append(table.slice(start, APPEND_BATCH))
        append_s.append(time.perf_counter() - t0)
    res["append_batch_rows"] = APPEND_BATCH
    res["append_p50_s"], res["append_p95_s"] = round(pct(append_s, 50), 3), round(pct(append_s, 95), 3)

    doc_ids = table["doc_id"].to_pylist()
    published = table["published"].to_numpy()
    rng = np.random.default_rng(5)
    unique_docs = np.array(sorted(set(doc_ids)))
    hi = int(published.max())
    date = (hi - 7 * 86400, hi + 1)
    doc_index = {d: i for i, d in enumerate(unique_docs)}
    row_doc = torch.tensor([doc_index[d] for d in doc_ids], device="cuda")
    date_mask = torch.from_numpy((published >= date[0]) & (published < date[1])).to("cuda")

    def measure(label: str, docs: Sequence[str] | None, use_date: bool) -> None:
        mask = None
        if docs is not None:
            chosen = torch.zeros(len(unique_docs), dtype=torch.bool, device="cuda")
            chosen[[doc_index[d] for d in docs]] = True
            mask = chosen[row_doc]
        if use_date:
            mask = date_mask if mask is None else mask & date_mask
        lat, recall = [], []
        for q in queries:
            truth = exact(vg, q, K, mask)
            t0 = time.perf_counter()
            got = engine.search(q, K, docs, date if use_date else None)
            lat.append((time.perf_counter() - t0) * 1000)
            if truth:
                recall.append(len(set(got) & set(truth)) / len(truth))
        res[label] = {
            "p50_ms": round(pct(lat, 50), 1),
            "p95_ms": round(pct(lat, 95), 1),
            "recall_at_50": round(statistics.mean(recall), 3) if recall else None,
        }
        print(f"{label:28} {res[label]}", flush=True)

    measure("unfiltered_with_backlog", None, False)  # appended rows not yet folded in
    t0 = time.perf_counter()
    engine.maintain()
    res["maintain_s"] = round(time.perf_counter() - t0, 1)
    measure("unfiltered", None, False)
    measure("date_7d", None, True)
    for size in GATE_SIZES:
        gate = list(rng.choice(unique_docs, size=size, replace=False))
        measure(f"gate_{size}_docs", gate, False)
    gate = list(rng.choice(unique_docs, size=1_000, replace=False))
    measure("gate_1000_docs_and_date", gate, True)
    res["disk_gb"] = round(engine.disk_bytes() / 1e9, 2)
    print(f"disk {res['disk_gb']} GB, maintain {res['maintain_s']} s", flush=True)
    (out / f"results-{engine_name}.json").write_text(json.dumps(res, indent=2))
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=sorted(ENGINES), required=True)
    ap.add_argument("--model", default="BAAI/bge-m3")
    args = ap.parse_args()
    run(args.engine, args.model)
