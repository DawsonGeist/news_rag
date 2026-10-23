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

    def __init__(self, root: Path, reuse: bool = False, params: dict[str, Any] | None = None) -> None:
        import lancedb

        if not reuse:
            shutil.rmtree(root, ignore_errors=True)
        self.root = root
        self.db = lancedb.connect(root)
        self.tbl: Any = self.db.open_table("passages") if reuse else None
        self.params = params or {}

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
        s = self.tbl.search(q).limit(k).select(["row", "_distance"])
        for knob in ("nprobes", "ef", "refine_factor"):
            if knob in self.params:
                s = getattr(s, knob)(self.params[knob])
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


def _docker_du(container: str, path: str) -> int:
    import subprocess

    out = subprocess.run(
        ["docker", "exec", container, "du", "-sb", path], capture_output=True, text=True, check=True
    ).stdout
    return int(out.split()[0])


class QdrantEngine:
    name = "qdrant"
    COLLECTION = "proto_passages"

    def __init__(self, root: Path, reuse: bool = False, params: dict[str, Any] | None = None) -> None:
        from qdrant_client import QdrantClient

        self.c = QdrantClient(url="http://127.0.0.1:6333", prefer_grpc=True, timeout=600)
        self.params = params or {}
        if not reuse and self.c.collection_exists(self.COLLECTION):
            self.c.delete_collection(self.COLLECTION)

    def _points(self, rows: pa.Table):  # type: ignore[no-untyped-def]
        vecs = np.asarray(rows["vector"].combine_chunks().flatten().to_numpy()).reshape(rows.num_rows, -1)
        payload = [
            {"doc_id": d, "published": int(t)}
            for d, t in zip(rows["doc_id"].to_pylist(), rows["published"].to_pylist(), strict=True)
        ]
        return rows["row"].to_pylist(), vecs, payload

    def load(self, rows: pa.Table) -> None:
        from qdrant_client import models as m

        dim = rows.schema.field("vector").type.list_size
        self.c.create_collection(
            self.COLLECTION,
            vectors_config=m.VectorParams(size=dim, distance=m.Distance.COSINE, on_disk=True),
            quantization_config=m.ScalarQuantization(
                scalar=m.ScalarQuantizationConfig(type=m.ScalarType.INT8, always_ram=True)
            ),
        )
        self.c.create_payload_index(self.COLLECTION, "doc_id", m.PayloadSchemaType.KEYWORD)
        self.c.create_payload_index(self.COLLECTION, "published", m.PayloadSchemaType.INTEGER)
        for start in range(0, rows.num_rows, 20_000):
            ids, vecs, payload = self._points(rows.slice(start, 20_000))
            self.c.upload_collection(self.COLLECTION, vecs, payload, ids=ids, batch_size=1024, parallel=4)

    def _wait_green(self) -> None:
        from qdrant_client import models as m

        while self.c.get_collection(self.COLLECTION).status != m.CollectionStatus.GREEN:
            time.sleep(1)

    def build_index(self) -> None:
        self._wait_green()  # Qdrant indexes in the background as segments fill

    def append(self, rows: pa.Table) -> None:
        from qdrant_client import models as m

        ids, vecs, payload = self._points(rows)
        self.c.upsert(self.COLLECTION, m.Batch(ids=ids, vectors=vecs.tolist(), payloads=payload), wait=True)

    def maintain(self) -> None:
        self._wait_green()

    def search(self, q, k, docs, date):  # type: ignore[no-untyped-def]
        from qdrant_client import models as m

        must = []
        if docs is not None:
            must.append(m.FieldCondition(key="doc_id", match=m.MatchAny(any=list(docs))))
        if date is not None:
            must.append(m.FieldCondition(key="published", range=m.Range(gte=date[0], lt=date[1])))
        sp = None
        if self.params:
            sp = m.SearchParams(
                hnsw_ef=self.params.get("hnsw_ef"),
                acorn=m.AcornSearchParams(enable=True) if self.params.get("acorn") else None,
                quantization=m.QuantizationSearchParams(
                    rescore=self.params.get("rescore", True),
                    oversampling=self.params.get("oversampling"),
                ),
            )
        r = self.c.query_points(
            self.COLLECTION,
            query=q.tolist(),
            limit=k,
            query_filter=m.Filter(must=must) if must else None,
            search_params=sp,
        )
        return [int(p.id) for p in r.points]

    def disk_bytes(self) -> int:
        return _docker_du("newsrag-qdrant-1", f"/qdrant/storage/collections/{self.COLLECTION}")


class Neo4jEngine:
    name = "neo4j"

    def __init__(self, root: Path, reuse: bool = False, params: dict[str, Any] | None = None) -> None:
        from neo4j import GraphDatabase

        repo = Path(__file__).resolve().parents[2]
        env = dict(
            line.split("=", 1) for line in (repo / "deploy" / ".env").read_text().split() if "=" in line
        )
        self.d = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j", env["NEO4J_PASSWORD"]))
        if not reuse:
            self._q("DROP INDEX proto_passage_vec IF EXISTS")
            self._q("MATCH (p:ProtoPassage) CALL (p) { DETACH DELETE p } IN TRANSACTIONS OF 20000 ROWS")

    def _q(self, cypher: str, **params: Any) -> list[Any]:
        if "IN TRANSACTIONS" in cypher:  # needs an implicit (auto-commit) transaction
            with self.d.session() as s:
                return list(s.run(cypher, params))
        return self.d.execute_query(cypher, params).records

    def _create(self, rows: pa.Table) -> None:
        vecs = np.asarray(rows["vector"].combine_chunks().flatten().to_numpy()).reshape(rows.num_rows, -1)
        batch = [
            {"row": r, "doc_id": d, "published": int(t), "vector": v.tolist()}
            for r, d, t, v in zip(
                rows["row"].to_pylist(), rows["doc_id"].to_pylist(), rows["published"].to_pylist(), vecs, strict=True
            )
        ]
        # Native VECTOR<FLOAT32> (Cypher 25): a plain list property is stored as 64-bit
        # doubles, which more than doubled the store in the first run.
        dim = len(batch[0]["vector"])
        self._q(
            "CYPHER 25 UNWIND $rows AS r CREATE (:ProtoPassage {row: r.row, doc_id: r.doc_id, "
            f"published: r.published, vector: vector(r.vector, {dim}, FLOAT32)}})",
            rows=batch,
        )

    def load(self, rows: pa.Table) -> None:
        for start in range(0, rows.num_rows, 5_000):
            self._create(rows.slice(start, 5_000))

    def build_index(self) -> None:
        dim = 1024
        self._q(
            "CREATE VECTOR INDEX proto_passage_vec IF NOT EXISTS FOR (p:ProtoPassage) ON p.vector "
            "WITH [p.doc_id, p.published] "
            f"OPTIONS {{indexConfig: {{`vector.dimensions`: {dim}, `vector.similarity_function`: 'cosine'}}}}"
        )
        self._q("CALL db.awaitIndexes(36000)")

    def append(self, rows: pa.Table) -> None:
        self._create(rows)

    def maintain(self) -> None:
        self._q("CALL db.awaitIndexes(3600)")

    def search(self, q, k, docs, date):  # type: ignore[no-untyped-def]
        where = []
        params: dict[str, Any] = {"v": q.tolist(), "k": k}
        if docs is not None:
            where.append("p.doc_id IN $docs")
            params["docs"] = list(docs)
        if date is not None:
            where.append("p.published >= $lo AND p.published < $hi")
            params["lo"], params["hi"] = date
        clause = f" WHERE {' AND '.join(where)}" if where else ""
        rec = self._q(
            "MATCH (p:ProtoPassage) SEARCH p IN (VECTOR INDEX proto_passage_vec FOR $v"
            f"{clause} LIMIT $k) SCORE AS s RETURN p.row AS row",
            **params,
        )
        return [r["row"] for r in rec]

    def disk_bytes(self) -> int:
        return _docker_du("newsrag-neo4j-1", "/data/databases/neo4j")


ENGINES = {"lancedb": LanceEngine, "qdrant": QdrantEngine, "neo4j": Neo4jEngine}


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


def run(
    engine_name: str,
    model: str,
    root: Path | None = None,
    random_queries: bool = False,
    reuse: bool = False,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    s = load_settings()
    out = root or s.data_dir / "prototype" / "vector_store"
    table, vectors = load_corpus(out, model)
    n = table.num_rows
    append_rows = min(APPEND_ROWS, n // 4)
    print(f"corpus: {n} passages, dim {vectors.shape[1]}", flush=True)
    if random_queries:  # smoke tests: no model load
        queries = np.random.default_rng(3).standard_normal((N_QUERIES, vectors.shape[1])).astype(np.float32)
        queries /= np.linalg.norm(queries, axis=1, keepdims=True)
    else:
        queries = query_vectors(model, N_QUERIES)
    vg = torch.from_numpy(np.asarray(vectors)).to("cuda", torch.float16)

    engine: Engine = ENGINES[engine_name](out / f"store-{engine_name}", reuse, params)
    res: dict[str, Any] = {"engine": engine_name, "model": model, "passages": n, "params": params or {}}
    if reuse:
        return _search_suite(engine, table, queries, vg, res, out, engine_name, tuned=True)

    base = table.slice(0, n - append_rows)
    t0 = time.perf_counter()
    engine.load(base)
    res["bulk_load_s"] = round(time.perf_counter() - t0, 1)
    t0 = time.perf_counter()
    engine.build_index()
    res["index_build_s"] = round(time.perf_counter() - t0, 1)
    print(f"load {res['bulk_load_s']} s, index {res['index_build_s']} s", flush=True)

    append_s = []
    for start in range(n - append_rows, n, APPEND_BATCH):
        t0 = time.perf_counter()
        engine.append(table.slice(start, APPEND_BATCH))
        append_s.append(time.perf_counter() - t0)
    res["append_batch_rows"] = APPEND_BATCH
    res["append_p50_s"], res["append_p95_s"] = round(pct(append_s, 50), 3), round(pct(append_s, 95), 3)

    return _search_suite(engine, table, queries, vg, res, out, engine_name, tuned=False)


def _search_suite(engine, table, queries, vg, res, out, engine_name, tuned):  # type: ignore[no-untyped-def]
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

    if not tuned:
        measure("unfiltered_with_backlog", None, False)  # appended rows not yet folded in
        t0 = time.perf_counter()
        engine.maintain()
        res["maintain_s"] = round(time.perf_counter() - t0, 1)
    measure("unfiltered", None, False)
    measure("date_7d", None, True)
    for size in GATE_SIZES:
        if size > len(unique_docs):
            continue
        gate = list(rng.choice(unique_docs, size=size, replace=False))
        measure(f"gate_{size}_docs", gate, False)
    gate = list(rng.choice(unique_docs, size=min(1_000, len(unique_docs)), replace=False))
    measure("gate_1000_docs_and_date", gate, True)
    res["disk_gb"] = round(engine.disk_bytes() / 1e9, 2)
    print(f"disk {res['disk_gb']} GB", flush=True)
    suffix = "-tuned" if tuned else ""
    (out / f"results-{engine_name}{suffix}.json").write_text(json.dumps(res, indent=2))
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=sorted(ENGINES), required=True)
    ap.add_argument("--model", default="BAAI/bge-m3")
    ap.add_argument("--root", type=Path, default=None, help="Corpus directory (default: the data dir's).")
    ap.add_argument("--random-queries", action="store_true", help="Smoke test: no model load.")
    ap.add_argument("--reuse", action="store_true", help="Search the already-loaded store only.")
    ap.add_argument("--params", type=json.loads, default=None, help="Engine search params (JSON).")
    args = ap.parse_args()
    run(args.engine, args.model, args.root, args.random_queries, args.reuse, args.params)
