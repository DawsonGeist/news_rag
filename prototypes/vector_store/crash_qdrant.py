"""Prototype P2.1: does Qdrant keep every acknowledged write across a hard kill?

Each iteration upserts batches into a scratch collection with ``wait=True`` (Qdrant has
applied the batch to its WAL and segments before acknowledging), SIGKILLs the container
at a random moment, restarts it and checks:
  - every acknowledged batch is fully present (no lost writes)
  - the collection is readable and searchable, with status green after recovery
A batch in flight at the kill may or may not be present; it was never acknowledged.
"""

from __future__ import annotations

import random
import subprocess
import threading
import time

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client import models as m

COLLECTION = "proto_crash"
CONTAINER = "newsrag-qdrant-1"
DIM, BATCH = 1024, 500


def client() -> QdrantClient:
    return QdrantClient(url="http://127.0.0.1:6333", timeout=60)


def wait_up() -> QdrantClient:
    for _ in range(120):
        try:
            c = client()
            c.get_collections()
            return c
        except Exception:
            time.sleep(1)
    raise RuntimeError("qdrant did not come back")


def iteration(n: int, rng: random.Random) -> list[str]:
    c = wait_up()
    if c.collection_exists(COLLECTION):
        c.delete_collection(COLLECTION)
    c.create_collection(
        COLLECTION,
        vectors_config=m.VectorParams(size=DIM, distance=m.Distance.COSINE, on_disk=True),
        quantization_config=m.ScalarQuantization(scalar=m.ScalarQuantizationConfig(type=m.ScalarType.INT8)),
    )
    acked: list[int] = []
    stop = threading.Event()

    def writer() -> None:
        vecs = np.random.default_rng(n).standard_normal((BATCH, DIM)).astype(np.float32)
        b = 0
        while not stop.is_set():
            ids = list(range(b * BATCH, (b + 1) * BATCH))
            try:
                client().upsert(
                    COLLECTION,
                    m.Batch(ids=ids, vectors=vecs.tolist(), payloads=[{"batch": b}] * BATCH),
                    wait=True,
                )
            except Exception:
                return  # the kill landed; this batch was not acknowledged
            acked.append(b)
            b += 1

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(rng.uniform(1.0, 8.0))
    subprocess.run(["docker", "kill", "-s", "KILL", CONTAINER], check=True, capture_output=True)
    t.join(timeout=60)
    stop.set()
    subprocess.run(["docker", "start", CONTAINER], check=True, capture_output=True)
    c = wait_up()
    for _ in range(300):
        if c.get_collection(COLLECTION).status == m.CollectionStatus.GREEN:
            break
        time.sleep(1)
    problems = []
    for b in acked:
        got = c.count(COLLECTION, count_filter=m.Filter(must=[m.FieldCondition(key="batch", match=m.MatchValue(value=b))]), exact=True).count
        if got != BATCH:
            problems.append(f"acknowledged batch {b}: {got}/{BATCH} points")
    hits = c.query_points(COLLECTION, query=np.ones(DIM).tolist(), limit=5).points
    if acked and not hits:
        problems.append("not searchable after recovery")
    print(f"iteration {n}: {len(acked)} batches acknowledged before the kill, {'ok' if not problems else problems}", flush=True)
    return problems


if __name__ == "__main__":
    rng = random.Random(1)
    failures = [p for i in range(10) for p in iteration(i, rng)]
    client().delete_collection(COLLECTION)
    print("PASS" if not failures else f"FAIL: {failures}")
