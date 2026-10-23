"""Knowledge base, step 2: stream the dump through ``pigz`` into Parquet shards in parallel.

The main process only cuts the decompressed stream into blocks at line boundaries;
each worker parses one block and writes its own shard pair (entities, claims), so
nothing large travels back. Memory is bounded by the number of blocks in flight.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from newsrag.kb.extract import EntityRow, rows_from_block
from newsrag.procutil import owned_pool

BLOCK_BYTES = 64 * 1024 * 1024
_TS = pa.timestamp("us", tz="UTC")

ENTITY_SCHEMA = pa.schema(
    [
        ("qid", pa.string()),
        ("label", pa.string()),
        ("description", pa.string()),
        ("aliases", pa.list_(pa.string())),
        ("sitelinks", pa.int32()),
        ("enwiki", pa.string()),
        ("instance_of", pa.list_(pa.string())),
        ("subclass_of", pa.list_(pa.string())),
        ("cik", pa.string()),
        ("ticker", pa.string()),
        ("website", pa.string()),
    ]
)
CLAIM_SCHEMA = pa.schema(
    [
        ("qid", pa.string()),
        ("prop", pa.string()),
        ("target", pa.string()),
        ("start", _TS),
        ("end", _TS),
        ("rank", pa.string()),
    ]
)


class DumpError(RuntimeError):
    pass


def read_blocks(
    dump: Path, block_bytes: int = BLOCK_BYTES, max_blocks: int | None = None
) -> Iterator[bytes]:
    """Decompressed dump in blocks that end on a line boundary. ``max_blocks`` reads only
    the start (tests, or a dump still downloading).

    A dump that fails to decompress (truncated or corrupt download) raises ``DumpError``
    at the end of the stream: silently stopping early would build a partial KB that
    looks complete.
    """
    proc = subprocess.Popen(
        ["pigz", "-dc", str(dump)], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert proc.stdout is not None and proc.stderr is not None
    carry, n, finished = b"", 0, False
    try:
        while True:
            chunk = proc.stdout.read(block_bytes)
            if not chunk:
                finished = True
                break
            data = carry + chunk
            cut = data.rfind(b"\n")
            if cut < 0:
                carry = data
                continue
            yield data[: cut + 1]
            carry = data[cut + 1 :]
            n += 1
            if max_blocks is not None and n >= max_blocks:
                return
        if carry.strip():
            yield carry
    finally:
        if finished:
            err = proc.stderr.read().decode(errors="replace").strip()
            if proc.wait() != 0:
                raise DumpError(f"{dump.name}: decompression failed ({err[:200]})")
        else:
            proc.kill()
            proc.wait()


def _write_shard(rows: list[EntityRow], out: Path, n: int) -> tuple[int, int]:
    ents = pa.table(
        {
            "qid": [r.qid for r in rows],
            "label": [r.label for r in rows],
            "description": [r.description for r in rows],
            "aliases": [r.aliases for r in rows],
            "sitelinks": [r.sitelinks for r in rows],
            "enwiki": [r.enwiki for r in rows],
            "instance_of": [r.instance_of for r in rows],
            "subclass_of": [r.subclass_of for r in rows],
            "cik": [r.cik for r in rows],
            "ticker": [r.ticker for r in rows],
            "website": [r.website for r in rows],
        },
        schema=ENTITY_SCHEMA,
    )
    claims = [c for r in rows for c in r.claims]
    cl = pa.table(
        {
            "qid": [c.qid for c in claims],
            "prop": [c.prop for c in claims],
            "target": [c.target for c in claims],
            "start": [c.start for c in claims],
            "end": [c.end for c in claims],
            "rank": [c.rank for c in claims],
        },
        schema=CLAIM_SCHEMA,
    )
    pq.write_table(ents, out / f"entities-{n:06d}.parquet", compression="zstd")
    pq.write_table(cl, out / f"claims-{n:06d}.parquet", compression="zstd")
    return ents.num_rows, cl.num_rows


def _job(block: bytes, out: str, n: int) -> tuple[int, int]:
    return _write_shard(list(rows_from_block(block)), Path(out), n)


@dataclass
class ExtractStats:
    blocks: int = 0
    entities: int = 0
    claims: int = 0


def extract_dump(
    dump: Path, out: Path, workers: int, max_blocks: int | None = None, progress: bool = True
) -> ExtractStats:
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.parquet"):  # a rerun starts clean; shards are cheap to redo
        old.unlink()
    stats = ExtractStats()
    with owned_pool(workers, max_tasks_per_child=50) as pool:
        pending: set[Future[tuple[int, int]]] = set()

        def collect(block_until: int) -> None:
            nonlocal pending
            while len(pending) > block_until:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for f in done:
                    e, c = f.result()
                    stats.entities += e
                    stats.claims += c

        for n, block in enumerate(read_blocks(dump, max_blocks=max_blocks)):
            pending.add(pool.submit(_job, block, str(out), n))
            stats.blocks += 1
            collect(2 * workers)  # bounded memory: at most ~2x workers blocks in flight
            if progress and stats.blocks % 100 == 0:
                print(
                    f"blocks {stats.blocks} ({stats.blocks * 64 / 1024:.0f} GiB): "
                    f"{stats.entities:,} entities, {stats.claims:,} claims",
                    flush=True,
                )
        collect(0)
    return stats
