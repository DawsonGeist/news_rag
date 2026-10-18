"""Kill test for the storage layer (Phase 1 design, "Testing"; Gate 1 criterion G1.4).

A child process runs a deterministic workload against a fresh data directory and
is SIGKILLed at random moments. After each kill it is restarted; the last run goes
to completion. Then every storage invariant is checked, plus the exact expected
content: each synthetic story must be stored with exactly its distinct texts.

The workload plays the fetcher (blob install + frontier ``fetched``), the storage
writer (revisions, Parquet part, frontier ``stored``) and the retention job. Once
the real fetcher, extractor and poller exist (P1.2 to P1.4), this harness grows
into the full-pipeline kill test against the local fixture server.
"""

from __future__ import annotations

import argparse
import random
import re
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from newsrag import ids
from newsrag.contracts.v1 import ContentKind, DateSource, Document, Pipeline
from newsrag.obs.events import worker_id
from newsrag.storage.blobs import BlobStore, expire_blobs, sweep_blobs
from newsrag.storage.compaction import compact
from newsrag.storage.fsio import ProcessLock, TmpArea
from newsrag.storage.recovery import reclaim_expired_leases, recover_writer, verify_storage
from newsrag.storage.state import connect, init_schema, transaction
from newsrag.storage.writer import WRITER_LOCK, ExtractedDoc, StorageWriter, claim_for_extraction
from newsrag.timeutil import from_db, to_db, utcnow

N_STORIES = 40
BATCH = 7
LEASE_S = 1  # short, so a restarted worker can reclaim the dead one's leases quickly
VERSION = "killtest-0"


def story_variants(i: int) -> int:
    """How many discovered URLs (tracking variants) point at story ``i``."""
    return 1 + i % 3


def story_url(i: int, k: int) -> str:
    return f"https://example.com/story/{i}?variant={k}"


def story_canonical(i: int) -> str:
    return f"https://example.com/story/{i}"


def story_text(i: int, k: int) -> str:
    # Variants alternate between two texts, so story i has min(variants, 2) revisions
    # and every third story exercises the "unchanged content_hash" path.
    return f"Story {i} edition {k % 2}.\n\nSecond paragraph of story {i}."


def expected_hashes() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for i in range(N_STORIES):
        d = ids.doc_id(story_canonical(i))
        out[d] = {
            ids.content_hash(f"Story {i}", story_text(i, k)) for k in range(story_variants(i))
        }
    return out


def _body(i: int, k: int) -> bytes:
    text = story_text(i, k).replace("\n\n", "</p><p>")
    return (
        f'<html><head><link rel="canonical" href="{story_canonical(i)}"><title>Story {i}</title>'
        f"</head><body><p>{text}</p></body></html>"
    ).encode()


def _parse(body: bytes) -> tuple[int, int]:
    """Recover (story, edition) from a synthetic page: a stand-in for the extractor."""
    m = re.search(rb"<p>Story (\d+) edition (\d+)\.", body)
    if m is None:
        raise ValueError("not a killtest page")
    return int(m.group(1)), int(m.group(2))


def run_worker(data_dir: Path) -> None:
    conn = connect(data_dir / "state" / "crawl.sqlite3")
    init_schema(conn)
    wid = worker_id("killtest")
    with (
        ProcessLock(data_dir / "state" / "locks" / WRITER_LOCK),
        TmpArea(data_dir / "tmp", "killtest") as tmp,
    ):
        recover_writer(conn, data_dir, utcnow())
        reclaim_expired_leases(conn, "fetching", utcnow())
        blobs = BlobStore(data_dir, tmp)
        writer = StorageWriter(conn, data_dir, tmp, wid, VERSION)

        # Poller: discover every URL (idempotent).
        now = to_db(utcnow())
        with transaction(conn):
            conn.executemany(
                "INSERT INTO frontier (url_id, url, host, source_id, discovered_at, state, "
                "ready_at, next_attempt_at, updated_at) VALUES (?, ?, 'example.com', "
                "'example.com', ?, 'pending', ?, ?, ?) ON CONFLICT DO NOTHING",
                [
                    (ids.url_id(story_url(i, k)), story_url(i, k), now, now, now, now)
                    for i in range(N_STORIES)
                    for k in range(story_variants(i))
                ],
            )

        while True:
            # Fetcher: claim, "download", stage the blob, commit blob + frontier together.
            with transaction(conn):
                rows = conn.execute(
                    "SELECT url_id, url, attempts FROM frontier WHERE state = 'pending' LIMIT ?",
                    (BATCH,),
                ).fetchall()
                lease = to_db(utcnow() + timedelta(seconds=LEASE_S))
                conn.executemany(
                    "UPDATE frontier SET state = 'fetching', lease_owner = ?, "
                    "lease_expires_at = ?, attempts = attempts + 1 WHERE url_id = ?",
                    [(wid, lease, r["url_id"]) for r in rows],
                )
            for r in rows:
                i = int(r["url"].split("/story/")[1].split("?")[0])
                k = int(r["url"].split("variant=")[1])
                staged = blobs.stage(_body(i, k), "text/html")
                with transaction(conn):
                    now_dt = utcnow()
                    blobs.commit(conn, staged, now_dt, now_dt + timedelta(days=60))
                    conn.execute(
                        "UPDATE frontier SET state = 'fetched', lease_owner = NULL, "
                        "lease_expires_at = NULL, final_url = url, raw_blob_hash = ?, "
                        "fetched_at = ?, ready_at = ?, updated_at = ? WHERE url_id = ?",
                        (
                            staged.blob_hash,
                            to_db(now_dt),
                            to_db(now_dt),
                            to_db(now_dt),
                            r["url_id"],
                        ),
                    )

            # Writer: claim, extract, commit one part.
            claims = claim_for_extraction(conn, wid, BATCH, LEASE_S)
            items = []
            for c in claims:
                started = utcnow()
                row = conn.execute(
                    "SELECT discovered_at, fetched_at FROM frontier WHERE url_id = ?", (c.url_id,)
                ).fetchone()
                i, edition = _parse(blobs.read(c.raw_blob_hash))
                text = story_text(i, edition)
                canonical = ids.choose_canonical(c.final_url or c.url, story_canonical(i))
                doc = Document(
                    doc_id=ids.doc_id(canonical),
                    revision=1,
                    pipeline=Pipeline.RSS,
                    source_id="example.com",
                    canonical_url=canonical,
                    url_canonicalizer_version=ids.CANONICALIZER_VERSION,
                    final_url=c.final_url or c.url,
                    title=f"Story {i}",
                    extracted_text=text,
                    content_kind=ContentKind.FULL_TEXT,
                    language="en",
                    word_count=len(text.split()),
                    published_at=None,
                    published_at_source=DateSource.NONE,
                    discovered_at=from_db(row["discovered_at"]),
                    fetched_at=from_db(row["fetched_at"]),
                    ingested_at=utcnow(),
                    content_hash=ids.content_hash(f"Story {i}", text),
                    raw_blob_hash=c.raw_blob_hash,
                    extractor_version=VERSION,
                    pipeline_version=VERSION,
                )
                items.append(ExtractedDoc(doc, c.url_id, c.ready_at, started, utcnow()))
            writer.commit(items)
            # Compact after every commit, with a tiny target, so kills land mid-compaction.
            compact(conn, data_dir, tmp, utcnow(), target_bytes=4096)

            if not rows and not claims:
                left = conn.execute(
                    "SELECT count(*) FROM frontier WHERE state != 'stored'"
                ).fetchone()[0]
                if left == 0:
                    break
                # Rows still leased by a killed predecessor: wait for the leases to lapse.
                time.sleep(LEASE_S / 2)
                reclaim_expired_leases(conn, "fetching", utcnow())
                reclaim_expired_leases(conn, "extracting", utcnow())

        # Retention: expire every blob, then the daily orphan sweep.
        expire_blobs(conn, data_dir, utcnow() + timedelta(days=365))
        sweep_blobs(conn, data_dir)
    conn.close()


def check(data_dir: Path) -> list[str]:
    conn = connect(data_dir / "state" / "crawl.sqlite3")
    try:
        problems = verify_storage(conn, data_dir)
        not_done = conn.execute("SELECT count(*) FROM frontier WHERE state != 'stored'").fetchone()
        if not_done[0]:
            problems.append(f"{not_done[0]} frontier rows not stored")
        got: dict[str, set[str]] = {}
        for r in conn.execute("SELECT doc_id, content_hash FROM doc_revisions"):
            got.setdefault(r["doc_id"], set()).add(r["content_hash"])
        if got != expected_hashes():
            problems.append("stored revisions differ from the expected content")
        tmp = data_dir / "tmp"
        if tmp.exists() and any(tmp.iterdir()):
            problems.append(f"tmp not empty: {[p.name for p in tmp.iterdir()]}")
        return problems
    finally:
        conn.close()


def _spawn(data_dir: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "newsrag.testing.killtest", "worker", str(data_dir)],
        stderr=subprocess.PIPE,
    )


@dataclass
class IterationResult:
    kills: int
    problems: list[str]


def run_iteration(
    data_dir: Path, rng: random.Random, window_s: float, kills: int
) -> IterationResult:
    done_kills = 0
    for _ in range(kills):
        proc = _spawn(data_dir)
        try:
            proc.wait(timeout=rng.uniform(0.0, window_s))
        except subprocess.TimeoutExpired:
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            done_kills += 1
            continue
        if proc.returncode != 0:  # finished early: fine, unless it crashed
            assert proc.stderr is not None
            return IterationResult(done_kills, [f"worker failed: {proc.stderr.read().decode()}"])
    proc = _spawn(data_dir)
    _, err = proc.communicate(timeout=300)
    if proc.returncode != 0:
        return IterationResult(done_kills, [f"final worker failed: {err.decode()}"])
    return IterationResult(done_kills, check(data_dir))


def run_kill_test(
    iterations: int = 20, kills_per_iteration: int = 2, seed: int | None = None
) -> list[str]:
    """Returns one failure line per failed iteration (empty list = pass)."""
    rng = random.Random(seed)
    failures = []
    with tempfile.TemporaryDirectory(prefix="newsrag-killtest-") as root:
        # Calibrate the kill window to an uninterrupted run.
        start = time.monotonic()
        clean = run_iteration(Path(root) / "calibrate", rng, 0.0, 0)
        window = (time.monotonic() - start) * 1.2
        if clean.problems:
            return [f"uninterrupted run failed: {clean.problems}"]
        for n in range(iterations):
            res = run_iteration(Path(root) / f"it{n}", rng, window, kills_per_iteration)
            print(
                f"iteration {n + 1}/{iterations}: {res.kills} kills, "
                f"{'ok' if not res.problems else 'FAIL'}",
                flush=True,
            )
            if res.problems:
                failures.append(f"iteration {n + 1}: {res.problems[:5]}")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("data_dir", type=Path)
    args = parser.parse_args()
    if args.cmd == "worker":
        run_worker(args.data_dir)


if __name__ == "__main__":
    main()
