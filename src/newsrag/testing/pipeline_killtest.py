"""Full-pipeline kill test (Phase 1 design, "Testing"; Gate 1 criterion G1.4).

Each iteration serves two synthetic feeds and their articles from local HTTP servers
on two loopback hosts, starts ``newsrag run all``, and SIGKILLs a random process (a
child, or the supervisor itself) at random moments, restarting the supervisor
whenever it was the victim. It then waits for the pipeline to settle, stops it
cleanly and checks the invariants:

- every committed file exists and matches its sha256, and holds its ledger rows
- no duplicate ``(doc_id, revision)``, revisions contiguous
- every discovered URL reached ``stored`` (all pages are healthy)
- exactly the expected Documents exist, one revision each
- ``tmp/`` is empty and no pipeline process is left running
"""

from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from newsrag import ids
from newsrag.config import Settings, load_settings
from newsrag.contracts.v1 import FeedCandidate, FeedMedium, FeedOrigin
from newsrag.feeds.registry import import_candidates, set_status
from newsrag.storage.recovery import verify_storage
from newsrag.storage.state import connect, init_schema
from newsrag.storage.tables import DOCUMENTS, committed_files, read_documents
from newsrag.timeutil import utcnow

HOSTS = ("127.0.0.1", "127.0.0.2")
STORIES_PER_HOST = 12
SETTLE_TIMEOUT_S = 180.0

CONFIG = """
[fetch]
default_crawl_delay_s = 0.0
request_timeout_s = 3.0
retry_base_s = 1.0
lease_s = 4
reservation_slack_s = 1.0

[poll]
min_interval_s = 2
default_interval_s = 2

[writer]
flush_rows = 8
flush_seconds = 0.5
extract_workers = 2
"""


def _story(host: str, i: int) -> bytes:
    paras = "".join(
        f"<p>Paragraph {p} of story {i} from {host}. The council met again on Tuesday "
        f"to discuss the budget, the new library hours and the river trail project.</p>"
        for p in range(4)
    )
    return (
        f'<!doctype html><html><head><meta charset="utf-8"><title>Story {i}</title>'
        f'<script type="application/ld+json">{{"@type": "NewsArticle", "headline": '
        f'"Story {i} from {host}", "datePublished": "2026-10-1{i % 7}T12:00:00Z"}}</script>'
        f"</head><body><article><h1>Story {i} from {host}</h1>{paras}</article></body></html>"
    ).encode()


def _feed(base: str) -> bytes:
    items = "".join(
        f"<item><title>Story {i}</title><link>{base}/story/{i}</link>"
        f"<pubDate>Fri, 16 Oct 2026 12:00:00 GMT</pubDate></item>"
        for i in range(STORIES_PER_HOST)
    )
    return (
        f'<?xml version="1.0"?><rss version="2.0"><channel><title>t</title>'
        f"<link>{base}/</link><description>d</description>{items}</channel></rss>"
    ).encode()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        addr: Any = self.server.server_address
        host, port = str(addr[0]), int(addr[1])
        base = f"http://{host}:{port}"
        if self.path == "/robots.txt":
            body, ctype, status = b"User-agent: *\nAllow: /\n", "text/plain", 200
        elif self.path == "/feed.xml":
            body, ctype, status = _feed(base), "application/rss+xml", 200
        elif self.path.startswith("/story/"):
            body, ctype, status = (
                _story(host, int(self.path.rsplit("/", 1)[1])),
                "text/html",
                200,
            )
        else:
            body, ctype, status = b"", "text/plain", 404
        self.send_response(status)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        pass


@contextmanager
def _servers() -> Iterator[list[str]]:
    servers = [ThreadingHTTPServer((h, 0), _Handler) for h in HOSTS]
    for srv in servers:
        threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        addrs: list[Any] = [s.server_address for s in servers]
        yield [f"http://{a[0]}:{a[1]}" for a in addrs]
    finally:
        for srv in servers:
            srv.shutdown()
            srv.server_close()


def _children(pid: int) -> list[int]:
    """Direct children of ``pid``, from /proc/*/stat (field 4 is the parent pid)."""
    out = []
    for stat in Path("/proc").glob("[0-9]*/stat"):
        try:
            fields = stat.read_text().rsplit(")", 1)[1].split()
        except OSError:
            continue
        if int(fields[1]) == pid:
            out.append(int(stat.parent.name))
    return out


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still "exists" until reaped; it is dead for our purposes.
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except OSError:
        return False
    return state != "Z"


def _wait_dead(pids: list[int], timeout_s: float) -> list[int]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        pids = [p for p in pids if _alive(p)]
        if not pids:
            return []
        time.sleep(0.1)
    return pids


@dataclass
class _Run:
    settings: Settings
    env: dict[str, str]
    proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        log = (self.settings.data_dir / "run-all.out").open("ab")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "newsrag.cli", "run", "all"],
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
        # Wait until the supervisor is up (signal handlers installed, children
        # started): a SIGTERM during interpreter startup would bypass the graceful stop.
        deadline = time.monotonic() + 30
        while len(_children(self.proc.pid)) < 3 and time.monotonic() < deadline:
            time.sleep(0.05)

    def stop(self) -> list[str]:
        """SIGTERM the supervisor; report anything that failed to stop."""
        problems = []
        assert self.proc is not None
        kids = _children(self.proc.pid)
        self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=120)
            if code != 0:
                problems.append(f"supervisor exited {code} on SIGTERM")
        except subprocess.TimeoutExpired:
            problems.append("supervisor did not stop")
            self.proc.kill()
        if left := _wait_dead(kids, 10):
            problems.append(f"processes left running: {left}")
            for p in left:
                os.kill(p, signal.SIGKILL)
        return problems


def _settled(settings: Settings, expected: int) -> bool:
    conn = connect(settings.state_db)
    try:
        row = conn.execute("SELECT count(*), sum(state = 'stored') FROM frontier").fetchone()
        return bool(row[0] == expected and row[1] == expected)
    finally:
        conn.close()


def _check(settings: Settings, bases: list[str]) -> list[str]:
    conn = connect(settings.state_db)
    try:
        problems = verify_storage(conn, settings.data_dir)
        states = dict(conn.execute("SELECT state, count(*) FROM frontier GROUP BY 1").fetchall())
        if set(states) != {"stored"}:
            problems.append(f"frontier states: {states}")
        docs = read_documents(committed_files(conn, settings.data_dir, DOCUMENTS))
        want = {ids.doc_id(f"{b}/story/{i}") for b in bases for i in range(STORIES_PER_HOST)}
        got = [(d.doc_id, d.revision) for d in docs]
        if sorted(d for d, _ in got) != sorted(want) or any(r != 1 for _, r in got):
            problems.append(f"documents: {len(got)} stored, {len(want)} expected")
        tmp = settings.tmp_dir
        if tmp.exists() and any(tmp.iterdir()):
            problems.append(f"tmp not empty: {[p.name for p in tmp.iterdir()]}")
        return problems
    finally:
        conn.close()


def run_iteration(root: Path, rng: random.Random, kills: int) -> tuple[int, list[str]]:
    data_dir = root / "data"
    cfg = root / "newsrag.toml"
    cfg.write_text(f'data_dir = "{data_dir}"\n{CONFIG}', encoding="utf-8")
    env = os.environ | {"NEWSRAG_CONFIG": str(cfg), "NEWSRAG_DATA_DIR": str(data_dir)}
    settings = load_settings(cfg)
    with _servers() as bases:
        conn = connect(settings.state_db)
        init_schema(conn)
        cands = []
        for b in bases:
            url = ids.normalize_url(f"{b}/feed.xml")
            cands.append(
                FeedCandidate(
                    feed_id=ids.feed_id(url),
                    feed_url=url,
                    source_id=ids.registered_domain(url),
                    listed_domain=ids.registered_domain(url),
                    country="United States",
                    categories=["Local News"],
                    medium=FeedMedium.TEXT,
                    origin=FeedOrigin.MANUAL,
                )
            )
        import_candidates(conn, cands, utcnow())
        set_status(conn, [c.feed_id for c in cands], "active", "killtest", utcnow())
        conn.close()

        run = _Run(settings, env)
        run.start()
        done_kills = 0
        for _ in range(kills):
            time.sleep(rng.uniform(0.3, 3.0))
            assert run.proc is not None
            kids = _children(run.proc.pid)
            if rng.random() < 0.25 or not kids:
                run.proc.send_signal(signal.SIGKILL)
                run.proc.wait()
                # Children follow via PDEATHSIG; wait so the new supervisor's writer
                # does not race the dying one for the writer lock.
                _wait_dead(kids, 30)
                run.start()
            else:
                os.kill(rng.choice(kids), signal.SIGKILL)
            done_kills += 1

        expected = len(bases) * STORIES_PER_HOST
        deadline = time.monotonic() + SETTLE_TIMEOUT_S
        while not _settled(settings, expected) and time.monotonic() < deadline:
            time.sleep(0.5)
        problems = run.stop()
        problems += _check(settings, bases)
        if problems:
            out = (data_dir / "run-all.out").read_text(errors="replace")[-2000:]
            problems.append(f"supervisor output tail: {out}")
        return done_kills, problems


def run_pipeline_kill_test(
    iterations: int = 20, kills_per_iteration: int = 3, seed: int | None = None
) -> list[str]:
    rng = random.Random(seed)
    failures = []
    with tempfile.TemporaryDirectory(prefix="newsrag-pipeline-kill-") as root:
        for n in range(iterations):
            it_root = Path(root) / f"it{n}"
            it_root.mkdir()
            kills, problems = run_iteration(it_root, rng, kills_per_iteration)
            print(
                f"iteration {n + 1}/{iterations}: {kills} kills, "
                f"{'ok' if not problems else 'FAIL'}",
                flush=True,
            )
            if problems:
                failures.append(f"iteration {n + 1}: {problems[:6]}")
    return failures
