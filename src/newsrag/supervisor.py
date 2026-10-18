"""``newsrag run all``: run the poller, fetcher and writer as child processes and keep
them running (Gate 1, G1.2: 72 h unattended).

- A child that exits for any reason other than shutdown is restarted after a backoff
  that doubles with each crash in the last ten minutes (1 s up to 60 s).
- Children get SIGTERM on shutdown and up to ``STOP_TIMEOUT_S`` to drain (the writer
  flushes its buffer), then SIGKILL.
- Children are tied to the supervisor with PR_SET_PDEATHSIG, so if the supervisor is
  killed they die too instead of running on as orphans next to a new supervisor.
- Each child appends to ``logs/<name>.log``; restarts and exits go to
  ``logs/supervisor.log``, which is the G1.2 evidence.
"""

from __future__ import annotations

import ctypes
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

from newsrag.config import Settings
from newsrag.storage.fsio import ProcessLock, sweep_tmp

log = logging.getLogger("newsrag.supervisor")

COMPONENTS = ("poller", "fetcher", "writer")
STOP_TIMEOUT_S = 90.0
CRASH_WINDOW_S = 600.0
MAX_BACKOFF_S = 60.0
_PR_SET_PDEATHSIG = 1


def _die_with_parent() -> None:  # pragma: no cover - runs in the child before exec
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    libc.prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)


@dataclass
class Child:
    name: str
    proc: subprocess.Popen[bytes] | None = None
    log_file: IO[bytes] | None = None
    crashes: deque[float] = field(default_factory=deque)
    restart_at: float = 0.0

    def backoff_s(self, now: float) -> float:
        while self.crashes and now - self.crashes[0] > CRASH_WINDOW_S:
            self.crashes.popleft()
        return min(MAX_BACKOFF_S, 2.0 ** max(0, len(self.crashes) - 1))


class Supervisor:
    def __init__(self, settings: Settings, components: tuple[str, ...] = COMPONENTS) -> None:
        self.s = settings
        self.logs = settings.data_dir / "logs"
        self.children = [Child(name) for name in components]
        self.stopping = threading.Event()

    def _command(self, name: str) -> list[str]:
        return [sys.executable, "-m", "newsrag.cli", "run", name]

    def _start(self, child: Child) -> None:
        if child.log_file is None:
            child.log_file = (self.logs / f"{child.name}.log").open("ab")
        child.proc = subprocess.Popen(
            self._command(child.name),
            stdout=child.log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            preexec_fn=_die_with_parent,
            env=os.environ | {"NEWSRAG_DATA_DIR": str(self.s.data_dir)},
        )
        log.info("started %s pid %d", child.name, child.proc.pid)

    def _check(self, child: Child, now: float) -> None:
        if child.proc is None:
            if now >= child.restart_at:
                self._start(child)
            return
        code = child.proc.poll()
        if code is None:
            return
        child.proc = None
        child.crashes.append(now)
        delay = child.backoff_s(now)
        child.restart_at = now + delay
        log.warning("%s exited with %s; restarting in %.0f s", child.name, code, delay)

    def run(self, poll_s: float = 0.5) -> None:
        self.logs.mkdir(parents=True, exist_ok=True)
        with ProcessLock(self.s.locks_dir / "supervisor.lock"):
            log.info("supervisor pid %d, data dir %s", os.getpid(), self.s.data_dir)
            # Staging areas of processes that died (e.g. with a previous supervisor).
            sweep_tmp(self.s.tmp_dir)
            while not self.stopping.is_set():
                now = time.monotonic()
                for child in self.children:
                    self._check(child, now)
                self.stopping.wait(poll_s)
            self._stop_all()
            # A child killed just before shutdown is never restarted to sweep its own.
            sweep_tmp(self.s.tmp_dir)

    def _stop_all(self) -> None:
        running = [c for c in self.children if c.proc is not None]
        for c in running:
            assert c.proc is not None
            c.proc.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + STOP_TIMEOUT_S
        for c in running:
            assert c.proc is not None
            try:
                c.proc.wait(timeout=max(0.0, deadline - time.monotonic()))
                log.info("%s stopped (exit %s)", c.name, c.proc.returncode)
            except subprocess.TimeoutExpired:
                log.error("%s did not stop in %.0f s; killing", c.name, STOP_TIMEOUT_S)
                c.proc.kill()
                c.proc.wait()
        for c in self.children:
            if c.log_file is not None:
                c.log_file.close()

    def child_pids(self) -> dict[str, int]:
        return {c.name: c.proc.pid for c in self.children if c.proc is not None}


def setup_supervisor_logging(logs_dir: Path) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    file_handler = logging.FileHandler(logs_dir / "supervisor.log")
    file_handler.setFormatter(fmt)
    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stream)
