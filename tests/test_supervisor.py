import sys
import threading
import time
from pathlib import Path

from newsrag.config import Settings
from newsrag.supervisor import Child, Supervisor


def test_backoff_doubles_within_window_and_resets() -> None:
    c = Child("x")
    assert c.backoff_s(0) == 1
    for t, want in [(1, 1), (2, 2), (3, 4), (4, 8)]:
        c.crashes.append(t)
        assert c.backoff_s(t) == want
    for t in range(5, 20):
        c.crashes.append(t)
    assert c.backoff_s(20) == 60  # capped
    assert c.backoff_s(20 + 601) == 1  # old crashes age out


class _Fake(Supervisor):
    """Children are tiny scripts: 'crasher' exits at once, 'sleeper' runs until SIGTERM."""

    def _command(self, name: str) -> list[str]:
        code = "import sys; sys.exit(3)" if name == "crasher" else "import time; time.sleep(60)"
        return [sys.executable, "-c", code]


def test_restarts_crashed_child_and_stops_cleanly(tmp_path: Path) -> None:
    sup = _Fake(Settings(data_dir=tmp_path / "data"), ("crasher", "sleeper"), jobs=())
    t = threading.Thread(target=sup.run, kwargs={"poll_s": 0.05})
    t.start()
    try:
        deadline = time.monotonic() + 10
        crasher = sup.children[0]
        while len(crasher.crashes) < 2 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert len(crasher.crashes) >= 2  # restarted after the first exit
        sleeper_pid = sup.child_pids()["sleeper"]
    finally:
        sup.stopping.set()
        t.join(timeout=30)
    assert not t.is_alive()
    assert sup.children[1].proc is not None and sup.children[1].proc.returncode is not None
    assert sleeper_pid > 0
    assert (tmp_path / "data" / "logs" / "crasher.log").exists()


def test_jobs_run_on_schedule(tmp_path: Path) -> None:
    sup = Supervisor(
        Settings(data_dir=tmp_path / "data"), (), jobs=(("help", ("--help",), 0.0, 3600.0),)
    )
    t = threading.Thread(target=sup.run, kwargs={"poll_s": 0.05})
    t.start()
    try:
        deadline = time.monotonic() + 20
        log = tmp_path / "data" / "logs" / "help.log"
        while not (log.exists() and b"Usage" in log.read_bytes()) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert b"Usage" in log.read_bytes()
        assert len(sup.job_due) == 1  # scheduled once; next run an hour out
    finally:
        sup.stopping.set()
        t.join(timeout=30)
