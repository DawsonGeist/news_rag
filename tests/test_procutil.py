import os
import signal
import subprocess
import sys
import time
from pathlib import Path

SCRIPT = """
import os, sys, time
from newsrag.procutil import owned_pool

def pid() -> int:
    time.sleep(0.2)
    return os.getpid()

if __name__ == "__main__":
    pool = owned_pool(2)
    pids = {f.result() for f in [pool.submit(pid) for _ in range(6)]}
    print(" ".join(map(str, sorted(pids))), flush=True)
    time.sleep(60)
"""


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    return state != "Z"


def test_pool_workers_die_when_owner_is_sigkilled(tmp_path: Path) -> None:
    script = tmp_path / "owner.py"
    script.write_text(SCRIPT)
    owner = subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE, text=True)
    assert owner.stdout is not None
    workers = [int(p) for p in owner.stdout.readline().split()]
    assert workers and all(_alive(p) for p in workers)
    owner.send_signal(signal.SIGKILL)
    owner.wait()
    deadline = time.monotonic() + 5
    while any(_alive(p) for p in workers) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not any(_alive(p) for p in workers), "pool workers outlived their owner"
