"""Process pools whose workers die with their owner.

Python 3.14's default start method is ``forkserver``: pool workers are children of a
forkserver process, not of the process that owns the pool. When the owner is
SIGKILLed (a crash, the kill test, a supervisor restart), the forkserver is adopted
by init and lives on with its workers, about 150 MB each. The kill tests leaked one
such tree per killed writer.

Here, workers are started with ``spawn``, so they are direct children of the owner,
and each sets ``PR_SET_PDEATHSIG(SIGKILL)``: the kernel kills it the moment its
parent dies. PDEATHSIG follows the parent *thread*; workers are started from the
owner's main thread or the executor's manager thread, which live as long as the pool.
"""

from __future__ import annotations

import ctypes
import multiprocessing
import os
import signal
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from typing import Any

_PR_SET_PDEATHSIG = 1


def die_with_parent(sig: int = signal.SIGKILL) -> None:
    """Ask the kernel to send ``sig`` to this process when its parent dies (Linux)."""
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    if libc.prctl(_PR_SET_PDEATHSIG, sig) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")


def _worker_init(
    parent_pid: int, user_init: Callable[..., None] | None, args: tuple[Any, ...]
) -> None:
    die_with_parent()
    # The parent may have died between spawn and prctl; then nobody would signal us.
    if os.getppid() != parent_pid:
        os._exit(1)
    if user_init is not None:
        user_init(*args)


def owned_pool(
    workers: int,
    *,
    initializer: Callable[..., None] | None = None,
    initargs: tuple[Any, ...] = (),
    max_tasks_per_child: int | None = 1000,
    mp_context: Any = None,
) -> ProcessPoolExecutor:
    """A ProcessPoolExecutor whose workers cannot outlive this process."""
    ctx = mp_context or multiprocessing.get_context("spawn")
    return ProcessPoolExecutor(
        max_workers=workers,
        mp_context=ctx,
        initializer=_worker_init,
        initargs=(os.getpid(), initializer, initargs),
        max_tasks_per_child=max_tasks_per_child,
    )
