"""Durable file primitives (contracts/v1/README.md, "Durability protocol").

A file becomes durable in four steps: write it under ``data/tmp/``, fsync it,
``os.replace`` it into place, fsync the destination directory. Directories created
on the way are fsynced into their parents too, or a power cut can lose the new
directory entry along with the file in it. POSIX only: the backend runs on Linux.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import shutil
from pathlib import Path
from types import TracebackType
from typing import Self

from ulid import ULID


def fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def make_dirs(path: Path) -> None:
    """``mkdir -p`` that fsyncs each new directory's parent so the entry survives a crash."""
    missing: list[Path] = []
    p = path
    while not p.exists():
        missing.append(p)
        p = p.parent
    for d in reversed(missing):
        try:
            d.mkdir()
        except FileExistsError:
            continue
        fsync_dir(d.parent)


def write_fsync(path: Path, data: bytes) -> None:
    """Create ``path`` (must not exist), write ``data`` and fsync it."""
    with path.open("xb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def install(src: Path, dest: Path) -> None:
    """Atomically move a fsynced file into place and make the rename durable."""
    make_dirs(dest.parent)
    os.replace(src, dest)
    fsync_dir(dest.parent)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


class LockHeld(RuntimeError):
    pass


def _lock_file(path: Path) -> int:
    """Open and exclusively flock ``path``; raise LockHeld if another process has it.

    Re-checks after locking that ``path`` still names the locked inode: a sweeper may
    have unlinked a stale lock file between our open and our flock.
    """
    while True:
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise LockHeld(str(path)) from None
        try:
            if os.stat(path).st_ino == os.fstat(fd).st_ino:
                return fd
        except FileNotFoundError:
            pass
        os.close(fd)


class ProcessLock:
    """Exclusive, crash-safe lock held for a process's lifetime (the kernel drops a flock
    when its holder dies). Used to enforce "one storage writer"."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> Self:
        make_dirs(self.path.parent)
        self._fd = _lock_file(self.path)
        return self

    def release(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> Self:
        return self.acquire()

    def __exit__(
        self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.release()


class TmpArea:
    """A process's private staging directory, ``data/tmp/<owner>-<pid>-<ulid>/``.

    Several processes share ``data/tmp/``, so startup cleanup must not delete files a
    live process is about to install. Each area is guarded by a flock on
    ``<name>.lock`` beside it; ``sweep_tmp`` removes only areas whose lock is free,
    i.e. whose process has exited or died.
    """

    def __init__(self, tmp_root: Path, owner: str) -> None:
        self.root = tmp_root
        self.name = f"{owner}-{os.getpid()}-{ULID()}"
        self.dir = tmp_root / self.name
        self._lock = tmp_root / f"{self.name}.lock"
        self._fd: int | None = None

    def open(self) -> Self:
        make_dirs(self.root)
        # Lock before creating the directory, so a sweeper never sees our directory
        # without a held lock beside it.
        self._fd = _lock_file(self._lock)
        self.dir.mkdir()
        return self

    def new_path(self, suffix: str = "") -> Path:
        if self._fd is None:
            raise RuntimeError("TmpArea not open")
        return self.dir / f"{ULID()}{suffix}"

    def close(self) -> None:
        if self._fd is None:
            return
        shutil.rmtree(self.dir, ignore_errors=True)
        self._lock.unlink(missing_ok=True)
        os.close(self._fd)
        self._fd = None

    def __enter__(self) -> Self:
        return self.open()

    def __exit__(
        self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()


def sweep_tmp(tmp_root: Path) -> int:
    """Delete staging areas of dead processes and any loose files. Returns entries removed."""
    if not tmp_root.exists():
        return 0
    removed = 0
    for lock in tmp_root.glob("*.lock"):
        try:
            fd = _lock_file(lock)
        except LockHeld:
            continue
        try:
            shutil.rmtree(tmp_root / lock.stem, ignore_errors=True)
            lock.unlink(missing_ok=True)
            removed += 1
        finally:
            os.close(fd)
    for entry in tmp_root.iterdir():
        if entry.suffix == ".lock" or (tmp_root / f"{entry.name}.lock").exists():
            continue
        # A directory without a lock file, or a stray file: nobody can be using it,
        # because areas take their lock before creating their directory.
        if entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)
        else:
            entry.unlink(missing_ok=True)
        removed += 1
    return removed
