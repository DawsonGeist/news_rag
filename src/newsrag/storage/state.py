"""Crawl-state SQLite database (contracts/v1/sql/crawl_state.sql)."""

from __future__ import annotations

import sqlite3
from importlib import resources
from pathlib import Path

from newsrag.contracts.v1 import SCHEMA_VERSION

DDL_RESOURCE = ("newsrag.contracts.v1", "sql/crawl_state.sql")


def ddl() -> str:
    package, name = DDL_RESOURCE
    return resources.files(package).joinpath(name).read_text(encoding="utf-8")


def connect(path: Path) -> sqlite3.Connection:
    """Open the state DB with the durability settings every component must use.

    Never open this file over a network mount; WAL needs shared memory on one host.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)  # explicit BEGIN/COMMIT
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create all tables if missing. Idempotent."""
    conn.executescript(ddl())
    conn.execute(
        "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?) "
        "ON CONFLICT (key) DO NOTHING",
        (SCHEMA_VERSION,),
    )
