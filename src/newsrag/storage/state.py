"""Crawl-state SQLite database (contracts/v1/sql/crawl_state.sql)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import resources
from pathlib import Path

from newsrag.contracts.v1 import SCHEMA_VERSION

DDL_PACKAGE = "newsrag.contracts.v1"
# Applied in order on every start; every statement is CREATE ... IF NOT EXISTS, so an
# existing database gains tables added in later phases without a migration step.
DDL_FILES = ("sql/crawl_state.sql", "sql/enrichment_state.sql")
# Columns added to provisional tables after databases already had them (CREATE TABLE IF
# NOT EXISTS won't add a column). Additive only: (table, column, declaration).
ADDED_COLUMNS = (("enrichments", "entity_ids", "TEXT"),)


def ddl() -> str:
    return "\n".join(
        resources.files(DDL_PACKAGE).joinpath(name).read_text(encoding="utf-8")
        for name in DDL_FILES
    )


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


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """``BEGIN IMMEDIATE`` ... ``COMMIT``, rolled back on any exception.

    IMMEDIATE takes the write lock up front. File installs that must be serialized
    with other writers (blob installs vs. retention unlinks) happen inside it.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def init_schema(conn: sqlite3.Connection) -> None:
    """Create all tables if missing. Idempotent."""
    conn.executescript(ddl())
    for table, column, decl in ADDED_COLUMNS:
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in have:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
    conn.execute(
        "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?) "
        "ON CONFLICT (key) DO NOTHING",
        (SCHEMA_VERSION,),
    )
