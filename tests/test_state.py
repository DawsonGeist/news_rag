from pathlib import Path

from newsrag.storage.state import connect, init_schema

EXPECTED_TABLES = {
    "schema_meta",
    "sources",
    "feeds",
    "feed_health",
    "feed_polls",
    "hosts",
    "frontier",
    "url_sightings",
    "blobs",
    "table_files",
    "doc_revisions",
    "stage_events",
}


def test_init_schema_idempotent(tmp_path: Path) -> None:
    conn = connect(tmp_path / "state" / "crawl.sqlite3")
    init_schema(conn)
    init_schema(conn)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables >= EXPECTED_TABLES
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    version = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    assert version[0] == "1.0"
