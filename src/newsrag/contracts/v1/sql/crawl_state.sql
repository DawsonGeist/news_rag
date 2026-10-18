-- newsrag crawl state, contract v1.
--
-- Mutable, transactional state for Phase 1. Opened with journal_mode=WAL,
-- synchronous=FULL, foreign_keys=ON (see newsrag.storage.state).
-- Timestamps are UTC text in the fixed format YYYY-MM-DDTHH:MM:SS.ffffffZ so
-- that string order is time order. Each table names its single owning
-- component; other components only read it.

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

-- Publishers. Owner: feed vetter.
CREATE TABLE IF NOT EXISTS sources (
    source_id    TEXT PRIMARY KEY,          -- registered domain, e.g. 'foxnews.com'
    country      TEXT NOT NULL,
    display_name TEXT,
    listed_rank  INTEGER,
    created_at   TEXT NOT NULL
) STRICT;

-- Feeds. Owner: vetter writes candidate/status columns; poller writes poll columns.
CREATE TABLE IF NOT EXISTS feeds (
    feed_id              TEXT PRIMARY KEY,
    feed_url             TEXT NOT NULL,
    source_id            TEXT NOT NULL REFERENCES sources(source_id),
    categories_json      TEXT NOT NULL,     -- JSON array, primary category first
    medium               TEXT NOT NULL CHECK (medium IN ('text', 'audio', 'video')),
    origin               TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'candidate'
                         CHECK (status IN ('candidate', 'active', 'review', 'rejected', 'paused')),
    status_reason        TEXT,
    etag                 TEXT,
    last_modified        TEXT,
    poll_interval_s      INTEGER NOT NULL DEFAULT 1800,
    next_poll_at         TEXT,
    last_polled_at       TEXT,
    last_success_at      TEXT,
    last_new_item_at     TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS feeds_due ON feeds (status, next_poll_at);
CREATE INDEX IF NOT EXISTS feeds_source ON feeds (source_id);

-- Vetting history, append-only; the latest row per feed is current. Owner: vetter.
CREATE TABLE IF NOT EXISTS feed_health (
    feed_id     TEXT NOT NULL REFERENCES feeds(feed_id),
    checked_at  TEXT NOT NULL,
    verdict     TEXT NOT NULL CHECK (verdict IN ('accept', 'reject', 'review')),
    report_json TEXT NOT NULL,              -- FeedHealthReport v1
    PRIMARY KEY (feed_id, checked_at)
) STRICT;

-- One row per poll attempt. Owner: poller. Pruned after poll_log_retention_days.
CREATE TABLE IF NOT EXISTS feed_polls (
    feed_id      TEXT NOT NULL REFERENCES feeds(feed_id),
    polled_at    TEXT NOT NULL,
    http_status  INTEGER,
    not_modified INTEGER NOT NULL DEFAULT 0,
    items_seen   INTEGER NOT NULL DEFAULT 0,
    items_new    INTEGER NOT NULL DEFAULT 0,
    duration_ms  INTEGER,
    error_code   TEXT,
    PRIMARY KEY (feed_id, polled_at)
) STRICT;

-- Per-host politeness and robots cache. Owner: fetcher (the poller reads it for feed hosts).
CREATE TABLE IF NOT EXISTS hosts (
    host               TEXT PRIMARY KEY,    -- e.g. 'www.foxnews.com'
    robots_txt         TEXT,
    robots_status      INTEGER,
    robots_fetched_at  TEXT,
    crawl_delay_s      REAL NOT NULL,
    next_allowed_at    TEXT,
    backoff_until      TEXT,
    consecutive_errors INTEGER NOT NULL DEFAULT 0
) STRICT;

-- URL frontier: the work queue for fetch and extract.
-- Owner: poller inserts; fetcher moves pending->fetching->fetched;
-- storage writer moves fetched->extracting->stored. Leases make claims crash-safe:
-- a row whose lease expired is reclaimable by any worker.
CREATE TABLE IF NOT EXISTS frontier (
    url_id            TEXT PRIMARY KEY,     -- ids.url_id(url)
    url               TEXT NOT NULL,        -- normalized, as discovered
    host              TEXT NOT NULL,
    source_id         TEXT NOT NULL,
    feed_id           TEXT,                 -- first feed that surfaced it
    discovered_at     TEXT NOT NULL,
    feed_published_at TEXT,
    feed_updated_at   TEXT,
    feed_title        TEXT,
    feed_summary      TEXT,
    state             TEXT NOT NULL CHECK (state IN
                      ('pending', 'fetching', 'fetched', 'extracting', 'stored', 'failed', 'skipped')),
    priority          INTEGER NOT NULL DEFAULT 0,  -- higher first
    attempts          INTEGER NOT NULL DEFAULT 0,
    ready_at          TEXT NOT NULL,        -- when it entered its current ready state
    next_attempt_at   TEXT NOT NULL,
    lease_owner       TEXT,
    lease_expires_at  TEXT,
    final_url         TEXT,
    http_status       INTEGER,
    content_type      TEXT,
    raw_blob_hash     TEXT,
    fetched_at        TEXT,
    doc_id            TEXT,
    revision          INTEGER,
    skip_reason       TEXT,
    last_error        TEXT,
    updated_at        TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS frontier_ready ON frontier (state, next_attempt_at);
CREATE INDEX IF NOT EXISTS frontier_host ON frontier (host, state);
CREATE INDEX IF NOT EXISTS frontier_doc ON frontier (doc_id);
CREATE INDEX IF NOT EXISTS frontier_lease ON frontier (state, lease_expires_at);

-- Every (url, feed) sighting; the same story often appears in several feeds. Owner: poller.
CREATE TABLE IF NOT EXISTS url_sightings (
    url_id        TEXT NOT NULL,
    feed_id       TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (url_id, feed_id)
) STRICT;

-- Content-addressed raw blobs. Owner: fetcher inserts; retention job expires.
-- Retention marks state='expired' in a transaction, then unlinks the file.
CREATE TABLE IF NOT EXISTS blobs (
    blob_hash    TEXT PRIMARY KEY,          -- sha256 of the raw body bytes
    rel_path     TEXT NOT NULL,             -- raw/ab/cd/<hash>.html.zst
    size_raw     INTEGER NOT NULL,
    size_stored  INTEGER NOT NULL,
    content_type TEXT,
    stored_at    TEXT NOT NULL,
    expires_at   TEXT,                      -- NULL = keep forever (golden set, pinned)
    state        TEXT NOT NULL CHECK (state IN ('present', 'expired'))
) STRICT;
CREATE INDEX IF NOT EXISTS blobs_expiry ON blobs (state, expires_at);

-- Committed table files (immutable Parquet parts). Owner: storage writer ONLY.
-- A file that is not listed here with state='committed' does not exist.
CREATE TABLE IF NOT EXISTS table_files (
    file_id              TEXT PRIMARY KEY,  -- ULID
    table_name           TEXT NOT NULL,     -- 'documents'
    table_schema_version TEXT NOT NULL,
    rel_path             TEXT NOT NULL UNIQUE,
    row_count            INTEGER NOT NULL,
    size_bytes           INTEGER NOT NULL,
    sha256               TEXT NOT NULL,
    min_ingested_at      TEXT,
    max_ingested_at      TEXT,
    state                TEXT NOT NULL CHECK (state IN ('committed', 'superseded')),
    superseded_by        TEXT,
    created_at           TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS table_files_table ON table_files (table_name, state);

-- Revision ledger: one row per stored Document revision. Owner: storage writer.
-- Compaction rewrites file_id in the same transaction that supersedes the old files.
CREATE TABLE IF NOT EXISTS doc_revisions (
    doc_id        TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    content_hash  TEXT NOT NULL,
    raw_blob_hash TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    source_id     TEXT NOT NULL,
    content_kind  TEXT NOT NULL,
    file_id       TEXT NOT NULL REFERENCES table_files(file_id),
    published_at  TEXT,
    ingested_at   TEXT NOT NULL,
    PRIMARY KEY (doc_id, revision)
) STRICT;
CREATE INDEX IF NOT EXISTS doc_revisions_hash ON doc_revisions (content_hash);
CREATE INDEX IF NOT EXISTS doc_revisions_file ON doc_revisions (file_id);

-- Stage events (StageEvent v1). Owner: every stage, append-only.
-- Raw rows kept stage_event_retention_days, then rolled up.
CREATE TABLE IF NOT EXISTS stage_events (
    event_id         TEXT PRIMARY KEY,      -- ULID
    stage            TEXT NOT NULL,
    status           TEXT NOT NULL,
    url_id           TEXT,
    doc_id           TEXT,
    revision         INTEGER,
    feed_id          TEXT,
    worker_id        TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    enqueued_at      TEXT,
    started_at       TEXT NOT NULL,
    finished_at      TEXT NOT NULL,
    error_code       TEXT,
    detail           TEXT,
    pipeline_version TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS stage_events_finished ON stage_events (stage, finished_at);
CREATE INDEX IF NOT EXISTS stage_events_url ON stage_events (url_id);
