-- newsrag enrichment state (Phase 2), contract v1. PROVISIONAL until the enricher ships.
--
-- Same conventions as crawl_state.sql: UTC text timestamps (YYYY-MM-DDTHH:MM:SS.ffffffZ),
-- each table names its single owner, other components only read it. Created by
-- newsrag.storage.state.init_schema next to the crawl state; existing databases gain these
-- tables on their next start (CREATE ... IF NOT EXISTS).

-- Enrichment progress per Document revision and pipeline version. Owner: enricher.
-- Work discovery: doc_revisions rows with no 'done' row here for the current
-- pipeline_version. A new model or chunker is a new pipeline_version, so re-enrichment
-- is just more rows; old outputs stay until superseded. Leases make claims crash-safe,
-- as in the frontier.
CREATE TABLE IF NOT EXISTS enrichments (
    doc_id           TEXT NOT NULL,
    revision         INTEGER NOT NULL,
    pipeline_version TEXT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('pending', 'running', 'done', 'failed')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    lease_owner      TEXT,
    lease_expires_at TEXT,
    last_error       TEXT,
    queued_at        TEXT NOT NULL,     -- when the revision became known to the enricher
    started_at       TEXT,
    finished_at      TEXT,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (doc_id, revision, pipeline_version)
) STRICT;
CREATE INDEX IF NOT EXISTS enrichments_ready ON enrichments (pipeline_version, state, queued_at);
CREATE INDEX IF NOT EXISTS enrichments_lease ON enrichments (state, lease_expires_at);

-- Manual entity-link corrections. Owner: entity linker; rows are written only by a
-- human through the CLI. Like manual feed decisions they are FINAL: re-linking applies
-- them and never overwrites them.
--   kind 'force':  mentions with this surface (in doc_id, or everywhere if NULL) link to entity_id
--   kind 'forbid': mentions with this surface (in doc_id, or everywhere) never link to entity_id
CREATE TABLE IF NOT EXISTS link_overrides (
    override_id TEXT PRIMARY KEY,          -- hash32(kind:surface:doc_id:entity_id)
    kind        TEXT NOT NULL CHECK (kind IN ('force', 'forbid')),
    surface     TEXT NOT NULL,             -- normalized mention surface
    doc_id      TEXT,                      -- NULL = applies to every document
    entity_id   TEXT NOT NULL,             -- wd:<QID> or local:<hash32>
    reason      TEXT NOT NULL,
    created_at  TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS link_overrides_surface ON link_overrides (surface);
