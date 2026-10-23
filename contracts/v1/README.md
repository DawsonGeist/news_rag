# Contracts v1

These are the rules every component follows so that any component can be rebuilt, replaced or moved to another machine without touching the others. Where this file and [docs/overview.md](../../docs/overview.md) disagree, this file wins.

## Where things live

| Artifact | Location | Notes |
| --- | --- | --- |
| Record models (source of truth) | [src/newsrag/contracts/v1/](../../src/newsrag/contracts/v1/) | Pydantic v2. Validation rules are code, not prose. |
| JSON Schemas | [schemas/](schemas/) | Generated with `newsrag contracts export`; a test fails if they are stale. Use them from non-Python components. |
| Crawl-state DDL | [src/newsrag/contracts/v1/sql/crawl_state.sql](../../src/newsrag/contracts/v1/sql/crawl_state.sql) | SQLite. Every table names its single owning component. |
| Enrichment-state DDL (provisional) | [src/newsrag/contracts/v1/sql/enrichment_state.sql](../../src/newsrag/contracts/v1/sql/enrichment_state.sql) | Phase 2: `enrichments` (enricher progress) and `link_overrides` (final manual link corrections). Applied after the crawl state on every start. |
| ID and hashing rules | [src/newsrag/ids.py](../../src/newsrag/ids.py) | Pinned by `tests/test_ids.py`. |

## Stability

- **Stable:** `Document`, `StageEvent`, `FeedCandidate`, `FeedHealthReport`, the crawl-state DDL and `ids.py`. A minor change (`schema_version` 1.0 → 1.1) may only add optional fields or new enum values. Anything else is a breaking change and goes in `contracts/v2/`, with the old version kept readable.
- **Provisional:** `Segment`, `Embedding`, `Mention`, `Entity`, `MentionLink`, `EntityRelation`, `Snippet` and `DocRelation`, plus the enrichment-state DDL. They can change freely until the component that produces them ships. At that point they become stable.

Every record carries `schema_version`. Every derived record also carries `pipeline_version`, plus the `model_id`, `chunker_version` or `linker_version` that produced it.

## Identifiers

All IDs are deterministic, so retries and re-runs are idempotent.

| ID | Rule | Shape |
| --- | --- | --- |
| `url_id` | `hash32(url_key(discovered_url))`. Assigned when a feed surfaces a URL, before any fetch. | 32 hex |
| `doc_id` | `hash32(url_key(canonical_url))`. Assigned after fetching, once the canonical URL is known. | 32 hex |
| `feed_id` | `hash32(url_key(feed_url))` | 32 hex |
| `segment_id`, `mention_id`, `snippet_id` | `hash32` of the producing inputs, including the version of the code that made them (see each model's docstring) | 32 hex |
| `relation_id` | `hash32(src:relation:dst:source:valid_from)`. The same edge from different sources stays separate. | 32 hex |
| `override_id` | `hash32(kind:surface:doc_id:entity_id)` | 32 hex |
| `event_id`, `file_id` (a Parquet part in `table_files`) | ULID | 26 chars |
| `content_hash` | `sha256(title + "\n\n" + extracted_text)`. A headline change counts as a new revision. | 64 hex |
| `raw_blob_hash` | `sha256` of the raw response body bytes | 64 hex |
| `source_id` | Registered domain (eTLD+1) of the **listed publisher**, not of the feed host. A podcast hosted on simplecast still belongs to `nytimes.com`. | text |
| `entity_id` | `wd:<QID>` when linked to Wikidata, otherwise `local:<hash32>` | text |

`hash32(s)` is the first 32 hex characters of `sha256(s.encode("utf-8"))`.

**URL normalization** (`normalize_url`, canonicalizer version `url-v1`):

1. Lowercase the scheme and host, and IDNA-encode the host.
2. Drop the userinfo, the default port and the fragment.
3. Percent-encode any unsafe characters. Decode escapes of unreserved characters and uppercase all other escapes.
4. Remove dot segments.
5. Drop tracking query parameters (a conservative, frozen list) and sort the remaining parameters.

The result is still a real, fetchable URL.

**URL key** (`url_key`): the normalized URL with the scheme forced to `https`, a leading `www.` removed, and a trailing `/` removed except on the root. It exists only to be hashed, so it may not be fetchable.

**Canonical URL** (`choose_canonical`): `rel=canonical` is used only when all of these hold:

- It's absolute http(s).
- It's on the same registered domain as the final URL.
- It isn't the site root.

Otherwise the final URL after redirects is used. Cross-site canonicals (a local paper pointing at AP) are ignored on purpose, because who published something is part of the data.

One document can be reached through several `url_id`s: tracking variants, AMP pages, redirects. The frontier records the `url_id → doc_id` mapping for each one.

## Revisions and duplicates

- A `(doc_id, revision)` pair is immutable once stored.
- The same `doc_id` with a new `content_hash` gets `revision = max + 1`.
- The same `doc_id` with an already-stored `content_hash` is a no-op. The frontier row is marked stored and points at the existing revision.
- Different `doc_id`s with the same `content_hash` are both kept and linked later with `DocRelation(dup_of)`. They are never collapsed.
- **Re-fetch policy (v1):** an already-stored URL is re-queued only in two cases:
  - a feed reports a newer `updated` timestamp or a changed title for it, or
  - an operator asks for it.
  There are no blind scheduled re-crawls.

## Text and locators

`Document.extracted_text` is the anchor for every text locator. These rules are enforced when a Document is constructed:

- NFC-normalized, with no `\r`.
- Paragraphs are separated by exactly `\n\n`. No other newlines appear.
- No leading or trailing whitespace.
- Never modified after it's stored. Changed text is a new revision.

All locators use half-open `[start, end)` ranges:

- **`TextLocator`:** `field` is `body` (indexes `extracted_text`) or `title` (indexes `title`). Offsets count **Unicode code points**, which are Python `str` indices. JavaScript strings index UTF-16, so clients must convert.
- **`TimeLocator`:** milliseconds from the start of the media.
- **`PageLocator`:** a 1-based page number plus code-point offsets into that page's text.

A derived record always stores the `(doc_id, revision)` its locator refers to.

## Time

- Everything is timezone-aware UTC, and naive datetimes are rejected.
- In SQLite, timestamps are stored as `YYYY-MM-DDTHH:MM:SS.ffffffZ` (see `newsrag.timeutil`).
- `published_at` is nullable and never back-filled. `published_at_source` records where it came from (`json_ld` > `meta_tag` > `feed` > `url_path` > `page_text`).
- `discovered_at <= fetched_at <= ingested_at` always holds.

## Content kinds

| `content_kind` | Meaning | Empty text allowed |
| --- | --- | --- |
| `full_text` | The article body was extracted | no |
| `summary_only` | Only a teaser or feed summary was available | no |
| `suspected_paywall` | Paywall markers were found or the text looks truncated | yes |
| `non_article` | A video page, gallery, live-blog shell or index page | yes |

Every fetched page gets a Document, including paywalled and non-article pages. Filtering happens downstream, so a bad heuristic never loses data.

## Durability protocol

0. **One writer per table group.** Table files belong to groups (`storage.tables.TABLE_GROUPS`): `documents` is written by the storage writer, and `enrichment` (segments, mentions, mention links, snippets, entities, entity relations) by the enricher. Each group has exactly one writer process, holding the group's lock in `state/locks/`. Only that writer installs, compacts or removes files under its tables' `tables/<table>/` directories, and its startup cleanup never looks outside them, because another group's writer may be mid-install.
1. **Stage a file:** write it into the process's own staging area under `tmp/` and fsync it. Each area is `tmp/<owner>-<pid>-<ulid>/`, guarded by an flock on `<name>.lock`, so one process's cleanup never deletes another live process's files.
2. **Commit:** open a single SQLite write transaction (`BEGIN IMMEDIATE`; WAL, `synchronous=FULL`). Inside it:
   - `os.replace` the file into place and fsync its directory, plus any new parent directories
   - record the manifest rows (`blobs`, `table_files`, `doc_revisions`)
   - advance the `frontier` state

   Moving the file in under the write lock serializes it with retention's unlinks. A crash after the rename but before `COMMIT` leaves an orphan file, and orphans don't exist by rule 3.
3. **The manifest decides what exists:** a file not listed in the manifest does not exist. Cleanup:
   - **On startup:** delete the tmp areas whose lock is free (their process is dead). The storage writer also deletes table files with no `committed` row. Each component reclaims frontier rows in the states it owns. The fetcher reclaims `fetching` rows whose lease expired, at startup and every minute, because several fetchers may run. The writer reclaims **every** `extracting` row at startup, expired or not: it holds the single-writer lock, so any such row belongs to a dead predecessor.
   - **The enricher** at startup deletes `enrichment` table files with no `committed` row and hands every `running` row of `enrichments` back to `pending` (or `failed` once it has used `enricher.max_attempts`).
   - **Daily, with retention:** delete blob files whose row is `expired` or missing. This walks all of `raw/`, so it doesn't run at startup.
4. **Retention:** mark the row expired in a transaction first. Then unlink the file inside a write transaction that re-checks that the row is still not `present`.

5. **Stores outside the manifest** (the vector store, later the graph store) hold derived data only, and every write to them must be **idempotent**: keyed by IDs derived from content (a point's ID is its `segment_id`), so re-running a batch overwrites rather than duplicates. Write them *before* the transaction that marks the work done, and record in SQLite what each one must hold (`enrichments.vectors`). SQLite is the oracle: at startup the enricher compares the store's point count with the `done` rows, and on a mismatch reconciles, re-queueing revisions with missing points and deleting points of revisions that aren't `done`. Only a revision's latest version is kept in the vector store.

Never put the SQLite database or the Parquet tables on a network mount. Other machines reach the data through an API, or through copies of immutable segment files.

## Stage events

Every stage execution appends one `StageEvent`. The lag metrics are:

- **discovery lag:** `discover.finished_at − published_at`
- **ingest lag:** `searchable − discover.finished_at`
- **end-to-end lag:** `searchable − published_at`

"Searchable" means `store.finished_at` in Phase 1 and `index.finished_at` from Phase 2 on. The enricher's `segment` event has `enqueued_at` = the revision's `ingested_at`, so its **index lag** (stored → searchable, G2.3) is `index.finished_at − segment.enqueued_at`; `report lag` computes it from `enrichments` for revisions stored inside the window. Report p50, p95 and p99 per source and per stage.

## Query API

Not defined yet. It's the first deliverable of Phase 3 and will be added here as `QueryRequest` and `QueryResponse` before any query-service code is written.
