# Phase 1 design: vet, poll, fetch, extract, store

Status: draft for review · 2026-10-17

Phase 1 builds an archive. It continuously discovers US news articles from vetted RSS feeds, stores their raw HTML and an extracted Document, and survives power loss. It also measures ingest lag from the first day. Search is not part of Phase 1, but everything downstream (segmenting, embedding, NER) will be computed from this archive and can be recomputed.

Read [contracts/v1/README.md](../contracts/v1/README.md) first. This document assumes it.

## Scope

| # | Component | Phase 1 deliverable |
| --- | --- | --- |
| 1 | Feed vetter | Stage A (offline list extraction) is done. Stage B checks each feed; stage C samples articles from it. |
| 2 | Feed poller | Conditional GETs, adaptive intervals, frontier inserts |
| 3 | Article fetcher | Polite async fetch, robots.txt, raw blob store |
| 4 | Extractor | Blob to Document (text, date, language, content kind) |
| 8 | Storage writer | Parquet table files, manifest, recovery, compaction, retention |
| 13 | Observability (part) | `stage_events`, `newsrag report lag`, the kill test |

## Process model

```
             feeds (active)                 frontier                          table_files
 vetter ──▶ [SQLite] ──▶ poller ──▶ [pending] ──▶ fetcher ──▶ [fetched] ──▶ writer ──▶ Parquet
  (on demand)                         │                  │                     │ (pool of extractors)
                                      └── stage_events ◀─┴─────────────────────┘
```

- Each component is a long-running CLI process: `newsrag run poller`, `newsrag run fetcher` and `newsrag run writer`. `newsrag run all` supervises all three as child processes and restarts any that crash.
- **The work queue is the `frontier` table.** Workers claim rows with a lease (`lease_owner`, `lease_expires_at`) inside a short transaction. If a worker dies, its lease expires and another worker reclaims the row. Several fetcher processes can therefore share the queue, which is how fetching scales.
- The writer owns a `ProcessPoolExecutor` of extractors. Extraction is a pure function, `(blob bytes, frontier hints) → Document`, so it can later move anywhere. Every commit still goes through the single writer.
- SQLite is fine at this scale. At 50k articles a day, frontier writes are well under 10 per second. If Phase 1 ever needs fetchers on several machines, the crawl state moves to Postgres behind the same table contracts.

### Frontier state machine

```
pending ──claim──▶ fetching ──ok──▶ fetched ──claim──▶ extracting ──commit──▶ stored
   ▲                  │                                     │
   └──retry/backoff───┤                                     └──error──▶ fetched (retry) / failed
                      ├──robots/non-HTML/too large──▶ skipped
                      └──max attempts──▶ failed
stored ──feed reports update──▶ pending (low priority re-fetch)
```

- Retries back off exponentially: 1 min × 4^attempt with ±20% jitter, up to `fetch.max_attempts` (5).
- `429` and `503` honour `Retry-After` and also push back the host's `backoff_until`.
- `404` and `410` fail immediately. Other `4xx` responses get one retry.

## Data layout

```
~/newsrag-data/               ($NEWSRAG_DATA_DIR; outside the repo, local disk only)
  state/crawl.sqlite3         crawl state + manifest (WAL)
  raw/ab/cd/<sha256>.html.zst raw bodies, zstd level 9, rolling retention (default 60 days)
  tables/documents/
    hot/part-<ulid>.parquet   small parts written every flush
    2026/10/part-<ulid>.parquet  compacted monthly parts
  tmp/<owner>-<pid>-<ulid>/   one staging area per process, guarded by <name>.lock (flock)
  state/locks/writer.lock     held by the single storage writer
  logs/
```

- Readers must list files from `table_files WHERE state='committed'`, never by globbing the directory.
- DuckDB reads them with `read_parquet([...])`.

## Component designs

### 1. Feed vetter

**Stage A (done):** `newsrag feeds candidates` reads `feeds/source/feeds_world.yaml` and `feeds/supplement_us.yaml` and writes [feeds/candidates_us.yaml](../feeds/candidates_us.yaml). The current run produced:

- 10,839 candidates, of which 9,069 are text feeds and 1,770 are audio feeds (podcasts, kept for a future transcription pipeline).
- 120 non-English domains dropped.
- 91 feeds dropped for being in the "Other" category.

`newsrag feeds vet` works one publisher at a time, up to `vet.concurrency` (64) publishers at once. All requests go through `crawl.http` with `HostSlots`, so a vetting run stays polite even while the pipeline runs: one request per host, crawl delay, and robots.txt on every hop. Audio candidates get a `reject`/`audio_feed` verdict without any traffic, because G1.1 wants every candidate to hold a verdict. `--limit N --seed S` vets a random, reproducible pilot of whole publishers.

**Stage B, the feed check (text feeds only):** fetch the feed (up to 3 tries for network errors, 429 and 5xx, with backoff) and parse it with the poller's parser. It records:

- format and item count
- the share of items with dates, and the newest item's date
- an estimate of items per day
- the share of items with audio enclosures
- whether robots.txt allows the feed

**Stage C, the article sample:** sample the newest items, round-robin across the publisher's feeds: up to 3 per feed and at most 5 per publisher. Run the real extractor on them in a process pool. A feed whose own items weren't sampled is judged on the publisher's samples (same site, same CMS). Sampled pages aren't stored. Stage C records:

- the share of articles yielding full text, and the share showing paywall markers
- the share in English, and the share with a publication date from any source, the feed included (will this feed's Documents carry a date?)
- the median word count

**Verdict rules** (thresholds go in config and are tuned after the first run):

| Verdict | Condition |
| --- | --- |
| reject | Unreachable after 3 tries, unparseable, empty, stale (newest item over 30 days old), robots disallows the feed or every sampled article, every sample fetch failed (`article_fetch_failed`, usually HTTP 403 bot blocking), `paywall_ratio ≥ 0.5`, `english_ratio < 0.5`, audio, or `duplicate_feed` (exactly the same item URLs as a shorter feed URL of the same publisher) |
| review | `full_text_ratio < 0.67` (labelled by the most common non-full kind: `summary_only`, `paywalled` or `low_word_count`), `published_at_found_ratio < 0.5`, or `median_word_count < 150` when not already explained by the full-text ratio |
| accept | Everything else |

- Each publisher's reports commit together: a `feed_health` row per feed, and `feeds.status` set from the verdict (`active`, `review`, `rejected`). `status_reason` holds the issues, and the report's `detail` holds the evidence: HTTP statuses, sample kinds, sample errors.
- `newsrag feeds report --list review` prints the human review queue. A human decision (`newsrag feeds set-status active|rejected|review <url> --reason ...`) is final: it is recorded as a `manual` health row, and the vetter never touches that feed again. Rejected feeds can be reviewed later with `--list rejected`.
- `newsrag feeds export` writes the accepted list to `feeds/vetted_us.yaml` (committed) so another machine can reproduce the set.
- Runs resume: feeds already vetted by the current `VETTER_VERSION` are skipped. Bumping the version re-vets everything; `--recheck` does it by hand.
- **Pilot (2026-10-17, 50 feeds, 34 publishers, about 1 min):** 27 accept, 6 review, 17 reject (3 audio, 3 duplicate mirrors, 3 bot-blocked, 3 stale, 2 paywalled, 2 robots, 1 HTTP 403). It found and fixed two rule bugs: full-content feeds misread as `summary_only` (extractor `extract-2`), and `no_dates` ignoring feed dates (`vet-2`).

### 2. Feed poller

- Single asyncio process (`newsrag run poller`) that polls feeds where `status='active' AND next_poll_at <= now`, up to `poll.concurrency` (16) at once.
- **Claiming:** at most one feed per host. The claim reserves the host, as the fetcher does, and pushes the feed's `next_poll_at` past the longest possible poll. A crashed poller's claim lapses on its own.
- **Conditional GET** with the stored `etag` and `last_modified`. A `304` counts as a successful poll with no new items.
- **Adaptive interval:** a poll that finds new items multiplies the interval by 0.7. A poll with nothing new multiplies it by 1.5. The result is clamped to [5 min, 6 h], and the first interval is 30 min. Each `next_poll_at` gets ±10% jitter so polls don't synchronize.
- Feed hosts use the same per-host politeness as articles, through `crawl.http`: robots.txt on every redirect hop, and one request per host shared with the fetcher.
- **Parsing:** feedparser. Links resolve against the feed URL, and FeedBurner's `origLink` is preferred over its tracking redirect. A response with no entries and no feed version (an HTML page) is `unparseable`. Item dates are read without feedparser's `updated`→`published` aliasing. An Atom entry with only `<updated>` uses it as the publication time. Dates more than a day in the future are dropped.
- **For each item:**
  - normalize the link
  - compute `url_id`
  - `INSERT ... ON CONFLICT DO NOTHING` into `frontier` with `state='pending'` and the feed's title, summary, published and updated hints
  - insert into `url_sightings`
  - write a `discover` stage event for new URLs only
- If an existing stored URL's feed `updated` timestamp moves forward or its title changes, the row is re-queued at low priority (`priority = -1`; the revision policy). Only **the feed that first listed the URL** counts. Section feeds often abbreviate headlines, and comparing titles across feeds would re-fetch the same article forever.
- After `pause_after_failures` (10) consecutive failures, the feed is set to `paused` with a reason. A 429 or 503 pushes the next poll past `Retry-After` and backs off the whole host.
- Each poll appends to `feed_polls`. Its feed state, frontier inserts, sightings, discover events and host release commit in one transaction.

### Supervisor

`newsrag run all` runs the poller, fetcher and writer as child processes:
- A child that exits for any reason other than shutdown is restarted. The delay doubles with each crash in the last 10 minutes, from 1 s up to 60 s.
- SIGTERM stops all children gracefully (90 s, so the writer can flush), then SIGKILLs any that remain.
- Children are tied to the supervisor with `PR_SET_PDEATHSIG`. If the supervisor dies, they die too, rather than running on as orphans beside a new supervisor.
- Logs go to `logs/<component>.log`. Starts, exits and restarts go to `logs/supervisor.log`, which is the G1.2 evidence.
- The supervisor sweeps dead processes' tmp areas when it starts and after it stops.

### 3. Article fetcher

- **Concurrency:** asyncio with httpx (HTTP/2 enabled). At most `global_concurrency` (64) requests in flight and **one per host**. When a fetch ends, the host's `next_allowed_at` is set to now + `crawl_delay_s`: robots.txt `Crawl-delay` if present, else 5 s, capped at 60 s.
- **Claiming work:** one transaction leases up to `claim_batch` (200) ready rows, **at most one per host**, from hosts whose `next_allowed_at` and `backoff_until` have passed. This stops one big publisher from starving the rest.
- **Host reservation:** the claim also pushes the host's `next_allowed_at` past the longest possible fetch (2 × request timeout + 30 s). No other claim, in this process or another fetcher process, can pick that host until the fetch ends and sets the real next time. A crashed fetcher's reservation simply lapses. This needs no extra columns or locks.
- **robots.txt:** parsed with protego against the product token `newsrag`, and cached in `hosts` for 24 h.
  - A `4xx` response means "allow".
  - A `5xx` or network error means "unavailable": the URL is retried with backoff, the whole host backs off, and the result is cached for only one retry interval. `robots_status = 0` records a network error.
  - A disallowed URL is set to `skipped` with `skip_reason='robots'`.
- **Request:**
  - Headers: the `User-Agent` and a `From:` header carrying the operator's email (RFC 9110). gzip, br and zstd are accepted.
  - Redirects are followed **by hand** (up to 5), so robots.txt is checked on every hop, including hops to other hosts.
  - Body limit of 5 MB, counted on decoded bytes, so a small gzip bomb is caught too.
  - Only `text/html` and `application/xhtml+xml` are kept. A missing `Content-Type` is kept too, and the extractor decides. Anything else becomes `skipped`.
- **Commit guard:** the result is committed only if the row is still `fetching` under this worker's lease. If the lease lapsed and another worker took the row, the result is dropped.
- **On success:**
  1. sha256 the body and stage it compressed in the process's tmp area (in a thread, off the event loop)
  2. in one transaction: `BlobStore.commit` (rename into `raw/` and upsert the `blobs` row with `expires_at = now + retention.raw_days`), set the frontier row to `fetched` (with `final_url`, `http_status`, `raw_blob_hash`), and write a `fetch` stage event. The rename happens under the write lock so that it serializes with retention's unlinks.
- No JavaScript rendering in Phase 1. The vetter flags sites that need it, because their full-text ratio comes out low.

### 4. Extractor (runs inside the writer's process pool)

The extractor takes the decompressed blob, the final URL and the frontier hints, and returns a Document or an error code.

1. **Decode:** the HTTP charset if it decodes cleanly, then the page's `<meta charset>`, then strict UTF-8, and statistical detection only as a last resort (it guesses badly on short pages).
2. **Parse:** run lxml once, then pull the `rel=canonical` link, the JSON-LD blocks (`NewsArticle` and similar, including `@graph`), the meta tags, and every `class`/`id` value.
3. **Canonical URL:** `ids.choose_canonical`, then `doc_id`.
4. **Body:** trafilatura `bare_extraction` in its default (balanced) mode with comments excluded. Precision mode was rejected because it drops the text of inline links and leaves broken sentences. The headline is removed from the first line when trafilatura repeats it, either alone or fused to the next block. The text is normalized to the contract: NFC, each paragraph stripped, paragraphs joined with `\n\n`, and the zero-width characters and NBSPs that some sites inject removed or replaced.
5. **Title:** JSON-LD `headline`, then `og:title`, then trafilatura's title, then `<title>`.
6. **`published_at`:** the first plausible value of JSON-LD `datePublished`, then the publication meta tags (`article:published_time` and similar), the feed item date, a URL path date (`/2026/10/17/`), and finally trafilatura's date guess (source `page_text`).
   - A timestamp without an offset is discarded rather than guessed. Feed dates are already UTC.
   - A date-only value is pinned to 12:00 UTC, which keeps every US publication time on that date within ±12 h.
   - Plausible means between 1995 and one day after the fetch.
   - `modified_at` comes from JSON-LD `dateModified` or the modified-time meta tags.
7. **Language:** py3langid on the title plus the first 2,000 characters. Below 50% confidence the language is left unknown, because a bare headline can come out as Dutch. Non-English documents are still stored, because downstream filters on `language='en'`.
8. **Content kind:**
   - `suspected_paywall` if JSON-LD `isAccessibleForFree` is false (on the article or one of its parts), or paywall markers appear in `class`/`id` attributes together with fewer than 250 words. Markers are never matched in scripts, and vendor names such as Piano are not markers, because NPR uses Piano for donation prompts.
   - `non_article` if there are fewer than 80 words and no article JSON-LD, or the page has no article JSON-LD and is the site root or declares an `og:type` other than `article` (homepages and section pages).
   - If the extracted text is no longer than the feed summary, the summary becomes `extracted_text`. The kind is `summary_only` if the summary is under 150 words (a teaser), and `full_text` otherwise: a full-content feed carries the whole article (`extract-2`).
   - `suspected_paywall` if an article page yields no text at all.
   - otherwise `full_text`
9. **Assemble:** `content_hash` and `word_count` are computed, and `extractor_version` is a hand-bumped string plus the trafilatura version.

Synthetic fixtures (`tests/fixtures/synthetic/articles/`, with `expected.yaml`) cover each heuristic. The extractor is deterministic, so G1.6 re-extraction reproduces `content_hash`. `ingested_at` is provisionally `fetched_at` until the writer sets it.

### 8. Storage writer

- **Extraction loop** (`newsrag run writer`): lease `fetched` rows (setting them to `extracting`), submit them to the pool of `extract_workers` processes, and buffer the resulting Documents.
- **Charging failures to the right page.** Extraction runs untrusted pages through C extensions, so a worker can hang or die.
  - Deterministic failures (`unparseable`, `blob_missing`) set the row to `failed`.
  - **Hangs:** each worker reports when it actually starts a job, because the executor queues calls ahead of its workers. A job running longer than `extract_timeout_s` (120 s) is charged a transient attempt. The pool is killed and its other jobs are handed back uncharged.
  - **Crashes:** a dead worker breaks the whole pool, and the culprit is unknown. Every job it held is handed back uncharged as a *suspect*, and suspects run one at a time in a single-worker quarantine pool, where a crash can only be the suspect's own. CPython 3.14 often misses a dead worker while another is busy; the hang rule then catches that job, still charged to the right page.
  - Transient failures go back to `fetched` with backoff, up to `extract_max_attempts` (3), then `failed`. Every failure writes an `extract` stage event.
- **Flush** when `flush_rows` (500) or `flush_seconds` (60) is reached. The 60 s flush bounds the writer's share of ingest lag.
  1. **Resolve revisions** for each Document against `doc_revisions` and the other Documents in the same batch:
     - known `content_hash` for that `doc_id`: no row is written, and the frontier row is set to stored with the existing revision
     - new `content_hash`: `revision = max + 1`
  2. **Write** the Parquet part (zstd, explicit Arrow schema derived from the Document model) to `tmp/`, fsync, rename into `hot/`, fsync the directory.
  3. **Commit** in one transaction: insert the `table_files` row (with sha256 and row count), insert the `doc_revisions` rows, set the frontier rows to `stored` (with `doc_id` and `revision`), and write the `extract` and `store` stage events.
- **Recovery on startup** (`recover_writer`, under the writer lock):
  - sweep the tmp areas of dead processes
  - delete table files that have no `committed` manifest row
  - reset `extracting` rows whose lease expired back to `fetched`. Extraction is deterministic, so redoing it is safe.

  The blob orphan sweep is not part of startup. It walks millions of files, so it runs with the daily retention job.
- **Compaction** (hourly; `newsrag run writer` does it between flushes):
  1. Merge closed `hot/` parts into monthly parts of about 128 MB.
  2. In one transaction, insert the new `table_files` row, mark the old rows `superseded`, and repoint `doc_revisions.file_id`.
  3. Delete the old files afterwards. If the process crashes before deleting them, startup cleanup removes them.
- **Raw retention** (daily):
  1. Select `blobs` where `state='present' AND expires_at < now` and mark them `expired` in a transaction.
  2. Unlink each file inside a write transaction that re-checks the row first, so a concurrent re-fetch that revived the blob wins.
  3. Sweep `raw/` for files whose row is missing or expired (`sweep_blobs`).

  Blobs referenced by golden fixtures have `expires_at = NULL`.

### 13. Observability (Phase 1 part)

- Every stage writes `StageEvent`s through one helper, `obs.events.record(...)`.
- `newsrag report lag [--since 24h] [--by source|stage]` prints discovery, ingest and end-to-end lag at p50, p95 and p99. It also prints the queue depth for each frontier state and the active, paused and rejected feed counts.
- `newsrag report disk` prints blob, Parquet and SQLite sizes, plus a fill forecast for the data volume.
- Raw stage events are kept 30 days, then rolled up into hourly percentiles per stage and source.

## Testing

- **Unit tests:** IDs, the contracts, date parsing, text normalization and the verdict rules.
- **Synthetic fixtures** (committed, `tests/fixtures/synthetic/`):
  - hand-written feed XML: RSS 2.0, Atom, a missing-dates case and an audio-enclosures case
  - article HTML: a full article, a paywall, a non-article page, a JSON-LD date, a meta-only date and a cross-site canonical
- **Golden fixtures** (local only, `tests/fixtures/golden/`, gitignored for copyright): about 40 real pages across 20 publishers, each with a hand-checked expected title, date, content kind and first and last paragraph. `newsrag fixtures capture <url>` politely fetches a page and pre-fills `case.yaml` from the extractor, marked `verified: false`. A human corrects it and sets `verified: true`. `newsrag fixtures refill` re-fills unverified cases after an extractor fix, and `newsrag fixtures report` prints the G1.5 metrics over verified cases only.
- **Integration tests:** the full poll → fetch → extract → store path against a local HTTP server (`respx` or a `pytest` fixture server) serving the synthetic fixtures.
- **Kill test** (`newsrag test kill`): 20 iterations. Each serves two synthetic feeds and their articles from local HTTP servers on two loopback hosts, starts `newsrag run all`, and `SIGKILL`s a random process (a child or the supervisor) at random moments, 3 times by default. It waits for the pipeline to settle, stops it cleanly, and checks the invariants. `--storage-only` runs the faster storage-layer workload instead. The invariants are:
  - every `committed` file exists and matches its sha256
  - every `doc_revisions` row points at a committed file that contains it
  - there are no duplicate `(doc_id, revision)` pairs
  - every frontier URL eventually reaches `stored`, `skipped` or `failed`
  - no files are left in `tmp/`

## Gate 1: exit criteria for Phase 1

| # | Criterion | Measured by |
| --- | --- | --- |
| G1.1 | At least 1,000 accepted text feeds, with every candidate holding a verdict | `feed_health` |
| G1.2 | 72 hours of continuous unattended running with no manual intervention | supervisor log |
| G1.3 | Ingest lag p95 under 10 min and p99 under 30 min over the 72 h | `report lag` |
| G1.4 | The kill test passes 20 of 20 iterations, and one real power-pull test passes | `test kill` + manual |
| G1.5 | Golden set: date within ±24 h on 90% or more, content kind correct on 95% or more, first and last paragraph present on 90% or more of full-text pages | golden tests |
| G1.6 | Re-extracting the archive from blobs reproduces identical `content_hash` values for an unchanged `extractor_version` | `newsrag reextract --verify` |
| G1.7 | The disk forecast fits on the current SSD for 12 months at the observed rate | `report disk` |

Gate 1 also starts the labeling of the 30 opinion queries that Phase 2's retrieval evaluation needs.

Gates 2 and 3 are outlined here and get detailed in their own design docs:

- **Gate 2** (the retrieval evaluation passes): on the 30 labeled queries, recall@50 of supporting articles reaches the target set when the labels exist, and the index lag p95 is under 15 min.
- **Gate 3** (stance quality): precision@10 of supporting versus opposing results on the labeled set, and the opposing-views section is correct on most queries.

## Build order within Phase 1

1. **P1.0** (done): skeleton, contracts, state DDL, candidate extraction.
2. **P1.1** (done): blob store, table-file writer, recovery, plus their unit tests and kill-test harness. Everything else commits through this, so it comes first.
3. **P1.2** (done): fetcher with robots.txt and politeness, tested against mock transports and a local HTTP server.
4. **P1.3** (done): extractor, the writer's extraction loop, synthetic fixtures, and the golden-fixture tooling. Verifying the golden cases is a human task, ongoing until Gate 1.
5. **P1.4** (done): poller, supervisor (`run all`), feed import and status commands, and the full-pipeline kill test.
6. **P1.5:** vetter stages B and C, reusing the fetcher and extractor (built; pilot done). Run it on all 9,069 text candidates.
7. **P1.6:** lag and disk reports, compaction and retention jobs.
8. **P1.7:** the 72 h soak, the power-pull test and the Gate 1 review.

## Risks

- **Disk:** see G1.7. At 30k articles a day, raw blobs come to about 1.5–3 GB a day, which is about 90–180 GB at 60 days. Extracted text is about 0.1 GB a day.
- **Being blocked:** some publishers block non-browser clients. Phase 1 stays polite and identifiable and does not evade blocks. Feeds that block us are rejected.
- **JavaScript-only sites:** these get flagged by the vetter. A headless-browser fetcher is a later, optional component behind the same blob contract.
- **Extraction quality varies by CMS:** the golden set must cover the major CMS families (WordPress, Arc, Brightspot, custom).
