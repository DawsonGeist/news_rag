# Decision log

Newest first. Each entry records what was decided, why, and what would reopen it.

## 2026-10-23: P2.1 segmenter and the GPU stack

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Sentence splitter | pySBD (rule-based, English); it only chooses *cut points*, and sentences always cover the whole paragraph | Handled every news hard case (Sen., U.S., J.D., 3 p.m., Inc., curly quotes) at about 3k paragraphs/s per core. Using cuts, not its strings, prevents silent text loss (it dropped a trailing hashtag run on a real page). | Quality problems on a hand-checked sample |
| Passage packing | Whole paragraphs up to 200 words; paragraphs over 400 words split by sentence; no overlap (`seg-1`) | Archive: 4.8 passages/doc, median 174 words, so about 200k vectors/day: 38 GB/yr (512-d int8) to 75 GB/yr (1024-d) | Prototype recall favours overlap or another size |
| PyTorch | 2.14.1 from the `cu130` wheel index (Blackwell `sm_120`; driver 595 supports CUDA 13.2). ML libraries live in the optional `ml` extra. | Phase 1 installs stay lean; the CUDA build has to come from PyTorch's own index | — |
| Containers | Docker Engine from Docker's apt repository for Neo4j and Qdrant | Author's choice | — |

## 2026-10-23: P2.0 contracts and storage

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Table groups | Table files belong to groups (`documents`; `enrichment`), each with exactly one writer process and lock. Recovery, compaction and removal are scoped to the caller's group. | A second writer (the enricher) arrives in Phase 2. An unscoped startup sweep would delete another writer's part between its rename and its commit. | — |
| Derived-table verification | Hash plus row count from Parquet metadata. Only `documents` has a ledger (`doc_revisions`). | Derived tables are tracked through `enrichments`, not a per-row ledger | A derived table needs row-level checks |
| Enrichment DDL | A separate provisional file, `enrichment_state.sql`, applied after the crawl state with `CREATE IF NOT EXISTS` | Keeps the stable crawl-state DDL untouched; existing databases gain the tables on their next start | — |
| Entity edges | A new provisional record, `EntityRelation`, with source, confidence, validity dates, evidence and weight. Same-pair edges from different sources stay separate. | The graph needs scored, sourced edges for the confidence slider, timelines and provenance | — |

## 2026-10-22: Phase 2 design answers; extractor extract-3

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Retrieval unit | **Passages** (consecutive paragraphs packed to about 120–200 words); sentences located at query time | Author approved. News paragraphs average about 28 words, too short to embed alone; about 4× fewer vectors | The prototype shows paragraph-level recall clearly better |
| Gate 2 retrieval target | Set recall@50 **after the first measurement** | No baseline yet to pick a number from | — |
| Graph store | **Neo4j** (Community edition, run locally, e.g. Docker) | Author's choice: mature path queries (Cypher), tooling and visualization ecosystem; fits the entity graph's subgraph, path and expand needs. Crash-safe transactions. | Path queries or updates too slow at archive scale |
| Vector store | Still decided by the P2.1 prototype: LanceDB vs **Qdrant** vs **Neo4j's built-in vector index** (one less service if it's good enough) | Qdrant is a vector database, not a graph alternative, so the author's "unless Qdrant is superior" is evaluated here | — |
| Extractor `extract-3` | Prune `<template>`, `<dialog>`, `[role=dialog]` and `[aria-modal=true]` before extraction; Ghost's `post-upgrade-cta` / members-only gate is a paywall marker | Found by the G1.5 golden set (notus.org sign-in modal text in bodies; foodsafetynews.com previews read as full text) | — |

## 2026-10-22: Wikidata and entity linking (Phase 2 inputs)

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Wikidata's roles | (1) the alias index for candidate generation; (2) entity attributes, including positions with start/end dates, CIK and ticker; (3) relation edges for the graph (employer, owner, parent/subsidiary, member of, family, position held); (4) publisher entities matched by official website, bringing in owners and staff | Matches the vision: background entities and publisher staff come from knowledge-base relations, not news co-mentions | — |
| Wikidata source | A local subset streamed from the periodic dump (monthly refresh), recorded as a snapshot version on every link | Local-first; the public query service is rate-limited and remote | — |
| Subset scope | **1 hop**: entities of the types we care about (people, organizations, companies, places, legislation, events) with English labels, plus their direct relation neighbours | Author's choice; expand to 2 hops later if background discovery needs it | Graph exploration feels too shallow |
| Ambiguity | Candidate generation → scoring (popularity prior, type agreement, context match, coherence, time validity, locality, within-document coreference) → link above a high threshold; keep close runners-up as lower-confidence `MentionLink` edges; otherwise a `local:` entity. Manual link corrections are final (like feed decisions). Prototype ReFinED first. | Precision first (existing decision); links are scored edges, never merges | — |
| Linking cadence | Continuous micro-batches: the Phase 2 GPU worker processes newly stored Documents every minute or so (segment → NER → link) | Author wants the graph as current as possible; per-minute batches are close to per-article and are the shape notifications need | — |
| Graph confidence UI | High-confidence links by default, plus a **confidence slider**. The server returns the subgraph with per-edge confidence down to a floor (about 0.3); the browser filters on the slider without re-querying. Layout stays fixed while filtering; WebGL renderer. An edge's score combines both endpoints' link confidence with the relation's confidence. | Author's request; client-side filtering keeps it cheap | Subgraphs grow beyond about 10k elements |

## 2026-10-19: Planned network architecture (VPS edge), not built yet

The author plans to rent a VPS as the system's public edge. Build towards this; nothing below exists yet.

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Public edge | A VPS with a public IP runs the reverse proxy (e.g. Caddy: TLS, auth). The home server stays behind NAT with **no router ports opened**. | Keeps the home network closed while making the frontend and API reachable | — |
| Tunnel | WireGuard, dialled **out** from the home server to the VPS (UDP 51820, `PersistentKeepalive = 25`). Docker is optional, for packaging services at either end. Tailscale/Headscale is an acceptable managed alternative. | Outbound-only tunnel needs no port forwarding | — |
| Inbound traffic | VPS proxy → tunnel → home **query API** only (Phase 3). Never expose SQLite, Parquet or the data directory. Auth, and serving snippets plus links rather than full text, before anyone else uses it (see Users and Content policy above). | Matches overview.md: the frontend is on another machine and reaches storage only through the query API | — |
| Crawler egress | An application-level HTTP proxy on the VPS, bound to the WireGuard address only. newsrag gets a `fetch.proxy` setting used by `make_client`. Only crawler traffic uses it; personal browsing stays on the home line. If the tunnel is down, crawling must **fail loudly, never fall back to the home IP**. | Isolates the home IP's reputation from bot-protection blocks (see "Handling hosts that block the crawler"). A fixed VPS IP stays honest and identifiable, unlike a VPN. | — |
| Push discovery | Once the edge exists, consider WebSub for feeds that advertise a hub (callbacks arrive at the VPS and are forwarded through the tunnel) | Near-instant discovery without heavier polling | — |
| Bandwidth | The crawler downloads a few GB a day; check the VPS plan's metering (ingress is usually free) | Avoid surprise costs | — |

## 2026-10-18: Handling hosts that block the crawler

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Repeated 403s | Back off the whole host from the 2nd consecutive host-level failure that includes a 403: 1 h, doubling, capped at 24 h, reset by any normal answer. Shared by the fetcher, poller and vetter (`release_host`). Streaks reuse `hosts.consecutive_errors`, so no migration was needed. | A 403 is a refusal. Trying the host's other URLs every 5 s ignores it, is impolite, and risks getting the home IP flagged more widely (bhg.com began blocking mid-soak). | Legitimate per-page 403s cause needless host pauses |
| Chronic blockers | Daily supervisor job `feeds pause-blocked`: pause active feeds with at least 80% 403 among at least 10 article fetches in 7 days. `paused` is operational, so a human decides from `feeds report --list paused`. | Vetting samples too few articles to trip bot protection. 54 feeds met the rule after 10 h, including Industry Dive's whole network. | — |

## 2026-10-18: P1.7 soak setup

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Process manager | A systemd user service (`deploy/install-service.sh`) running `newsrag run all`, with lingering enabled | Survives logout and closing the session. systemd restarts the supervisor; the supervisor restarts components. `KillMode=mixed` lets the supervisor stop children gracefully. | Moving to a system service or containers |
| Gate 1 soak | Approved by the author: 72 h of continuous crawling of all accepted feeds | G1.2 and G1.3 need it | — |
| Operator contact address | `From: dawsonnewsrag@gmail.com`, a dedicated mailbox. Supersedes the personal address in the P1.2 entries below. | Keeps the crawler contactable without putting the author's personal inbox in thousands of server logs | — |
| Crawler network path | The author's direct connection, no VPN | Every request already identifies the operator (`From:` email, User-Agent), so a VPN adds no anonymity. (Correction: an early before/after comparison blamed the VPN for a 6× rise in 403s. That was confounded by host mix; the 403s come from a few hosts that block regardless of IP. The network errors in that window came from an unstable connection while the VPN was being set up.) | The home IP gets flagged by bot protection: move the crawler's egress to a dedicated VPS IP, not a VPN |

## 2026-10-18: P1.6 maintenance and reports

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Compaction commit | All months in one transaction | A part spanning a month boundary feeds two months; committing them separately risks duplicate rows after a crash | Compaction runs get long enough to stall the writer |
| Compaction trigger | In the writer, when idle and at most hourly | Never delays a flush; the writer already owns `tables/` | — |
| Stage-event rollup | Deferred. Raw rows are kept. | Gate 1 needs only a 72 h window; about 6M rows a month is fine for SQLite | `stage_events` queries slow down |
| Disk forecast | Last 7 days' rate. Blobs plateau at retention; text and state grow linearly. Marked rough under a day of data. | Simple and explainable. Fixed SQLite overhead dominates small samples. | Phase 2 adds vectors (must be added to the forecast) |
| Re-extraction check | Compares only Documents with the current `extractor_version`; expired blobs counted, not compared | A new version is expected to differ | — |
| Maintenance jobs | The supervisor runs one-shot jobs (retention daily, first after 10 min) | Same restart, logging and PDEATHSIG handling as components | — |

## 2026-10-18: Found by the first full vetting run

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| HTTP version | HTTP/1.1 only (was HTTP/2) | With one request per host there is nothing to multiplex. httpx's HTTP/2 stack raised `h2.exceptions.ProtocolError` (escaping httpx unwrapped) on about one publisher in five at 64-way concurrency. | — |
| Unexpected network exceptions | Any exception from the HTTP stack becomes a `network:<Type>` result | One odd server must cost one URL, never a publisher or a batch | — |
| Process pools | `procutil.owned_pool`: `spawn` workers with `PR_SET_PDEATHSIG(SIGKILL)` | Under Python 3.14's default `forkserver`, every SIGKILLed writer leaked its forkserver and workers (66 processes, about 9.5 GB, after the kill tests). The pipeline kill test now fails on any leaked process. | — |

## 2026-10-17: P1.5 vetter

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Full-content feeds | A feed summary of 150+ words that is at least as long as the page text is the article: `full_text`, with the summary as text (`extract-2`) | The pilot misread alternet.org (626-word items) as `summary_only` | — |
| Date-found ratio | Counts dates from any source, the feed included (`vet-2`) | The question is whether Documents will carry a date. Page-only dates sent darpa.mil and others to review needlessly. | G1.5 shows feed dates are unreliable |
| Bot-blocked sites | Rejected (`article_fetch_failed`, evidence `http_403` in the detail) | Policy: never evade blocks. 3 of 34 pilot publishers block our crawler. | — |
| Mirror feeds | `duplicate_feed` when the item URL set is identical to a shorter feed URL of the same publisher. Subsets are kept. | popsugar.com lists 4 feeds with identical items | — |
| Manual feed decisions | Final. `set-status active/rejected/review` appends a `feed_health` row with `vetter_version: "manual"`. The vetter never re-vets or overwrites such a feed, even with `--recheck` or a version bump, and a decision made during a run beats the run's verdict. `candidate` and `paused` lock nothing. | Author's choice: re-reviewing feeds by hand costs far more than re-vetting | — |
| Sampled pages | Not stored in the archive | Vetting must not depend on, or bloat, the archive | — |
| Polite access outside the frontier | `HostSlots` reserves hosts in the shared `hosts` table | Vetting can run alongside the pipeline without breaking one-request-per-host | — |

## 2026-10-17: P1.4 poller and supervisor

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Shared politeness | Robots, redirects and host reservation moved to `crawl.http`, used by the fetcher and the poller. `hosts` is owned by that module. | A feed poll and an article fetch to the same host must not overlap | — |
| Re-fetch trigger | Only the feed that first listed a URL can re-queue it | Titles differ between section feeds, so comparing across feeds would re-fetch endlessly | — |
| Feed dates | Read without feedparser's `updated`→`published` alias. Atom `<updated>`-only entries use it as the publication time. | The alias made every publication time look like an update | — |
| Writer startup | Reclaims **all** `extracting` rows, not just expired ones | The kill test found rows stranded when a writer restarted within the lease period | — |
| Supervisor | Restart with doubling backoff; `PR_SET_PDEATHSIG` ties children to it; tmp sweep at start and stop | Found by the kill test: orphans after a supervisor kill, and stale tmp areas after a late child kill | — |
| Reservation slack | `fetch.reservation_slack_s` (30 s), so the kill test can shrink it | A fixed 30 s made every killed poll stall the test | — |

## 2026-10-17: P1.3 extractor and writer loop

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| trafilatura mode | Default (balanced), not `favor_precision` | On a live CBS page, precision mode dropped the text of inline links ("During a , a man…"). Broken sentences hurt stance search more than a little boilerplate. | Golden set shows boilerplate hurting G1.5 |
| Paywall markers | Matched only in `class`/`id` attributes; no vendor names | NPR (free) was flagged because its Piano donation scripts matched `piano-` | Golden set shows missed paywalls |
| Index pages | `non_article` when there's no article JSON-LD and either the site root or a non-article `og:type` | The CBS homepage passed the 80-word test on headline text alone | — |
| Date-only values | Pinned to 12:00 UTC | Keeps every US publication time on that date within ±12 h. Naive timestamps are still discarded. | — |
| Language confidence | Language left unknown below 0.5 | py3langid labelled an English headline as `nl` at 0.04 | — |
| Charset order | HTTP header, then `<meta>`, then strict UTF-8, then detection | Detection garbles short pages | — |
| Extraction failures | Charged only to the page that caused them: start-reported timeouts, plus a single-worker quarantine pool for crash suspects | A crash fails every job in the pool. Charging all of them would eventually mark healthy pages `failed`. CPython 3.14 also often misses a dead worker while another is busy. | — |
| Golden expectations | Pre-filled by the extractor but `verified: false`. Only verified cases count toward G1.5, and an empty set fails. | A machine guess must never become ground truth | — |

## 2026-10-17: P1.2 fetcher

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| One request per host | The claim reserves the host by pushing `hosts.next_allowed_at` past the longest possible fetch | Works across several fetcher processes with no DDL change. A crashed fetcher's reservation lapses on its own. | Fetchers span machines (Postgres) |
| Redirects | Followed by hand, with a robots.txt check on every hop | httpx's automatic redirects would skip robots for the target host | — |
| robots.txt unavailable | Retry the URL with backoff, back off the host, and re-check at the next retry instead of caching for 24 h | RFC 9309 says to assume disallow while unavailable. A one-off outage shouldn't block a host for a day. | — |
| Missing Content-Type | Kept and fetched | Misconfigured servers would otherwise lose real articles. The extractor marks non-articles. | Junk blobs show up in practice |
| Operator contact | User-Agent `newsrag/0.1`, with `From: dawson.geist@gmail.com` (RFC 9110). Supersedes the mailto User-Agent below. | The smoke test found that NPR's firewall stalls any request whose User-Agent contains an email or URL, but accepts `newsrag/0.1` plus `From:`. Author's choice. | Many sites reject the bare token |

## 2026-10-17: Backend desktop and P1.1 storage

These entries supersede the Runtime, VRAM, and Python rows of "Phase 1 foundations" below.

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Backend machine | Native Ubuntu 26.04 desktop: MSI Aegis R2, Core Ultra 7 265 (20 threads), 32 GB RAM, RTX 5060 Ti 16 GB, 2 TB NVMe. WSL and Windows are dropped. | This is the final backend machine. Setup is in [setup-linux.md](setup-linux.md). | — |
| VRAM budget | 16 GB, less about 0.3 GB while the GPU drives the display | Hardware. Model options are in [phase2-models.md](phase2-models.md). | Phase 2 model selection |
| Raw HTML retention | Unchanged: rolling 60 days | Author's choice, even with 1.7 TB free | Re-extraction needs older blobs |
| Python | 3.12+ supported. The desktop runs 3.14. | Ubuntu 26.04 default | — |
| Data directory | `~/newsrag-data`, outside the repo. Relative paths are resolved to absolute paths at load time. | A re-clone or `git clean` can't touch the archive, and two processes can't end up on different archives because of different working directories | — |
| Extract workers | `min(8, cpu_count - 4)`, which is 8 on this machine | 50k docs/day is under 1 doc/s at 50 to 200 ms each. The remaining threads go to the fetcher, poller and Phase 2 tokenization. | The extract queue backs up in `report lag` |
| Repository visibility | Private | Author's choice | Going public: re-check the User-Agent URL and that no golden fixtures are in history |
| User-Agent contact | `newsrag/0.1 (+mailto:dawson.geist@gmail.com)` | The repo is private, so a repo URL would be a 404. Webmasters need a way to reach the operator. | Contact address changes |
| Stance quality vs latency | Quality first: a 14B-class LLM cascade is acceptable at about 3 to 6 s per query | Author's choice | Measured 14B gain over 8B is negligible on the labeled set |
| Vector retention | Vectors are kept for the full archive, with no rolling window | Author's choice: every stored article must be searchable by stance | Disk forecast fails. Shrink dimensions or quantization first; don't drop vectors. |
| Staging files | One `TmpArea` per process (`tmp/<owner>-<pid>-<ulid>/` plus an flock), not a shared `tmp/` emptied at startup | With several processes, emptying `tmp/` at startup deleted other live processes' in-flight files | — |
| Install inside the transaction | The `os.replace` into `raw/` or `tables/` happens inside the `BEGIN IMMEDIATE` transaction that writes the manifest row | It serializes blob installs with retention's unlinks through the SQLite write lock. Otherwise a re-fetched blob could end up `present` with its file deleted. | — |
| Blob orphan sweep | Daily with retention, not at startup | It walks every blob (about 3M at 60 days). Orphans are harmless because the manifest says they don't exist. | — |
| Single writer | Enforced with an flock on `state/locks/writer.lock` | Revision numbers are resolved outside the write transaction, which is only safe with one writer | — |
| `ingested_at` | Set by the writer at flush time, overriding the extractor's value | Measures the writer's real share of ingest lag. It's not part of `content_hash`. | — |

## 2026-10-17: Phase 1 foundations

| Decision | Choice | Why | Revisit if |
| --- | --- | --- | --- |
| Product or learning project | Product: use existing engines | Overview default; confirmed by the author | Author wants to hand-build HNSW or the linker |
| Users | Author first. Friends later, so assume multiple users eventually. | Confirmed | Before sharing: add auth, and serve only snippets plus links (see Content policy) |
| Content policy | Open-access content only. Paywalled feeds are rejected by the vetter. | Author's choice. Sharing with others later makes this matter. | Paywall handling is planned |
| Language | English only. Non-English documents are still stored but filtered downstream. | Confirmed. Storing costs little and protects against language-detection errors. | — |
| Geography | US feeds only: the `United States` section of the source list plus a curated supplement | Confirmed | — |
| Categories | Everything except "Other", including Local, Sports and Lifestyle | Confirmed. Breadth helps the entity map. | Volume or disk pressure |
| Podcasts | Kept as `medium=audio` candidates, not ingested in Phase 1 | Text pipeline only. Later reuses the YouTube/Whisper path. | Transcription pipeline starts |
| Raw HTML retention | Rolling 60 days (configurable from 30 to 90). Extracted text is kept forever. | 800 GB SSD; keeping raw HTML forever fills it in under a year at the expected volume | Bigger disk or desktop |
| Runtime | WSL2 Ubuntu now, native Linux desktop later. Windows is fine for tests only. | Crash-safety semantics, Docker and the GPU all fit Linux | — |
| Scale target | No cap ("more is better"). Designed for 50k articles a day on one machine. | Author's choice | Gate 1 disk forecast fails |
| Python and packaging | Python 3.12+, pip with `pyproject.toml` (`pip install -e ".[dev]"`) | Author uses pip. 3.12 is the WSL default, and the Windows machine has 3.14. | — |
| Contracts format | Pydantic models are the source of truth. JSON Schema is generated and committed. SQLite DDL is a packaged resource. | Validation lives in one place; the JSON Schema serves non-Python components | — |
| Phase 1 table storage | Parquet parts listed in a SQLite manifest, read with DuckDB | Simplest crash-safe option for text tables. Raw blobs are the source of truth, so changing it is cheap. | Phase 2 Parquet vs LanceDB prototype (vectors) |
| Work queue | The `frontier` table with leases, no separate broker | One machine; crash-safe; inspectable with SQL | Fetchers span machines (move to Postgres) |
| `doc_id` | Hash of the URL key of the canonical URL (https, no `www.`, no trailing slash, no tracking params) | Collapses http/https and www variants before hashing | — |
| Storage file naming | `table_files` and `file_id`, not "segments" | Avoids a clash with the `Segment` record (paragraphs and sentences) | — |
| VRAM | 8 GB (RTX 2070 Max-Q) | Author freed VRAM by closing other projects | Phase 2 model selection |

## Open


- Golden fixtures stay local-only even though the repo is private, since the repo may become public again and git history keeps everything.
- Whether to add a headless-browser fetcher for JavaScript-only sites. Not in Phase 1.
