# Decision log

Newest first. Each entry records what was decided, why, and what would reopen it.

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
