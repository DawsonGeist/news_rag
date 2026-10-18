# Decision log

Newest first. Each entry records what was decided, why, and what would reopen it.

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

- Whether the GitHub repository will be public. This affects the user-agent URL and whether real-page golden fixtures may ever be committed. They're local-only for now.
- Whether to add a headless-browser fetcher for JavaScript-only sites. Not in Phase 1.
