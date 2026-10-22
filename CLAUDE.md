# newsrag

A local-first search engine over news and long-form content. It finds articles by the **opinion they express**, not just their topic. A query like "Trump is a good guy for energy" returns the articles and exact lines taking that stance, with opposing views shown separately, plus filters for entity and date. A second mode explores an entity map: who appears with whom, which publishers cover whom, and what was said about someone over time.

## Read first

- [docs/overview.md](docs/overview.md): the vision and the 13 components. Some details there are superseded, and the contracts win.
- [contracts/v1/README.md](contracts/v1/README.md): **normative** rules for IDs, revisions, text and locators, time, and durability.
- [docs/phase1-design.md](docs/phase1-design.md): Phase 1, its build order and Gate 1.
- [docs/phase2-design.md](docs/phase2-design.md): Phase 2 draft (segmenter, embedder, entity linking, Wikidata, stores, Gate 2).
- [docs/decisions.md](docs/decisions.md): what has been decided and why. Add an entry whenever you make a decision.
- [docs/vision.md](docs/vision.md): what the author wants, in priority order (opinion search, visual entity graph, timelines, notifications). Trace design choices back to it.
- [docs/ideas.md](docs/ideas.md): parked ideas (EDGAR pipeline, poll scheduling, …). Stick to the design plan; an idea moves into a phase design only when the author picks it up.

## Status

Phase 1 (vet, poll, fetch, extract, store) is **complete**: it runs as a systemd user service and passes all 7 Gate 1 criteria ([docs/gate1-review.md](docs/gate1-review.md)). Phase 2 (segment, embed, extract entities, link, evaluate) has an **approved design**, [docs/phase2-design.md](docs/phase2-design.md); build starts at P2.0. The graph store is Neo4j; the vector store is chosen by the P2.1 prototype.

## Layout

```
contracts/v1/          normative spec + generated JSON Schemas (do not hand-edit schemas/)
src/newsrag/
  contracts/v1/        pydantic models = contract source of truth; sql/crawl_state.sql
  ids.py               URL normalization, doc/url/feed IDs, hashes (frozen; tests pin values)
  timeutil.py          UTC helpers, SQLite timestamp format
  config.py            Settings (defaults in code, TOML + env overrides)
  feeds/               component 1: feed vetter
  crawl/               components 2-3: poller, fetcher
  extract/             component 4: extractor (pure functions, process pool)
  storage/             component 8: state DB, blob store, table files, recovery
  testing/             kill-test harness (shipped so `newsrag test kill` can run it)
  obs/                 component 13: stage events, lag reports
  cli.py               `newsrag` entry point; one subcommand per process
feeds/                 feed lists (see feeds/README.md)
tests/                 pytest; fixtures/synthetic committed, fixtures/golden local-only
docs/                  overview, phase designs, setup, decision log
```

Runtime data lives outside the repo, in `~/newsrag-data` by default (`$NEWSRAG_DATA_DIR` overrides it).

## Commands

```bash
pip install -e ".[dev]"            # in a venv; Python >= 3.12
pytest                             # all tests
ruff check src tests && ruff format src tests
mypy src                           # strict
newsrag contracts export           # regenerate JSON Schemas after changing a model
newsrag feeds candidates           # regenerate feeds/candidates_us.yaml
newsrag db init                    # create the crawl-state DB
newsrag storage verify [--deep]    # check storage invariants
newsrag storage reextract          # G1.6: re-extract blobs, compare content_hash
newsrag storage compact            # merge small Parquet parts (writer does this itself)
newsrag storage retention          # expire blobs, sweep orphans, prune poll log (daily job)
newsrag report lag --since 24h     # G1.3 lag percentiles, queue depth
newsrag report disk                # G1.7 sizes and 12-month forecast
newsrag test kill                  # full-pipeline SIGKILL test (Gate 1, G1.4); --storage-only is faster
newsrag run fetcher                # fetch pending frontier URLs until SIGTERM
newsrag run writer                 # extract + store fetched pages until SIGTERM
newsrag run poller                 # poll active feeds until SIGTERM
newsrag run all                    # supervise poller + fetcher + writer
newsrag feeds import               # load candidates_us.yaml into the DB (keeps statuses)
newsrag feeds set-status active <feed-url>...
newsrag feeds vet [--limit 50 --seed 1]   # vet feeds (resumable); --limit for a pilot
newsrag feeds report [--list review]       # verdict counts; the human review queue
# Manual feed decisions (set-status active|rejected|review) are final: never re-vet them.
newsrag feeds export               # accepted feeds -> feeds/vetted_us.yaml
newsrag fixtures capture <url>...  # save real pages as unverified golden cases (local only)
newsrag fixtures report            # G1.5 metrics over verified golden cases
```

Everything runs on one native Ubuntu desktop (see [docs/setup-linux.md](docs/setup-linux.md)). The code is POSIX-only (`fcntl`, directory fsync).

## Rules every change follows

- **Contracts are the boundary between components.** A component may only read or write another component's data through a contract record or a table it is documented to read. Every SQLite table names its single owner.
- **Contract changes:** a change to a model in `src/newsrag/contracts/v1/` needs three follow-ups:
  - run `newsrag contracts export`
  - update `contracts/v1/README.md` if a rule changed
  - for a stable record, make only additive changes (bump the minor `SCHEMA_VERSION`). Anything else goes in `contracts/v2/`.
- **Never change `ids.py` output for existing inputs.** That changes stored IDs and needs v2.
- **UTC everywhere.** Use `timeutil.utcnow()`. Naive datetimes are bugs. `published_at` is never back-filled.
- **Durability:** stage the file in your process's `TmpArea` and fsync it. Inside one `transaction(conn)`, `install` it (`os.replace`, then fsync the directory) and write the manifest rows and the frontier state. A file not in the manifest does not exist. Only the storage writer touches `tables/`, `table_files` and `doc_revisions`. Use `newsrag.storage.fsio` rather than hand-rolling any of this.
- **Derived data is rebuildable.** Every derived row records `pipeline_version` plus its model, chunker or linker version.
- **Stage events:** every stage writes a `StageEvent`, and lag must stay measurable.
- **Politeness:** respect robots.txt, keep one request in flight per host, use an identifiable User-Agent, and never evade blocks or paywalls. Content must be open access.
- **Local-first:** every runtime model runs locally (RTX 5060 Ti 16 GB VRAM, 32 GB RAM, 20 CPU threads). Claude is a development tool here, not a runtime dependency.
- **Golden fixtures** from real publishers stay in `tests/fixtures/golden/` (gitignored). Committed fixtures must be synthetic.
- **Style:** type hints everywhere (mypy strict), pydantic for records, small pure functions where possible, and comments that explain *why*. Match the surrounding code.

## Working conventions

- One component per session. Use the drill-down brief in [docs/overview.md](docs/overview.md), plus this file.
- Build the walking skeleton before splitting work across machines.
- Use Claude to propose evaluation queries and silver labels. A human verifies them, because they are the ground truth.
