# newsrag

A local-first search engine over news and long-form content. It finds articles by the **opinion they express**, not just their topic. A query like "Trump is a good guy for energy" returns the articles and exact lines taking that stance, with opposing views shown separately, plus filters for entity and date. A second mode explores an entity map: who appears with whom, which publishers cover whom, and what was said about someone over time.

## Read first

- [docs/overview.md](docs/overview.md): the vision and the 13 components. Some details there are superseded, and the contracts win.
- [contracts/v1/README.md](contracts/v1/README.md): **normative** rules for IDs, revisions, text and locators, time, and durability.
- [docs/phase1-design.md](docs/phase1-design.md): the current phase, its build order and Gate 1.
- [docs/decisions.md](docs/decisions.md): what has been decided and why. Add an entry whenever you make a decision.

## Status

Phase 1 (vet, poll, fetch, extract, store). P1.0 is done: skeleton, contracts, state DDL and feed candidates. Next is P1.1: the blob store, table-file writer and recovery.

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
  obs/                 component 13: stage events, lag reports
  cli.py               `newsrag` entry point; one subcommand per process
feeds/                 feed lists (see feeds/README.md)
tests/                 pytest; fixtures/synthetic committed, fixtures/golden local-only
docs/                  overview, phase designs, setup, decision log
data/                  runtime data (gitignored; $NEWSRAG_DATA_DIR)
```

## Commands

```bash
pip install -e ".[dev]"            # in a venv; Python >= 3.12
pytest                             # all tests
ruff check src tests && ruff format src tests
mypy src                           # strict
newsrag contracts export           # regenerate JSON Schemas after changing a model
newsrag feeds candidates           # regenerate feeds/candidates_us.yaml
newsrag db init                    # create the crawl-state DB
```

On the Windows machine, use `.venv\Scripts\python -m pytest` or similar. The long-running pipeline runs in WSL2 (see [docs/setup-wsl.md](docs/setup-wsl.md)).

## Rules every change follows

- **Contracts are the boundary between components.** A component may only read or write another component's data through a contract record or a table it is documented to read. Every SQLite table names its single owner.
- **Contract changes:** a change to a model in `src/newsrag/contracts/v1/` needs three follow-ups:
  - run `newsrag contracts export`
  - update `contracts/v1/README.md` if a rule changed
  - for a stable record, make only additive changes (bump the minor `SCHEMA_VERSION`). Anything else goes in `contracts/v2/`.
- **Never change `ids.py` output for existing inputs.** That changes stored IDs and needs v2.
- **UTC everywhere.** Use `timeutil.utcnow()`. Naive datetimes are bugs. `published_at` is never back-filled.
- **Durability:** write to `data/tmp/`, fsync, `os.replace`, fsync the directory, then commit the manifest rows and the frontier state in one SQLite transaction. A file not in the manifest does not exist. Only the storage writer touches `data/tables/`, `table_files` and `doc_revisions`.
- **Derived data is rebuildable.** Every derived row records `pipeline_version` plus its model, chunker or linker version.
- **Stage events:** every stage writes a `StageEvent`, and lag must stay measurable.
- **Politeness:** respect robots.txt, keep one request in flight per host, use an identifiable User-Agent, and never evade blocks or paywalls. Content must be open access.
- **Local-first:** every runtime model runs locally (RTX 2070 Max-Q, 8 GB VRAM; 16 GB RAM). Claude is a development tool here, not a runtime dependency.
- **Golden fixtures** from real publishers stay in `tests/fixtures/golden/` (gitignored). Committed fixtures must be synthetic.
- **Style:** type hints everywhere (mypy strict), pydantic for records, small pure functions where possible, and comments that explain *why*. Match the surrounding code.

## Working conventions

- One component per session. Use the drill-down brief in [docs/overview.md](docs/overview.md), plus this file.
- Build the walking skeleton before splitting work across machines.
- Use Claude to propose evaluation queries and silver labels. A human verifies them, because they are the ground truth.
