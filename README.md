# newsrag

A local-first search engine over news and long-form content that finds articles by the opinion they express, not just their topic.

**Status:** Phase 1 (the ingestion archive) is in progress. See [docs/phase1-design.md](docs/phase1-design.md).

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest
```

To set up the backend on WSL2 or Linux, see [docs/setup-wsl.md](docs/setup-wsl.md). Architecture and conventions are in [CLAUDE.md](CLAUDE.md), [docs/overview.md](docs/overview.md) and [contracts/v1/README.md](contracts/v1/README.md).
