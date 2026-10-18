"""Runtime settings.

Defaults live here. Overrides come from a TOML file (``$NEWSRAG_CONFIG``, else
``config/newsrag.toml`` in the repo if present; see ``config/newsrag.example.toml``),
then from ``$NEWSRAG_DATA_DIR`` for the data directory.

The data directory lives outside the repo (default ``~/newsrag-data``) so the
checkout can be moved, re-cloned or wiped without touching the archive.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

# src/newsrag/config.py -> repo root. Only meaningful for the editable dev install,
# which is the only supported install; it keeps dev commands independent of the cwd.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "newsrag.toml"
DEFAULT_DATA_DIR = Path("~/newsrag-data")
# The repo is private, so the UA carries a contact address webmasters can actually reach.
USER_AGENT = "newsrag/0.1 (+mailto:dawson.geist@gmail.com)"


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FetchSettings(_Section):
    global_concurrency: int = 64
    default_crawl_delay_s: float = 5.0
    max_crawl_delay_s: float = 60.0
    request_timeout_s: float = 30.0
    max_body_bytes: int = 5_000_000
    max_attempts: int = 5
    lease_s: int = 300


class PollSettings(_Section):
    min_interval_s: int = 300
    default_interval_s: int = 1800
    max_interval_s: int = 21600
    pause_after_failures: int = 10


def _default_extract_workers() -> int:
    # 50k docs/day is under 1 doc/s and trafilatura takes ~50-200 ms per page, so a few
    # workers clear even burst backlogs. Capping at 8 leaves the rest of the 20 threads
    # for the fetcher, poller and the Phase 2 GPU worker's tokenization.
    return max(1, min(8, (os.cpu_count() or 2) - 4))


class WriterSettings(_Section):
    flush_rows: int = 500
    flush_seconds: float = 60.0
    extract_workers: int = Field(default_factory=_default_extract_workers)


class RetentionSettings(_Section):
    raw_days: int = 60
    stage_event_days: int = 30
    poll_log_days: int = 14


class Settings(_Section):
    data_dir: Path = Field(default=DEFAULT_DATA_DIR, validate_default=True)
    user_agent: str = USER_AGENT
    fetch: FetchSettings = FetchSettings()
    poll: PollSettings = PollSettings()
    writer: WriterSettings = WriterSettings()
    retention: RetentionSettings = RetentionSettings()

    @field_validator("data_dir")
    @classmethod
    def _absolute_data_dir(cls, v: Path) -> Path:
        # Relative paths would resolve against whatever cwd a process starts in, so
        # two components could silently use two different archives.
        return v.expanduser().resolve()

    @property
    def state_db(self) -> Path:
        return self.data_dir / "state" / "crawl.sqlite3"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def tables_dir(self) -> Path:
        return self.data_dir / "tables"

    @property
    def tmp_dir(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def locks_dir(self) -> Path:
        return self.data_dir / "state" / "locks"


def load_settings(path: Path | None = None) -> Settings:
    if path is None:
        env = os.environ.get("NEWSRAG_CONFIG")
        path = Path(env) if env else (DEFAULT_CONFIG_PATH if DEFAULT_CONFIG_PATH.exists() else None)
    data: dict[str, object] = {}
    if path is not None:
        with path.open("rb") as f:
            data = tomllib.load(f)
    if env_dir := os.environ.get("NEWSRAG_DATA_DIR"):
        data["data_dir"] = env_dir
    return Settings.model_validate(data)
