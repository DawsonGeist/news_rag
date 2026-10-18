"""Runtime settings.

Defaults live here. Overrides come from a TOML file (``$NEWSRAG_CONFIG``, else
``config/newsrag.toml`` if present; see ``config/newsrag.example.toml``), then
from ``$NEWSRAG_DATA_DIR`` for the data directory.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

DEFAULT_CONFIG_PATH = Path("config/newsrag.toml")
USER_AGENT = "newsrag/0.1 (+https://github.com/DawsonGeist/news_rag)"


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


class WriterSettings(_Section):
    flush_rows: int = 500
    flush_seconds: float = 60.0
    extract_workers: int = Field(default_factory=lambda: max(1, (os.cpu_count() or 2) - 2))


class RetentionSettings(_Section):
    raw_days: int = 60
    stage_event_days: int = 30
    poll_log_days: int = 14


class Settings(_Section):
    data_dir: Path = Path("data")
    user_agent: str = USER_AGENT
    fetch: FetchSettings = FetchSettings()
    poll: PollSettings = PollSettings()
    writer: WriterSettings = WriterSettings()
    retention: RetentionSettings = RetentionSettings()

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
