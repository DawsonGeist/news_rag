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
CONTACT_EMAIL = "dawsonnewsrag@gmail.com"  # dedicated crawler mailbox, not personal
# A bare product token: some WAFs (NPR's, for one) stall User-Agents that contain an
# email or URL. The operator's contact goes in the From header instead (contact_email).
USER_AGENT = "newsrag/0.1"
# The product token robots.txt groups are matched against.
ROBOTS_AGENT = "newsrag"


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
    claim_batch: int = 200
    max_redirects: int = 5
    robots_ttl_s: int = 86400
    retry_base_s: float = 60.0  # retry n waits retry_base_s * 4**(n-1), +-20% jitter
    max_retry_after_s: int = 21600  # cap on a server's Retry-After
    reservation_slack_s: float = 30.0  # host/feed reservation = 2 x timeout + this
    # A host answering 403 repeatedly has blocked us: back off the whole host instead
    # of trying its URLs one by one (see politeness.blocked_backoff_s).
    block_streak_threshold: int = 2
    block_backoff_start_s: float = 3600.0
    block_backoff_max_s: float = 86400.0


class PollSettings(_Section):
    min_interval_s: int = 300
    default_interval_s: int = 1800
    max_interval_s: int = 21600
    pause_after_failures: int = 10
    concurrency: int = 16  # feeds polled at once (still one request per host)


def _default_extract_workers() -> int:
    # 50k docs/day is under 1 doc/s and trafilatura takes ~50-200 ms per page, so a few
    # workers clear even burst backlogs. Capping at 8 leaves the rest of the 20 threads
    # for the fetcher, poller and the Phase 2 GPU worker's tokenization.
    return max(1, min(8, (os.cpu_count() or 2) - 4))


class WriterSettings(_Section):
    flush_rows: int = 500
    flush_seconds: float = 60.0
    extract_workers: int = Field(default_factory=_default_extract_workers)
    extract_timeout_s: float = 120.0  # a page taking longer hangs a worker; pool is rebuilt
    extract_max_attempts: int = 3  # for transient failures (timeouts, crashed workers)
    compact_interval_s: float = 3600.0  # writer compacts hot parts at most this often
    compact_target_mb: int = 128  # target size of monthly parts


class VetSettings(_Section):
    """Feed vetting (phase1-design.md, component 1). Thresholds are tuned after a run."""

    concurrency: int = 64  # publishers vetted at once (still one request per host)
    feed_attempts: int = 3  # tries before a feed counts as unreachable
    samples_per_feed: int = 3
    samples_per_source: int = 5
    stale_days: int = 30
    reject_paywall_ratio: float = 0.5  # reject at or above
    reject_english_ratio: float = 0.5  # reject below
    review_full_text_ratio: float = 0.67  # review below
    review_date_ratio: float = 0.5  # review below
    review_median_words: int = 150  # review below


class EnricherSettings(_Section):
    """The Phase 2 enricher (phase2-design.md, "Process model")."""

    batch_docs: int = 256  # revisions per micro-batch: ~1,200 passages, ~10 s on the GPU
    poll_s: float = 5.0  # idle wait between looks for new revisions
    lease_s: float = 900.0
    max_attempts: int = 3
    device: str = "cuda"
    embed_batch: int = 64
    qdrant_url: str = "http://127.0.0.1:6333"
    qdrant_wait_s: float = 120.0  # at startup, e.g. after a reboot while Docker starts
    compact_interval_s: float = 3600.0
    compact_target_mb: int = 128
    # The entity queue (needs an indexed KB snapshot). Off until the linker's weights are
    # fitted on the linking test set, so the archive isn't backfilled twice.
    entities: bool = False
    entity_batch_docs: int = 64


class SupervisorSettings(_Section):
    # `newsrag run all` children. The enricher needs the GPU stack and Qdrant.
    components: list[str] = Field(
        default_factory=lambda: ["poller", "fetcher", "writer", "enricher"]
    )


class RetentionSettings(_Section):
    raw_days: int = 60
    stage_event_days: int = 30
    poll_log_days: int = 14


class Settings(_Section):
    data_dir: Path = Field(default=DEFAULT_DATA_DIR, validate_default=True)
    user_agent: str = USER_AGENT
    # Sent as the HTTP From header (RFC 9110 10.1.2), the standard place for a crawler
    # operator's address; some WAFs reject User-Agents that contain one.
    contact_email: str = CONTACT_EMAIL
    fetch: FetchSettings = FetchSettings()
    poll: PollSettings = PollSettings()
    writer: WriterSettings = WriterSettings()
    retention: RetentionSettings = RetentionSettings()
    vet: VetSettings = VetSettings()
    enricher: EnricherSettings = EnricherSettings()
    supervisor: SupervisorSettings = SupervisorSettings()

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
