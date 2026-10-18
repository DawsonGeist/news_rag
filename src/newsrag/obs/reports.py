"""``newsrag report lag`` and ``newsrag report disk`` (Gate 1: G1.3 and G1.7).

Lag definitions (contracts/v1/README.md, "Stage events"), per URL:

- discovery lag: first ``discover`` finished - ``published_at``
- ingest lag: first ``store`` finished - first ``discover`` finished
- end-to-end lag: first ``store`` finished - ``published_at``

"Searchable" is ``store.finished_at`` in Phase 1. A ``store`` with status ``skipped``
(unchanged content) counts: the content is searchable. Only URLs stored inside the
window are counted.
"""

from __future__ import annotations

import math
import shutil
import sqlite3
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from newsrag.config import Settings
from newsrag.timeutil import from_db, to_db

STAGES = ("discover", "fetch", "extract", "store")


def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest-rank percentile; None for no values."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


@dataclass
class Dist:
    n: int = 0
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None

    @classmethod
    def of(cls, values: Sequence[float]) -> Dist:
        return cls(
            len(values), percentile(values, 50), percentile(values, 95), percentile(values, 99)
        )


@dataclass
class LagGroup:
    discovery: Dist
    ingest: Dist
    end_to_end: Dist
    negative_discovery: int = 0  # published_at after discovery: misdated pages or feeds


@dataclass
class LagReport:
    since: datetime
    groups: dict[str, LagGroup]
    stages: dict[str, dict[str, Dist]] = field(default_factory=dict)  # stage -> queue/work
    stage_status: dict[str, dict[str, int]] = field(default_factory=dict)
    queue: dict[str, int] = field(default_factory=dict)
    oldest_pending_s: float | None = None
    feeds: dict[str, int] = field(default_factory=dict)


def lag_report(
    conn: sqlite3.Connection, now: datetime, since: timedelta, by_source: bool = False
) -> LagReport:
    start = now - since
    rows = conn.execute(
        "WITH d AS (SELECT url_id, min(finished_at) AS t FROM stage_events "
        "           WHERE stage = 'discover' AND status = 'ok' GROUP BY url_id), "
        "     s AS (SELECT url_id, min(finished_at) AS t FROM stage_events "
        "           WHERE stage = 'store' AND status IN ('ok', 'skipped') GROUP BY url_id) "
        "SELECT f.source_id, d.t AS discovered, s.t AS stored, r.published_at "
        "FROM s JOIN frontier f ON f.url_id = s.url_id "
        "LEFT JOIN d ON d.url_id = s.url_id "
        "LEFT JOIN doc_revisions r ON r.doc_id = f.doc_id AND r.revision = f.revision "
        "WHERE s.t >= ?",
        (to_db(start),),
    ).fetchall()
    lags: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    negative: dict[str, int] = defaultdict(int)
    for r in rows:
        key = r["source_id"] if by_source else "all"
        stored = from_db(r["stored"])
        discovered = from_db(r["discovered"]) if r["discovered"] else None
        published = from_db(r["published_at"]) if r["published_at"] else None
        if discovered is not None:
            lags[key]["ingest"].append((stored - discovered).total_seconds())
        if published is not None:
            lags[key]["e2e"].append((stored - published).total_seconds())
            if discovered is not None:
                d = (discovered - published).total_seconds()
                lags[key]["discovery"].append(d)
                negative[key] += d < 0
    groups = {
        k: LagGroup(Dist.of(v["discovery"]), Dist.of(v["ingest"]), Dist.of(v["e2e"]), negative[k])
        for k, v in sorted(lags.items())
    }
    report = LagReport(start, groups)

    for stage in STAGES:
        ev = conn.execute(
            "SELECT status, enqueued_at, started_at, finished_at FROM stage_events "
            "WHERE stage = ? AND finished_at >= ?",
            (stage, to_db(start)),
        ).fetchall()
        queue = [
            (from_db(e["started_at"]) - from_db(e["enqueued_at"])).total_seconds()
            for e in ev
            if e["enqueued_at"]
        ]
        work = [(from_db(e["finished_at"]) - from_db(e["started_at"])).total_seconds() for e in ev]
        report.stages[stage] = {"queue": Dist.of(queue), "work": Dist.of(work)}
        status: dict[str, int] = defaultdict(int)
        for e in ev:
            status[e["status"]] += 1
        report.stage_status[stage] = dict(status)

    report.queue = dict(conn.execute("SELECT state, count(*) FROM frontier GROUP BY state"))
    oldest = conn.execute(
        "SELECT min(ready_at) FROM frontier WHERE state IN ('pending', 'fetched') "
        "AND next_attempt_at <= ?",
        (to_db(now),),
    ).fetchone()[0]
    report.oldest_pending_s = (now - from_db(oldest)).total_seconds() if oldest else None
    report.feeds = dict(conn.execute("SELECT status, count(*) FROM feeds GROUP BY status"))
    return report


def _fmt_s(v: float | None) -> str:
    if v is None:
        return "-"
    if abs(v) < 120:
        return f"{v:.0f}s"
    if abs(v) < 7200:
        return f"{v / 60:.1f}m"
    return f"{v / 3600:.1f}h"


def format_lag(r: LagReport) -> str:
    out = [f"lag since {to_db(r.since)} (p50 / p95 / p99, n)"]
    for name, g in r.groups.items():
        out.append(f"  {name}")
        for label, d in (
            ("discovery", g.discovery),
            ("ingest", g.ingest),
            ("end-to-end", g.end_to_end),
        ):
            out.append(
                f"    {label:<11} {_fmt_s(d.p50):>7} {_fmt_s(d.p95):>7} {_fmt_s(d.p99):>7}  n={d.n}"
            )
        if g.negative_discovery:
            out.append(f"    ({g.negative_discovery} published after discovery: misdated)")
    out.append("stages (queue p50/p95, work p50/p95; statuses)")
    for stage, timings in r.stages.items():
        q, w = timings["queue"], timings["work"]
        out.append(
            f"  {stage:<9} queue {_fmt_s(q.p50):>6} {_fmt_s(q.p95):>6}  "
            f"work {_fmt_s(w.p50):>6} {_fmt_s(w.p95):>6}  {r.stage_status.get(stage, {})}"
        )
    out.append(f"frontier {r.queue}; oldest ready item waiting {_fmt_s(r.oldest_pending_s)}")
    out.append(f"feeds {r.feeds}")
    return "\n".join(out)


# --- disk ---------------------------------------------------------------------------------


@dataclass
class DiskReport:
    blob_bytes: int
    blob_count: int
    parquet_bytes: int
    parquet_rows: int
    sqlite_bytes: int
    fs_total: int
    fs_free: int
    window_days: float
    blob_bytes_per_day: float
    parquet_bytes_per_day: float
    sqlite_bytes_per_doc: float
    docs_per_day: float
    forecast_12mo: int
    fits_12mo: bool
    # Under a day of data, rates are noise and fixed SQLite overhead reads as growth.
    rough: bool = False


def disk_report(conn: sqlite3.Connection, settings: Settings, now: datetime) -> DiskReport:
    """Current usage and a 12-month forecast (G1.7) from the last 7 days' rate.

    Raw blobs plateau at ``retention.raw_days`` of intake; extracted text (Parquet) and
    the SQLite state grow linearly. Phase 2 vectors are not included yet.
    """
    blob_bytes, blob_count = conn.execute(
        "SELECT coalesce(sum(size_stored), 0), count(*) FROM blobs WHERE state = 'present'"
    ).fetchone()
    parquet_bytes, parquet_rows = conn.execute(
        "SELECT coalesce(sum(size_bytes), 0), coalesce(sum(row_count), 0) FROM table_files "
        "WHERE state = 'committed'"
    ).fetchone()
    db = settings.state_db
    sqlite_bytes = sum(p.stat().st_size for p in (db, db.with_name(db.name + "-wal")) if p.exists())
    usage = shutil.disk_usage(settings.data_dir)

    first = conn.execute("SELECT min(stored_at) FROM blobs").fetchone()[0]
    window_start = max(now - timedelta(days=7), from_db(first)) if first else now
    days = max((now - window_start).total_seconds() / 86400, 1 / 24)
    recent_blobs = conn.execute(
        "SELECT coalesce(sum(size_stored), 0) FROM blobs WHERE stored_at >= ?",
        (to_db(window_start),),
    ).fetchone()[0]
    recent_docs = conn.execute(
        "SELECT count(*) FROM doc_revisions WHERE ingested_at >= ?", (to_db(window_start),)
    ).fetchone()[0]
    blob_rate = recent_blobs / days
    docs_rate = recent_docs / days
    bytes_per_row = parquet_bytes / parquet_rows if parquet_rows else 0.0
    parquet_rate = docs_rate * bytes_per_row
    total_docs = conn.execute("SELECT count(*) FROM doc_revisions").fetchone()[0]
    sqlite_per_doc = sqlite_bytes / total_docs if total_docs else 0.0

    blobs_12mo = blob_rate * settings.retention.raw_days
    parquet_12mo = parquet_bytes + parquet_rate * 365
    sqlite_12mo = sqlite_bytes + sqlite_per_doc * docs_rate * 365
    forecast = int(blobs_12mo + parquet_12mo + sqlite_12mo)
    ours_now = blob_bytes + parquet_bytes + sqlite_bytes
    return DiskReport(
        blob_bytes=blob_bytes,
        blob_count=blob_count,
        parquet_bytes=parquet_bytes,
        parquet_rows=parquet_rows,
        sqlite_bytes=sqlite_bytes,
        fs_total=usage.total,
        fs_free=usage.free,
        window_days=round(days, 2),
        blob_bytes_per_day=blob_rate,
        parquet_bytes_per_day=parquet_rate,
        sqlite_bytes_per_doc=sqlite_per_doc,
        docs_per_day=docs_rate,
        forecast_12mo=forecast,
        fits_12mo=forecast < usage.free + ours_now,
        rough=days < 1,
    )


def _gb(b: float) -> str:
    return f"{b / 1e9:.2f} GB"


def format_disk(r: DiskReport) -> str:
    return "\n".join(
        [
            f"raw blobs   {_gb(r.blob_bytes):>11}  ({r.blob_count} present)",
            f"parquet     {_gb(r.parquet_bytes):>11}  ({r.parquet_rows} rows)",
            f"sqlite      {_gb(r.sqlite_bytes):>11}",
            f"filesystem  {_gb(r.fs_free):>11} free of {_gb(r.fs_total)}",
            f"rate over {r.window_days} days: {r.docs_per_day:.0f} docs/day, "
            f"blobs {_gb(r.blob_bytes_per_day)}/day, parquet {_gb(r.parquet_bytes_per_day)}/day",
            f"12-month forecast {_gb(r.forecast_12mo)} "
            f"(blobs plateau at retention; text and state grow): "
            f"{'FITS' if r.fits_12mo else 'DOES NOT FIT'} (G1.7)"
            + (" -- ROUGH: under a day of data" if r.rough else ""),
        ]
    )
