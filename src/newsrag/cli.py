"""``newsrag`` command line. Each long-running component gets its own subcommand so it
can run as a separate process (or on a separate machine) behind the contracts."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import typer

from newsrag.config import REPO_ROOT, load_settings

app = typer.Typer(no_args_is_help=True, add_completion=False)
contracts_app = typer.Typer(no_args_is_help=True, help="Contract schemas.")
feeds_app = typer.Typer(no_args_is_help=True, help="Feed lists and vetting.")
db_app = typer.Typer(no_args_is_help=True, help="Crawl-state database.")
storage_app = typer.Typer(no_args_is_help=True, help="Archive storage.")
test_app = typer.Typer(no_args_is_help=True, help="Durability tests.")
run_app = typer.Typer(no_args_is_help=True, help="Long-running pipeline processes.")
fixtures_app = typer.Typer(no_args_is_help=True, help="Golden fixtures (local only).")
app.add_typer(contracts_app, name="contracts")
app.add_typer(feeds_app, name="feeds")
app.add_typer(db_app, name="db")
app.add_typer(storage_app, name="storage")
app.add_typer(test_app, name="test")
app.add_typer(run_app, name="run")
app.add_typer(fixtures_app, name="fixtures")
report_app = typer.Typer(no_args_is_help=True, help="Lag and disk reports.")
app.add_typer(report_app, name="report")

SCHEMAS_DIR = REPO_ROOT / "contracts" / "v1" / "schemas"
FEEDS_DIR = REPO_ROOT / "feeds"


def render_schemas() -> dict[str, str]:
    """JSON Schema text for every v1 record, keyed by file name."""
    from newsrag.contracts.v1 import PROVISIONAL_RECORDS, STABLE_RECORDS

    out = {}
    for model in (*STABLE_RECORDS, *PROVISIONAL_RECORDS):
        schema = model.model_json_schema()
        schema["$id"] = f"newsrag/contracts/v1/{model.__name__}"
        schema["x-stability"] = "stable" if model in STABLE_RECORDS else "provisional"
        out[f"{model.__name__}.schema.json"] = json.dumps(schema, indent=2, sort_keys=True) + "\n"
    return out


@contracts_app.command("export")
def contracts_export(
    out_dir: Path = typer.Option(SCHEMAS_DIR, help="Where to write *.schema.json."),
    check: bool = typer.Option(False, help="Fail if files on disk differ instead of writing."),
) -> None:
    """Export JSON Schemas for all v1 records from the pydantic models."""
    rendered = render_schemas()
    stale = [
        n
        for n, t in rendered.items()
        if not (out_dir / n).exists() or (out_dir / n).read_text(encoding="utf-8") != t
    ]
    if check:
        if stale:
            typer.echo(f"out of date: {', '.join(stale)}. Run `newsrag contracts export`.")
            raise typer.Exit(1)
        typer.echo("schemas up to date")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, text in rendered.items():
        (out_dir / name).write_text(text, encoding="utf-8", newline="\n")
    typer.echo(f"wrote {len(rendered)} schemas to {out_dir} ({len(stale)} changed)")


@feeds_app.command("candidates")
def feeds_candidates(
    source: Path = typer.Option(FEEDS_DIR / "source" / "feeds_world.yaml"),
    supplement: Path = typer.Option(FEEDS_DIR / "supplement_us.yaml"),
    out: Path = typer.Option(FEEDS_DIR / "candidates_us.yaml"),
    country: str = typer.Option("United States"),
) -> None:
    """Vetter stage A: extract US feed candidates from the curated lists."""
    from newsrag.contracts.v1 import FeedOrigin
    from newsrag.feeds.candidates import extract_candidates, load_yaml, write_candidates

    inputs = [(load_yaml(source), FeedOrigin.FEEDS_YAML)]
    if supplement.exists():
        inputs.append((load_yaml(supplement), FeedOrigin.SUPPLEMENT))
    candidates, stats = extract_candidates(inputs, country=country)
    write_candidates(candidates, out)
    typer.echo(
        f"domains seen {stats.domains_seen}, dropped non-English {stats.domains_non_english}"
    )
    typer.echo(
        f"feeds seen {stats.feeds_seen}: excluded category {stats.feeds_excluded_category}, "
        f"invalid URL {stats.feeds_invalid_url}, merged duplicates {stats.feeds_merged_duplicate}"
    )
    typer.echo(f"candidates {len(candidates)} by medium: {dict(stats.by_medium)}")
    for cat, n in stats.by_category.most_common():
        typer.echo(f"  {n:6d}  {cat}")
    typer.echo(f"wrote {out}")


@feeds_app.command("import")
def feeds_import(
    path: Path = typer.Option(FEEDS_DIR / "candidates_us.yaml"),
    activate: bool = typer.Option(
        False, help="Set every imported feed active (for a vetted_us.yaml export)."
    ),
) -> None:
    """Load feed candidates into the crawl-state DB (idempotent; keeps statuses)."""
    from newsrag.feeds.registry import import_candidates, load_candidates, set_status
    from newsrag.storage.state import connect, init_schema
    from newsrag.timeutil import utcnow

    settings = load_settings()
    conn = connect(settings.state_db)
    init_schema(conn)
    cands = load_candidates(path)
    stats = import_candidates(conn, cands, utcnow())
    if activate:
        set_status(
            conn, [c.feed_id for c in cands], "active", f"imported from {path.name}", utcnow()
        )
    conn.close()
    typer.echo(
        f"sources new {stats.sources_new}; feeds new {stats.feeds_new}, "
        f"updated {stats.feeds_updated}"
    )


@feeds_app.command("vet")
def feeds_vet(
    limit: int | None = typer.Option(
        None, help="Pilot: vet whole publishers at random until this many feeds."
    ),
    seed: int = typer.Option(0, help="Random seed for --limit."),
    source: list[str] = typer.Option([], help="Only these publishers (source_id)."),
    recheck: bool = typer.Option(False, help="Re-vet feeds already vetted by this version."),
) -> None:
    """Vetter stages B and C: check feeds, sample articles, record verdicts (resumable)."""
    import asyncio

    from newsrag.crawl.http import make_client
    from newsrag.feeds.vetter import Vetter, report_counts, select_feeds
    from newsrag.procutil import owned_pool
    from newsrag.storage.state import connect, init_schema

    _setup_logging()
    settings = load_settings()
    conn = connect(settings.state_db)
    init_schema(conn)
    by_source = select_feeds(conn, recheck=recheck, limit=limit, seed=seed, sources=source)
    n = sum(len(f) for f in by_source.values())
    typer.echo(f"vetting {n} feeds from {len(by_source)} publishers")

    async def main() -> dict[str, int]:
        with owned_pool(settings.writer.extract_workers) as pool:
            async with make_client(settings) as client:
                return await Vetter(settings, conn, client, pool).run(by_source)

    counts = asyncio.run(main())
    typer.echo(f"this run: {counts}")
    typer.echo(f"all runs: {report_counts(conn)}")
    conn.close()


@feeds_app.command("report")
def feeds_report(
    list_status: str | None = typer.Option(
        None, "--list", help="Also list feeds with this status (e.g. review), with reasons."
    ),
) -> None:
    """Latest verdict counts and issues over all vetted feeds (G1.1)."""
    import json

    from newsrag.feeds.vetter import report_counts
    from newsrag.storage.state import connect

    settings = load_settings()
    conn = connect(settings.state_db)
    typer.echo(json.dumps(report_counts(conn), indent=2))
    if list_status:
        rows = conn.execute(
            "SELECT f.feed_url, f.status_reason, json_extract(h.report_json, '$.detail') "
            "FROM feeds f JOIN feed_health h ON h.feed_id = f.feed_id "
            "WHERE f.status = ? AND h.checked_at = "
            "(SELECT max(checked_at) FROM feed_health WHERE feed_id = f.feed_id) "
            "ORDER BY f.source_id, f.feed_url",
            (list_status,),
        ).fetchall()
        for url, reason, detail in rows:
            typer.echo(f"{url}\t{reason or ''}\t{detail or ''}")
    conn.close()


@feeds_app.command("export")
def feeds_export(
    candidates: Path = typer.Option(FEEDS_DIR / "candidates_us.yaml"),
    out: Path = typer.Option(FEEDS_DIR / "vetted_us.yaml"),
) -> None:
    """Write the accepted (active) feeds as candidate rows, for `feeds import --activate`."""
    from newsrag.feeds.candidates import write_candidates
    from newsrag.feeds.registry import load_candidates
    from newsrag.storage.state import connect

    settings = load_settings()
    conn = connect(settings.state_db)
    active = {r[0] for r in conn.execute("SELECT feed_id FROM feeds WHERE status = 'active'")}
    conn.close()
    rows = [c for c in load_candidates(candidates) if c.feed_id in active]
    write_candidates(rows, out, generated_by="newsrag feeds export")
    typer.echo(f"wrote {len(rows)} accepted feeds to {out}")


@feeds_app.command("pause-blocked")
def feeds_pause_blocked(
    dry_run: bool = typer.Option(False, help="Only list the feeds that would be paused."),
) -> None:
    """Pause active feeds whose article fetches were mostly answered 403 over the last
    7 days (the publisher blocks the crawler). Run daily by the supervisor."""
    from newsrag.feeds.registry import find_blocked_feeds, pause_blocked_feeds
    from newsrag.storage.state import connect
    from newsrag.timeutil import utcnow

    settings = load_settings()
    conn = connect(settings.state_db)
    found = (find_blocked_feeds if dry_run else pause_blocked_feeds)(conn, utcnow())
    conn.close()
    for b in found:
        typer.echo(f"{b.feed_url}\t{b.blocked}/{b.fetches} blocked")
    typer.echo(f"{len(found)} feeds {'would be ' if dry_run else ''}paused")


@feeds_app.command("set-status")
def feeds_set_status(
    status: str = typer.Argument(..., help="candidate | active | review | rejected | paused"),
    feeds: list[str] = typer.Argument(..., help="Feed URLs or feed_ids."),
    reason: str = typer.Option("no reason given", help="Why; kept with the decision."),
) -> None:
    """Set feed status by hand.

    active, rejected and review are decisions: they are final, and the vetter never
    re-vets or overwrites the feed. candidate and paused are operational and lock nothing.
    """
    from newsrag.feeds.registry import MANUAL_VERDICTS, decide_manually, resolve_feed, set_status
    from newsrag.storage.state import connect
    from newsrag.timeutil import utcnow

    settings = load_settings()
    conn = connect(settings.state_db)
    ids_ = []
    for ref in feeds:
        fid = resolve_feed(conn, ref)
        if fid is None:
            typer.echo(f"unknown feed: {ref} (run `newsrag feeds import` first?)", err=True)
            raise typer.Exit(1)
        ids_.append(fid)
    if status in MANUAL_VERDICTS:
        n = decide_manually(conn, ids_, status, reason, utcnow())
        typer.echo(f"{n} feeds set to {status} (manual decision; the vetter will keep it)")
    else:
        n = set_status(conn, ids_, status, reason, utcnow())
        typer.echo(f"{n} feeds set to {status}")
    conn.close()


@db_app.command("init")
def db_init() -> None:
    """Create the crawl-state database (idempotent)."""
    from newsrag.storage.state import connect, init_schema

    settings = load_settings()
    conn = connect(settings.state_db)
    init_schema(conn)
    conn.close()
    typer.echo(f"initialized {settings.state_db}")


@storage_app.command("verify")
def storage_verify(
    deep: bool = typer.Option(False, help="Re-hash every committed file and read every part."),
) -> None:
    """Check the storage invariants against the data directory (read-only)."""
    from newsrag.storage.recovery import verify_storage
    from newsrag.storage.state import connect

    settings = load_settings()
    conn = connect(settings.state_db)
    problems = verify_storage(conn, settings.data_dir, deep=deep)
    conn.close()
    for p in problems:
        typer.echo(p)
    typer.echo(f"{len(problems)} problems in {settings.data_dir}")
    if problems:
        raise typer.Exit(1)


@storage_app.command("compact")
def storage_compact() -> None:
    """Merge small Parquet parts into monthly parts. Needs the writer lock, so it fails
    while `run writer` is running (the writer compacts on its own)."""
    from newsrag.storage.compaction import compact
    from newsrag.storage.fsio import ProcessLock, TmpArea
    from newsrag.storage.state import connect
    from newsrag.storage.writer import WRITER_LOCK
    from newsrag.timeutil import utcnow

    settings = load_settings()
    conn = connect(settings.state_db)
    with ProcessLock(settings.locks_dir / WRITER_LOCK), TmpArea(settings.tmp_dir, "compact") as tmp:
        report = compact(
            conn, settings.data_dir, tmp, utcnow(), settings.writer.compact_target_mb * 1024 * 1024
        )
    conn.close()
    typer.echo(f"compaction: {report}")


@storage_app.command("reextract")
def storage_reextract(
    verify: bool = typer.Option(True, help="Compare content_hash (the only mode for now)."),
    sample: int | None = typer.Option(None, help="Check at most this many Documents."),
) -> None:
    """G1.6: re-extract stored blobs and confirm identical content_hash values."""
    from newsrag.procutil import owned_pool
    from newsrag.storage.blobs import BlobStore
    from newsrag.storage.fsio import TmpArea
    from newsrag.storage.reextract import reextract
    from newsrag.storage.state import connect

    settings = load_settings()
    conn = connect(settings.state_db)
    with (
        TmpArea(settings.tmp_dir, "reextract") as tmp,
        owned_pool(settings.writer.extract_workers) as pool,
    ):
        report = reextract(conn, BlobStore(settings.data_dir, tmp), pool, sample)
    conn.close()
    typer.echo(
        f"compared {report.compared}, matched {report.matched}, "
        f"mismatched {len(report.mismatches)}; skipped: other extractor version "
        f"{report.other_version}, blob expired {report.blob_missing}"
    )
    for m in report.mismatches[:20]:
        typer.echo(f"  mismatch {m}")
    if not report.passes:
        raise typer.Exit(1)


@storage_app.command("retention")
def storage_retention() -> None:
    """Expire raw blobs past retention, sweep orphaned blob files, prune the poll log."""
    from newsrag.storage.retention import run_retention
    from newsrag.storage.state import connect
    from newsrag.timeutil import utcnow

    _setup_logging()
    settings = load_settings()
    conn = connect(settings.state_db)
    report = run_retention(conn, settings, utcnow())
    conn.close()
    typer.echo(f"retention: {report}")


def _parse_since(value: str) -> timedelta:
    units = {"m": 60, "h": 3600, "d": 86400}
    if not value or value[-1] not in units or not value[:-1].isdigit():
        raise typer.BadParameter("use e.g. 30m, 24h or 7d")
    return timedelta(seconds=int(value[:-1]) * units[value[-1]])


@report_app.command("lag")
def report_lag(
    since: str = typer.Option("24h", help="Window, e.g. 30m, 24h, 3d."),
    by: str = typer.Option("all", help="all | source"),
) -> None:
    """Discovery, ingest and end-to-end lag p50/p95/p99, stage timings, queue depth (G1.3)."""
    from newsrag.obs.reports import format_lag, lag_report
    from newsrag.storage.state import connect
    from newsrag.timeutil import utcnow

    settings = load_settings()
    conn = connect(settings.state_db)
    typer.echo(format_lag(lag_report(conn, utcnow(), _parse_since(since), by == "source")))
    conn.close()


@report_app.command("disk")
def report_disk() -> None:
    """Blob, Parquet and SQLite sizes and a 12-month fill forecast (G1.7)."""
    from newsrag.obs.reports import disk_report, format_disk
    from newsrag.storage.state import connect
    from newsrag.timeutil import utcnow

    settings = load_settings()
    conn = connect(settings.state_db)
    typer.echo(format_disk(disk_report(conn, settings, utcnow())))
    conn.close()


@test_app.command("kill")
def test_kill(
    iterations: int = typer.Option(20),
    kills: int = typer.Option(3, help="SIGKILLs per iteration."),
    seed: int | None = typer.Option(None),
    storage_only: bool = typer.Option(
        False, help="Only the storage workload (fast), not the full `run all` pipeline."
    ),
) -> None:
    """SIGKILL the pipeline at random moments, restart it, check invariants (G1.4)."""
    if storage_only:
        from newsrag.testing.killtest import run_kill_test

        failures = run_kill_test(iterations, kills, seed)
    else:
        from newsrag.testing.pipeline_killtest import run_pipeline_kill_test

        failures = run_pipeline_kill_test(iterations, kills, seed)
    for f in failures:
        typer.echo(f)
    typer.echo(f"{iterations - len(failures)}/{iterations} iterations passed")
    if failures:
        raise typer.Exit(1)


def _setup_logging() -> None:
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )


@run_app.command("fetcher")
def run_fetcher_cmd() -> None:
    """Fetch pending frontier URLs until SIGTERM/SIGINT, then drain in-flight requests."""
    import asyncio
    import signal

    from newsrag.crawl.fetcher import run_fetcher
    from newsrag.storage.state import connect, init_schema

    _setup_logging()
    settings = load_settings()
    conn = connect(settings.state_db)
    init_schema(conn)

    async def main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        await run_fetcher(settings, conn, stop)

    asyncio.run(main())
    conn.close()


@run_app.command("poller")
def run_poller_cmd() -> None:
    """Poll active feeds until SIGTERM/SIGINT, then finish in-flight polls."""
    import asyncio
    import signal

    from newsrag.crawl.poller import run_poller
    from newsrag.storage.state import connect, init_schema

    _setup_logging()
    settings = load_settings()
    conn = connect(settings.state_db)
    init_schema(conn)

    async def main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        await run_poller(settings, conn, stop)

    asyncio.run(main())
    conn.close()


@run_app.command("all")
def run_all_cmd() -> None:
    """Supervise poller, fetcher and writer; restart any that die (G1.2)."""
    import signal

    from newsrag.storage.state import connect, init_schema
    from newsrag.supervisor import Supervisor, setup_supervisor_logging

    settings = load_settings()
    setup_supervisor_logging(settings.data_dir / "logs")
    conn = connect(settings.state_db)
    init_schema(conn)
    conn.close()
    sup = Supervisor(settings)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: sup.stopping.set())
    sup.run()


@run_app.command("writer")
def run_writer_cmd() -> None:
    """Extract and store fetched pages until SIGTERM/SIGINT, then drain and flush."""
    import signal
    import threading

    from newsrag.storage.state import connect, init_schema
    from newsrag.storage.writer_loop import run_writer

    _setup_logging()
    settings = load_settings()
    conn = connect(settings.state_db)
    init_schema(conn)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    run_writer(settings, conn, stop)
    conn.close()


@fixtures_app.command("capture")
def fixtures_capture(urls: list[str]) -> None:
    """Fetch pages politely and save them as unverified golden cases to check by hand."""
    from newsrag.testing.golden import capture

    settings = load_settings()
    for url in urls:
        try:
            typer.echo(f"saved {capture(settings, url)}")
        except Exception as e:
            typer.echo(f"{url}: {e}", err=True)


@fixtures_app.command("from-archive")
def fixtures_from_archive(urls: list[str]) -> None:
    """Make unverified golden cases from pages already in the archive (no new fetch)."""
    from newsrag.storage.blobs import BlobStore
    from newsrag.storage.fsio import TmpArea
    from newsrag.storage.state import connect
    from newsrag.testing.golden import capture_from_archive

    settings = load_settings()
    conn = connect(settings.state_db)
    with TmpArea(settings.tmp_dir, "fixtures") as tmp:
        blobs = BlobStore(settings.data_dir, tmp)
        for url in urls:
            try:
                typer.echo(f"saved {capture_from_archive(conn, blobs, url)}")
            except Exception as e:
                typer.echo(f"{url}: {e}", err=True)
    conn.close()


@fixtures_app.command("refill")
def fixtures_refill() -> None:
    """Re-fill unverified golden cases from the current extractor (no network)."""
    from newsrag.testing.golden import refill_unverified

    typer.echo(f"refilled {refill_unverified()} unverified cases")


@fixtures_app.command("report")
def fixtures_report() -> None:
    """Gate 1 G1.5 metrics over the verified golden cases."""
    from newsrag.testing.golden import (
        MIN_DATE_OK,
        MIN_KIND_OK,
        MIN_PARAGRAPHS_OK,
        TARGET_CASES,
        TARGET_PUBLISHERS,
        evaluate,
    )

    report = evaluate()
    for c in report.cases:
        if c.notes:
            typer.echo(f"{c.name}: {'; '.join(c.notes)}")
    typer.echo(
        f"{len(report.cases)}/{TARGET_CASES} verified cases from "
        f"{report.publishers}/{TARGET_PUBLISHERS} publishers ({report.unverified} unverified)"
    )
    if not report.cases:
        typer.echo("no verified cases: check case.yaml files and set verified: true")
        raise typer.Exit(1)
    typer.echo(
        f"  date within 24 h   {report.date_ok:6.1%}  (need {MIN_DATE_OK:.0%})\n"
        f"  content kind       {report.kind_ok:6.1%}  (need {MIN_KIND_OK:.0%})\n"
        f"  first+last para    {report.paragraphs_ok:6.1%}  (need {MIN_PARAGRAPHS_OK:.0%})"
    )
    if not report.passes:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
