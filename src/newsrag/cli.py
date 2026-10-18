"""``newsrag`` command line. Each long-running component gets its own subcommand so it
can run as a separate process (or on a separate machine) behind the contracts."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from newsrag.config import REPO_ROOT, load_settings

app = typer.Typer(no_args_is_help=True, add_completion=False)
contracts_app = typer.Typer(no_args_is_help=True, help="Contract schemas.")
feeds_app = typer.Typer(no_args_is_help=True, help="Feed lists and vetting.")
db_app = typer.Typer(no_args_is_help=True, help="Crawl-state database.")
storage_app = typer.Typer(no_args_is_help=True, help="Archive storage.")
test_app = typer.Typer(no_args_is_help=True, help="Durability tests.")
app.add_typer(contracts_app, name="contracts")
app.add_typer(feeds_app, name="feeds")
app.add_typer(db_app, name="db")
app.add_typer(storage_app, name="storage")
app.add_typer(test_app, name="test")

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


@test_app.command("kill")
def test_kill(
    iterations: int = typer.Option(20),
    kills: int = typer.Option(2, help="SIGKILLs per iteration before the final run."),
    seed: int | None = typer.Option(None),
) -> None:
    """SIGKILL the storage workload at random moments, restart it, check invariants (G1.4)."""
    from newsrag.testing.killtest import run_kill_test

    failures = run_kill_test(iterations, kills, seed)
    for f in failures:
        typer.echo(f)
    typer.echo(f"{iterations - len(failures)}/{iterations} iterations passed")
    if failures:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
