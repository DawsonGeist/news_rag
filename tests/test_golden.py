import shutil
from pathlib import Path

import pytest
import yaml

from newsrag.testing.golden import BODY_FILE, CASE_FILE, GOLDEN_DIR, evaluate, refill_unverified

SYNTHETIC = Path(__file__).parent / "fixtures" / "synthetic" / "articles"


def make_case(root: Path, name: str, expect: dict[str, object], verified: bool = True) -> None:
    d = root / "news.example" / name
    d.mkdir(parents=True)
    shutil.copy(SYNTHETIC / f"{name}.html", d / BODY_FILE)
    case = {
        "url": f"https://news.example/{name}",
        "final_url": f"https://news.example/{name}",
        "content_type": "text/html",
        "fetched_at": "2026-10-17T14:00:00+00:00",
        "verified": verified,
        "expect": expect,
    }
    (d / CASE_FILE).write_text(yaml.safe_dump(case), encoding="utf-8")


def test_metrics_machinery(tmp_path: Path) -> None:
    make_case(
        tmp_path,
        "meta_date",
        {
            "title": "River cleanup draws hundreds",
            "content_kind": "full_text",
            "published_at": "2026-10-16T20:00:00+00:00",  # within 24 h of 08:30
            "first_paragraph": "More than three hundred volunteers turned out",
            "last_paragraph": "can sign up at the library.",
        },
    )
    make_case(
        tmp_path,
        "non_article",
        {"content_kind": "full_text", "published_at": "2026-10-10T00:00:00+00:00"},
    )
    make_case(tmp_path, "spanish", {"content_kind": "full_text"}, verified=False)
    report = evaluate(tmp_path)
    assert report.unverified == 1
    by_name = {c.name.split("/")[1]: c for c in report.cases}
    assert by_name["meta_date"].date_ok and by_name["meta_date"].paragraphs_ok
    bad = by_name["non_article"]
    assert not bad.date_ok and not bad.kind_ok and not bad.paragraphs_ok
    assert report.date_ok == 0.5 and not report.passes


def test_refill_touches_only_unverified(tmp_path: Path) -> None:
    make_case(tmp_path, "meta_date", {"content_kind": "summary_only"}, verified=True)
    make_case(tmp_path, "spanish", {"content_kind": "summary_only"}, verified=False)
    assert refill_unverified(tmp_path) == 1
    load = lambda n: yaml.safe_load(  # noqa: E731
        (tmp_path / "news.example" / n / CASE_FILE).read_text(encoding="utf-8")
    )
    assert load("meta_date")["expect"]["content_kind"] == "summary_only"  # untouched
    assert load("spanish")["expect"]["content_kind"] == "full_text"


def test_golden_set_meets_gate_1() -> None:
    """G1.5 over the local, hand-verified golden set. Skipped until one exists."""
    report = evaluate(GOLDEN_DIR)
    if not report.cases:
        pytest.skip("no verified golden cases (capture with `newsrag fixtures capture`)")
    failing = [f"{c.name}: {c.notes}" for c in report.cases if c.notes]
    assert report.passes, failing
