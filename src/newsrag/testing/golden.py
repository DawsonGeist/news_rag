"""Golden fixtures: real publisher pages with hand-checked expectations (Gate 1, G1.5).

They stay local (``tests/fixtures/golden/`` is gitignored for copyright). Each case is a
directory holding the raw response body and a ``case.yaml``::

    url, final_url, content_type, fetched_at   # how it was captured
    verified: false                             # a human sets true after checking
    expect:
      title, content_kind, published_at         # published_at may be null
      first_paragraph, last_paragraph           # full_text pages only

``capture`` pre-fills ``expect`` from the current extractor so the human only corrects
it. Unverified cases are ignored by the metrics, so a guess is never ground truth.
"""

from __future__ import annotations

import asyncio
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from newsrag import ids
from newsrag.config import REPO_ROOT, Settings
from newsrag.contracts.v1 import ContentKind, Document
from newsrag.crawl.fetcher import FetchClaim, Fetcher
from newsrag.crawl.http import host_of, make_client
from newsrag.extract.extractor import ExtractContext, extract
from newsrag.storage.fsio import TmpArea
from newsrag.storage.state import connect, init_schema
from newsrag.timeutil import utcnow

GOLDEN_DIR = REPO_ROOT / "tests" / "fixtures" / "golden"
BODY_FILE = "body.bin"
CASE_FILE = "case.yaml"
DATE_TOLERANCE = timedelta(hours=24)

# Gate 1, G1.5 thresholds.
MIN_DATE_OK = 0.90
MIN_KIND_OK = 0.95
MIN_PARAGRAPHS_OK = 0.90
TARGET_CASES = 40  # "about 40 real pages across 20 publishers" (phase1-design.md)
TARGET_PUBLISHERS = 20


def _slug(url: str) -> str:
    path = re.sub(r"[^a-z0-9]+", "-", url.lower().split("://", 1)[-1]).strip("-")
    return f"{path[:80]}-{ids.hash32(url)[:8]}"


async def _fetch(settings: Settings, url: str) -> tuple[bytes, str, str | None, datetime]:
    """Fetch one page with the real fetcher's politeness rules, against a scratch DB."""
    with tempfile.TemporaryDirectory(prefix="newsrag-capture-") as scratch:
        s = settings.model_copy(update={"data_dir": Path(scratch)})
        conn = connect(s.state_db)
        init_schema(conn)
        try:
            with TmpArea(s.tmp_dir, "capture") as tmp:
                async with make_client(s) as client:
                    fetcher = Fetcher(s, conn, tmp, client, "capture")
                    norm = ids.normalize_url(url)
                    claim = FetchClaim(ids.url_id(norm), norm, host_of(norm), None, 1, utcnow())
                    out = await fetcher.fetch(claim)
        finally:
            conn.close()
    if out.kind != "fetched" or out.body is None:
        raise RuntimeError(
            f"not captured: {out.kind} {out.error_code or out.skip_reason} (HTTP {out.http_status})"
        )
    return out.body, out.final_url or norm, out.content_type, utcnow()


def _context(case: dict[str, Any], source_id: str) -> ExtractContext:
    fetched = datetime.fromisoformat(case["fetched_at"])
    return ExtractContext(
        final_url=case["final_url"],
        source_id=source_id,
        discovered_at=fetched,
        fetched_at=fetched,
        pipeline_version="golden",
        content_type=case.get("content_type"),
    )


def capture(settings: Settings, url: str, root: Path = GOLDEN_DIR) -> Path:
    body, final_url, ctype, fetched_at = asyncio.run(_fetch(settings, url))
    source_id = ids.registered_domain(final_url)
    case_dir = root / source_id / _slug(final_url)
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / BODY_FILE).write_bytes(body)
    case: dict[str, Any] = {
        "url": url,
        "final_url": final_url,
        "content_type": ctype,
        "fetched_at": fetched_at.isoformat(),
        "verified": False,
    }
    _prefill(case_dir, case)
    return case_dir


def _prefill(case_dir: Path, case: dict[str, Any]) -> None:
    """Write ``case`` with ``expect`` filled from the current extractor's output."""
    doc = extract((case_dir / BODY_FILE).read_bytes(), _context(case, case_dir.parent.name))
    if isinstance(doc, Document):
        paras = doc.extracted_text.split("\n\n")
        case["expect"] = {
            "title": doc.title,
            "content_kind": doc.content_kind.value,
            "published_at": doc.published_at.isoformat() if doc.published_at else None,
            "first_paragraph": paras[0] if doc.extracted_text else None,
            "last_paragraph": paras[-1] if doc.extracted_text else None,
        }
    (case_dir / CASE_FILE).write_text(
        "# Pre-filled by the extractor: CHECK every field against the page, then set\n"
        "# verified: true. published_at is the page's own publication time (UTC).\n"
        + yaml.safe_dump(case, sort_keys=False, allow_unicode=True, width=100),
        encoding="utf-8",
    )


def refill_unverified(root: Path = GOLDEN_DIR) -> int:
    """Re-run the extractor on unverified cases (after an extractor fix). Verified cases
    are ground truth and are never touched."""
    n = 0
    for case_file in sorted(root.glob(f"*/*/{CASE_FILE}")):
        case = yaml.safe_load(case_file.read_text(encoding="utf-8"))
        if not case.get("verified"):
            _prefill(case_file.parent, case)
            n += 1
    return n


@dataclass
class CaseResult:
    name: str
    date_ok: bool
    kind_ok: bool
    paragraphs_ok: bool | None  # None: not a full_text page
    notes: list[str] = field(default_factory=list)


@dataclass
class GoldenReport:
    cases: list[CaseResult]
    unverified: int

    def _ratio(self, values: list[bool]) -> float:
        return sum(values) / len(values) if values else 1.0

    @property
    def date_ok(self) -> float:
        return self._ratio([c.date_ok for c in self.cases])

    @property
    def kind_ok(self) -> float:
        return self._ratio([c.kind_ok for c in self.cases])

    @property
    def paragraphs_ok(self) -> float:
        return self._ratio([c.paragraphs_ok for c in self.cases if c.paragraphs_ok is not None])

    @property
    def publishers(self) -> int:
        return len({c.name.split("/", 1)[0] for c in self.cases})

    @property
    def passes(self) -> bool:
        return (
            bool(self.cases)
            and self.date_ok >= MIN_DATE_OK
            and self.kind_ok >= MIN_KIND_OK
            and self.paragraphs_ok >= MIN_PARAGRAPHS_OK
        )


def _check(case_dir: Path, case: dict[str, Any]) -> CaseResult:
    exp = case["expect"]
    source_id = case_dir.parent.name
    doc = extract((case_dir / BODY_FILE).read_bytes(), _context(case, source_id))
    name = f"{source_id}/{case_dir.name}"
    if not isinstance(doc, Document):
        return CaseResult(name, False, False, False, [f"extraction failed: {doc.error_code}"])
    notes = []
    want_date = exp.get("published_at")
    if want_date is None:
        date_ok = doc.published_at is None
    else:
        want = datetime.fromisoformat(want_date)
        date_ok = doc.published_at is not None and abs(doc.published_at - want) <= DATE_TOLERANCE
    if not date_ok:
        notes.append(f"date {doc.published_at} ({doc.published_at_source}) != {want_date}")
    kind_ok = doc.content_kind.value == exp["content_kind"]
    if not kind_ok:
        notes.append(f"kind {doc.content_kind.value} != {exp['content_kind']}")
    paragraphs_ok = None
    if exp["content_kind"] == ContentKind.FULL_TEXT.value:
        present = [
            bool(p) and p in doc.extracted_text
            for p in (exp.get("first_paragraph"), exp.get("last_paragraph"))
        ]
        paragraphs_ok = all(present)
        if not present[0]:
            notes.append("first paragraph missing")
        if not present[1]:
            notes.append("last paragraph missing")
    return CaseResult(name, date_ok, kind_ok, paragraphs_ok, notes)


def evaluate(root: Path = GOLDEN_DIR) -> GoldenReport:
    results, unverified = [], 0
    for case_file in sorted(root.glob(f"*/*/{CASE_FILE}")):
        case = yaml.safe_load(case_file.read_text(encoding="utf-8"))
        if not case.get("verified"):
            unverified += 1
            continue
        results.append(_check(case_file.parent, case))
    return GoldenReport(results, unverified)
