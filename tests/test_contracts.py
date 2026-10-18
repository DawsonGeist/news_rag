from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from newsrag import ids
from newsrag.cli import render_schemas
from newsrag.contracts.v1 import (
    ContentKind,
    DateSource,
    Document,
    Locator,
    Pipeline,
    Stage,
    StageEvent,
    StageStatus,
    TextLocator,
)

REPO = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)


def make_doc(**overrides: Any) -> Document:
    url = "https://example.com/news/story-1"
    text = "First paragraph.\n\nSecond paragraph."
    fields: dict[str, Any] = {
        "doc_id": ids.doc_id(url),
        "revision": 1,
        "pipeline": Pipeline.RSS,
        "source_id": "example.com",
        "canonical_url": url,
        "url_canonicalizer_version": ids.CANONICALIZER_VERSION,
        "final_url": url,
        "title": "A story",
        "extracted_text": text,
        "content_kind": ContentKind.FULL_TEXT,
        "language": "en",
        "word_count": 4,
        "published_at": T0 - timedelta(hours=2),
        "published_at_source": DateSource.JSON_LD,
        "discovered_at": T0,
        "fetched_at": T0 + timedelta(seconds=30),
        "ingested_at": T0 + timedelta(seconds=40),
        "content_hash": ids.content_hash("A story", text),
        "raw_blob_hash": ids.sha256_hex(b"<html></html>"),
        "extractor_version": "test-0",
        "pipeline_version": "test-0",
    }
    fields.update(overrides)
    return Document(**fields)


def test_document_valid_roundtrip() -> None:
    doc = make_doc()
    again = Document.model_validate_json(doc.model_dump_json())
    assert again == doc


def test_document_converts_offsets_to_utc() -> None:
    est = timezone(timedelta(hours=-5))
    doc = make_doc(published_at=datetime(2026, 10, 17, 5, 0, tzinfo=est))
    assert doc.published_at == datetime(2026, 10, 17, 10, 0, tzinfo=UTC)


def test_document_rejects_naive_datetime() -> None:
    with pytest.raises(ValidationError):
        make_doc(discovered_at=datetime(2026, 10, 17, 12, 0))


@pytest.mark.parametrize(
    "bad_text",
    [
        " leading space",
        "single\nnewline",
        "triple\n\n\nnewline",
        "carriage\r\n\r\nreturn",
        "café",  # NFD
    ],
)
def test_document_text_rules(bad_text: str) -> None:
    with pytest.raises(ValidationError):
        make_doc(extracted_text=bad_text)


def test_document_empty_text_only_for_non_article_or_paywall() -> None:
    make_doc(extracted_text="", content_kind=ContentKind.NON_ARTICLE)
    make_doc(extracted_text="", content_kind=ContentKind.SUSPECTED_PAYWALL)
    with pytest.raises(ValidationError):
        make_doc(extracted_text="", content_kind=ContentKind.FULL_TEXT)


def test_document_published_at_requires_source() -> None:
    with pytest.raises(ValidationError):
        make_doc(published_at_source=DateSource.NONE)
    make_doc(published_at=None, published_at_source=DateSource.NONE)


def test_document_time_order() -> None:
    with pytest.raises(ValidationError):
        make_doc(fetched_at=T0 - timedelta(seconds=1))


def test_document_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        make_doc(dup_of="x")


def test_locator_discriminated_union() -> None:
    adapter: TypeAdapter[Locator] = TypeAdapter(Locator)
    loc = adapter.validate_python({"kind": "time", "start_ms": 0, "end_ms": 1500})
    assert loc.kind == "time"
    assert isinstance(adapter.validate_python({"kind": "text", "start": 0, "end": 5}), TextLocator)
    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "text", "start": 5, "end": 1})


def test_stage_event() -> None:
    ev = StageEvent(
        event_id="01JABCDEF0000000000000000",
        stage=Stage.FETCH,
        status=StageStatus.OK,
        url_id=ids.url_id("https://e.com/a"),
        worker_id="laptop:fetch-1:0",
        started_at=T0,
        finished_at=T0,
        pipeline_version="test-0",
    )
    assert ev.attempt == 1


def test_exported_schemas_in_sync() -> None:
    schemas_dir = REPO / "contracts" / "v1" / "schemas"
    for name, text in render_schemas().items():
        path = schemas_dir / name
        assert path.exists(), f"missing {name}; run `newsrag contracts export`"
        assert path.read_text(encoding="utf-8") == text, (
            f"{name} stale; run `newsrag contracts export`"
        )
