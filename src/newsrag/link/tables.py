"""Derived tables of the entity stage (table group ``enrichment``): mentions,
mention links, snippets and local entities. Schemas are hand-written; tests check them
against the contract models."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import pyarrow as pa

from newsrag.contracts.v1 import SCHEMA_VERSION, Entity, Mention, MentionLink, Snippet

MENTIONS, MENTION_LINKS, SNIPPETS, ENTITIES = "mentions", "mention_links", "snippets", "entities"

_TS = pa.timestamp("us", tz="UTC")
_LOCATOR = pa.struct(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("field", pa.string(), nullable=False),
        pa.field("start", pa.int32(), nullable=False),
        pa.field("end", pa.int32(), nullable=False),
    ]
)


def _meta(table: str) -> dict[str, str]:
    return {"newsrag.table": table, "newsrag.schema_version": SCHEMA_VERSION}


MENTION_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("mention_id", pa.string(), nullable=False),
        pa.field("doc_id", pa.string(), nullable=False),
        pa.field("revision", pa.int32(), nullable=False),
        pa.field("segment_id", pa.string()),
        pa.field("surface", pa.string(), nullable=False),
        pa.field("locator", _LOCATOR, nullable=False),
        pa.field("ner_label", pa.string(), nullable=False),
        pa.field("ner_confidence", pa.float64()),
        pa.field("ner_model_id", pa.string(), nullable=False),
        pa.field("pipeline_version", pa.string(), nullable=False),
    ],
    metadata=_meta(MENTIONS),
)

MENTION_LINK_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("mention_id", pa.string(), nullable=False),
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("confidence", pa.float64(), nullable=False),
        pa.field("method", pa.string(), nullable=False),
        pa.field("linker_version", pa.string(), nullable=False),
        pa.field("kb_snapshot", pa.string()),
        pa.field("features", pa.string(), nullable=False),  # JSON object, sorted keys
        pa.field("created_at", _TS, nullable=False),
    ],
    metadata=_meta(MENTION_LINKS),
)

SNIPPET_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("snippet_id", pa.string(), nullable=False),
        pa.field("doc_id", pa.string(), nullable=False),
        pa.field("revision", pa.int32(), nullable=False),
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("mention_id", pa.string(), nullable=False),
        pa.field("text", pa.large_string()),
        pa.field("locator", _LOCATOR, nullable=False),
        pa.field("window_words", pa.int32(), nullable=False),
        pa.field("stance_score", pa.float64()),
        pa.field("stance_label", pa.string()),
        pa.field("stance_model_id", pa.string()),
        pa.field("pipeline_version", pa.string(), nullable=False),
    ],
    metadata=_meta(SNIPPETS),
)

ENTITY_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("canonical_name", pa.string(), nullable=False),
        pa.field("type", pa.string(), nullable=False),
        pa.field("aliases", pa.list_(pa.string()), nullable=False),
        pa.field("kb_id", pa.string()),
        pa.field("description", pa.string()),
        pa.field("kb_snapshot", pa.string()),
        pa.field("created_at", _TS, nullable=False),
    ],
    metadata=_meta(ENTITIES),
)

SCHEMAS: dict[str, tuple[pa.Schema, type[Any]]] = {
    MENTIONS: (MENTION_SCHEMA, Mention),
    MENTION_LINKS: (MENTION_LINK_SCHEMA, MentionLink),
    SNIPPETS: (SNIPPET_SCHEMA, Snippet),
    ENTITIES: (ENTITY_SCHEMA, Entity),
}


def to_table(name: str, records: Sequence[Any]) -> pa.Table:
    schema, _ = SCHEMAS[name]
    rows = []
    for r in records:
        row = r.model_dump(mode="python")
        if "features" in row:
            row["features"] = json.dumps(row["features"], sort_keys=True)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def from_table(name: str, table: pa.Table) -> list[Any]:
    _, model = SCHEMAS[name]
    out = []
    for row in table.to_pylist():
        if "features" in row:
            row["features"] = json.loads(row["features"])
        out.append(model.model_validate(row))
    return out
