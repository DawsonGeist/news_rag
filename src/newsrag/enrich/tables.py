"""The enricher's derived Parquet tables (table group ``enrichment``).

Like ``DOCUMENT_SCHEMA``, the Arrow schemas are written out by hand so that a model change
cannot silently change the on-disk format; tests fail if the two drift apart.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from newsrag.contracts.v1 import SCHEMA_VERSION, Segment

SEGMENTS = "segments"

SEGMENT_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("segment_id", pa.string(), nullable=False),
        pa.field("doc_id", pa.string(), nullable=False),
        pa.field("revision", pa.int32(), nullable=False),
        pa.field("level", pa.string(), nullable=False),
        pa.field("parent_id", pa.string()),
        pa.field("position", pa.int32(), nullable=False),
        pa.field("text", pa.large_string(), nullable=False),
        pa.field(
            "locator",
            pa.struct(
                [
                    pa.field("kind", pa.string(), nullable=False),
                    pa.field("field", pa.string(), nullable=False),
                    pa.field("start", pa.int32(), nullable=False),
                    pa.field("end", pa.int32(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field("chunker_version", pa.string(), nullable=False),
        pa.field("pipeline_version", pa.string(), nullable=False),
    ],
    metadata={"newsrag.table": SEGMENTS, "newsrag.schema_version": SCHEMA_VERSION},
)


def segments_to_table(segments: Sequence[Segment]) -> pa.Table:
    return pa.Table.from_pylist(
        [s.model_dump(mode="json") for s in segments], schema=SEGMENT_SCHEMA
    )


def table_to_segments(table: pa.Table) -> list[Segment]:
    return [Segment.model_validate(row) for row in table.to_pylist()]
