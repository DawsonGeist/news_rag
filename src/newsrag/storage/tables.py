"""Parquet table files listed in the ``table_files`` manifest. Owner: storage writer.

The Arrow schema is written out by hand rather than generated, so that a change to
the Document model cannot silently change the on-disk format; ``test_tables``
fails if the two drift apart.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from ulid import ULID

from newsrag.contracts.v1 import SCHEMA_VERSION, Document
from newsrag.storage.fsio import TmpArea, file_sha256, install
from newsrag.timeutil import to_db

DOCUMENTS = "documents"
PARQUET_COMPRESSION = "zstd"

_TS = pa.timestamp("us", tz="UTC")

DOCUMENT_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("doc_id", pa.string(), nullable=False),
        pa.field("revision", pa.int32(), nullable=False),
        pa.field("pipeline", pa.string(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("canonical_url", pa.string(), nullable=False),
        pa.field("url_canonicalizer_version", pa.string(), nullable=False),
        pa.field("final_url", pa.string(), nullable=False),
        pa.field("title", pa.string()),
        pa.field("authors", pa.list_(pa.string()), nullable=False),
        pa.field("extracted_text", pa.large_string(), nullable=False),
        pa.field("content_kind", pa.string(), nullable=False),
        pa.field("language", pa.string()),
        pa.field("language_confidence", pa.float64()),
        pa.field("word_count", pa.int32(), nullable=False),
        pa.field("published_at", _TS),
        pa.field("published_at_source", pa.string(), nullable=False),
        pa.field("modified_at", _TS),
        pa.field("discovered_at", _TS, nullable=False),
        pa.field("fetched_at", _TS, nullable=False),
        pa.field("ingested_at", _TS, nullable=False),
        pa.field("feed_id", pa.string()),
        pa.field("feed_title", pa.string()),
        pa.field("feed_summary", pa.string()),
        pa.field("content_hash", pa.string(), nullable=False),
        pa.field("raw_blob_hash", pa.string(), nullable=False),
        pa.field("extractor_version", pa.string(), nullable=False),
        pa.field("pipeline_version", pa.string(), nullable=False),
        pa.field("pipeline_meta", pa.string(), nullable=False),  # JSON object, sorted keys
    ],
    metadata={"newsrag.table": DOCUMENTS, "newsrag.schema_version": SCHEMA_VERSION},
)


def documents_to_table(docs: Sequence[Document]) -> pa.Table:
    rows: list[dict[str, Any]] = []
    for d in docs:
        row = d.model_dump(mode="python")
        row["pipeline_meta"] = json.dumps(d.pipeline_meta, sort_keys=True, ensure_ascii=False)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=DOCUMENT_SCHEMA)


def table_to_documents(table: pa.Table) -> list[Document]:
    docs = []
    for row in table.to_pylist():
        row["pipeline_meta"] = json.loads(row["pipeline_meta"])
        docs.append(Document.model_validate(row))
    return docs


@dataclass(frozen=True)
class StagedPart:
    file_id: str
    table_name: str
    tmp_path: Path
    rel_path: str
    row_count: int
    size_bytes: int
    sha256: str
    min_ingested_at: datetime | None
    max_ingested_at: datetime | None


def stage_documents_part(tmp: TmpArea, docs: Sequence[Document]) -> StagedPart:
    """Write ``docs`` as one Parquet part in the tmp area and fsync it."""
    if not docs:
        raise ValueError("refusing to write an empty part")
    file_id = str(ULID())
    tmp_path = tmp.new_path(".parquet")
    with tmp_path.open("xb") as f:
        pq.write_table(documents_to_table(docs), f, compression=PARQUET_COMPRESSION)
        f.flush()
        os.fsync(f.fileno())
    ingested = [d.ingested_at for d in docs]
    return StagedPart(
        file_id=file_id,
        table_name=DOCUMENTS,
        tmp_path=tmp_path,
        rel_path=f"tables/{DOCUMENTS}/hot/part-{file_id}.parquet",
        row_count=len(docs),
        size_bytes=tmp_path.stat().st_size,
        sha256=file_sha256(tmp_path),
        min_ingested_at=min(ingested),
        max_ingested_at=max(ingested),
    )


def install_part(conn: sqlite3.Connection, data_dir: Path, part: StagedPart, now: datetime) -> None:
    """Move a staged part into ``tables/`` and add its manifest row. Must run inside
    ``transaction(conn)`` so the row commits with the caller's other changes."""
    if not conn.in_transaction:
        raise RuntimeError("install_part must run inside a write transaction")
    install(part.tmp_path, data_dir / part.rel_path)
    conn.execute(
        "INSERT INTO table_files (file_id, table_name, table_schema_version, rel_path, "
        "row_count, size_bytes, sha256, min_ingested_at, max_ingested_at, state, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'committed', ?)",
        (
            part.file_id,
            part.table_name,
            SCHEMA_VERSION,
            part.rel_path,
            part.row_count,
            part.size_bytes,
            part.sha256,
            to_db(part.min_ingested_at) if part.min_ingested_at else None,
            to_db(part.max_ingested_at) if part.max_ingested_at else None,
            to_db(now),
        ),
    )


def committed_files(conn: sqlite3.Connection, data_dir: Path, table_name: str) -> list[Path]:
    """The files that make up ``table_name``. Readers must use this, never a directory glob."""
    rows = conn.execute(
        "SELECT rel_path FROM table_files WHERE table_name = ? AND state = 'committed' "
        "ORDER BY file_id",
        (table_name,),
    )
    return [data_dir / r[0] for r in rows]


def read_documents(paths: Sequence[Path]) -> list[Document]:
    docs: list[Document] = []
    for p in paths:
        docs.extend(table_to_documents(pq.read_table(p, schema=DOCUMENT_SCHEMA)))
    return docs
