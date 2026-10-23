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

# Table groups. Each group has exactly ONE writer process, holding the group's lock in
# ``state/locks/``; only that process installs, compacts or removes the group's files
# under ``tables/<table>/``. Recovery and compaction are scoped to the caller's group,
# so one writer's startup cleanup can never delete a part another writer is installing.
TABLE_GROUPS: dict[str, tuple[str, ...]] = {
    "documents": (DOCUMENTS,),  # the storage writer
    "enrichment": (  # the Phase 2 enricher
        "segments",
        "mentions",
        "mention_links",
        "snippets",
        "entities",
        "entity_relations",
    ),
}
GROUP_LOCKS = {"documents": "writer.lock", "enrichment": "enricher.lock"}


def group_of(table_name: str) -> str:
    for group, tables in TABLE_GROUPS.items():
        if table_name in tables:
            return group
    raise KeyError(f"table {table_name!r} belongs to no table group")


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


def stage_part(
    tmp: TmpArea,
    table_name: str,
    table: pa.Table,
    min_ingested_at: datetime | None = None,
    max_ingested_at: datetime | None = None,
) -> StagedPart:
    """Write any table's rows as one hot Parquet part in the tmp area and fsync it.
    Install it with ``install_part`` inside the writer's transaction."""
    group_of(table_name)  # unknown tables have no owner and must not be written
    if table.num_rows == 0:
        raise ValueError("refusing to write an empty part")
    file_id = str(ULID())
    tmp_path = tmp.new_path(".parquet")
    with tmp_path.open("xb") as f:
        pq.write_table(table, f, compression=PARQUET_COMPRESSION)
        f.flush()
        os.fsync(f.fileno())
    return StagedPart(
        file_id=file_id,
        table_name=table_name,
        tmp_path=tmp_path,
        rel_path=f"tables/{table_name}/hot/part-{file_id}.parquet",
        row_count=table.num_rows,
        size_bytes=tmp_path.stat().st_size,
        sha256=file_sha256(tmp_path),
        min_ingested_at=min_ingested_at,
        max_ingested_at=max_ingested_at,
    )


def stage_documents_part(tmp: TmpArea, docs: Sequence[Document]) -> StagedPart:
    """Write ``docs`` as one Parquet part in the tmp area and fsync it."""
    if not docs:
        raise ValueError("refusing to write an empty part")
    ingested = [d.ingested_at for d in docs]
    return stage_part(tmp, DOCUMENTS, documents_to_table(docs), min(ingested), max(ingested))


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


def read_revisions(
    conn: sqlite3.Connection,
    data_dir: Path,
    keys: Sequence[tuple[str, int]],
    attempts: int = 3,
) -> dict[tuple[str, int], Document]:
    """Read specific Document revisions through the manifest (``doc_revisions.file_id``).

    The storage writer may compact the part holding a revision between the lookup and
    the read; the old file is then gone, so the lookup is simply redone. Revisions
    the ledger doesn't know are absent from the result.
    """
    wanted = set(keys)
    for attempt in range(attempts):
        by_file: dict[str, set[str]] = {}
        ordered = sorted(wanted)
        for i in range(0, len(ordered), 400):
            chunk = ordered[i : i + 400]
            rows = conn.execute(
                "SELECT d.doc_id, t.rel_path FROM doc_revisions d "
                "JOIN table_files t USING (file_id) "
                f"WHERE (d.doc_id, d.revision) IN (VALUES {','.join(['(?, ?)'] * len(chunk))})",
                [v for key in chunk for v in key],
            )
            for r in rows:
                by_file.setdefault(r["rel_path"], set()).add(r["doc_id"])
        out: dict[tuple[str, int], Document] = {}
        try:
            for rel, doc_ids in by_file.items():
                table = pq.read_table(
                    data_dir / rel,
                    schema=DOCUMENT_SCHEMA,
                    filters=[("doc_id", "in", sorted(doc_ids))],
                )
                for d in table_to_documents(table):
                    if (d.doc_id, d.revision) in wanted:
                        out[(d.doc_id, d.revision)] = d
            return out
        except FileNotFoundError:
            if attempt == attempts - 1:
                raise
    return {}
