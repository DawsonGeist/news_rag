"""Compaction: merge small Parquet parts into monthly parts of about ``target_bytes``.

Owner: the storage writer (it holds the writer lock while compacting).

Each flush writes a small ``hot/`` part. Compaction rewrites, per month of
``ingested_at``, the hot parts plus any undersized monthly parts into
``tables/documents/YYYY/MM/part-<ulid>.parquet``. Durability follows the protocol:

1. stream the input rows into new parts in the tmp area (bounded memory: one input
   part at a time), fsync each;
2. in one transaction: install the new parts, insert their ``table_files`` rows, mark
   the inputs ``superseded``, and repoint every ``doc_revisions`` row to the part
   that now holds it;
3. delete the old files. A crash before this leaves superseded files that startup
   recovery removes, because only ``committed`` files exist.
"""

from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from ulid import ULID

from newsrag.contracts.v1 import SCHEMA_VERSION
from newsrag.storage.fsio import TmpArea, file_sha256, fsync_dir, install
from newsrag.storage.state import transaction
from newsrag.storage.tables import DOCUMENT_SCHEMA, DOCUMENTS, PARQUET_COMPRESSION
from newsrag.timeutil import to_db

TARGET_BYTES = 128 * 1024 * 1024
SMALL_FRACTION = 0.5  # monthly parts under half the target are merged again


@dataclass
class _Input:
    file_id: str
    rel_path: str
    size_bytes: int


@dataclass
class _Output:
    file_id: str
    tmp_path: Path
    rel_path: str
    rows: list[tuple[str, int]] = field(default_factory=list)  # (doc_id, revision)
    min_ingested: datetime | None = None
    max_ingested: datetime | None = None


@dataclass
class CompactionReport:
    months: int = 0
    inputs: int = 0
    outputs: int = 0
    rows: int = 0


def _month(ts: str | None) -> str:
    return (ts or "")[:7]  # "YYYY-MM" from the DB timestamp format


def plan(conn: sqlite3.Connection, target_bytes: int) -> dict[str, list[_Input]]:
    """Month -> parts worth merging. A month with a single part that is already big
    enough (or alone and not hot) is left alone."""
    rows = conn.execute(
        "SELECT file_id, rel_path, size_bytes, min_ingested_at, max_ingested_at "
        "FROM table_files WHERE table_name = ? AND state = 'committed' ORDER BY file_id",
        (DOCUMENTS,),
    ).fetchall()
    by_month: dict[str, list[_Input]] = defaultdict(list)
    for r in rows:
        hot = "/hot/" in r["rel_path"]
        small = r["size_bytes"] < target_bytes * SMALL_FRACTION
        if not (hot or small):
            continue
        # A hot part straddling a month boundary goes to both months; rows are split
        # by ingested_at when reading.
        for month in {_month(r["min_ingested_at"]), _month(r["max_ingested_at"])}:
            by_month[month].append(_Input(r["file_id"], r["rel_path"], r["size_bytes"]))
    out = {}
    for month, parts in by_month.items():
        if len(parts) > 1 or any("/hot/" in p.rel_path for p in parts):
            out[month] = parts
    return out


def _new_output(tmp: TmpArea, month: str) -> _Output:
    file_id = str(ULID())
    year, mon = month.split("-")
    return _Output(
        file_id,
        tmp.new_path(".parquet"),
        f"tables/{DOCUMENTS}/{year}/{mon}/part-{file_id}.parquet",
    )


def _month_rows(table: pa.Table, month: str) -> pa.Table:
    year, mon = (int(x) for x in month.split("-"))
    ts = table["ingested_at"]
    mask = pc.and_(pc.equal(pc.year(ts), year), pc.equal(pc.month(ts), mon))
    return table.filter(mask)


def _write_month(
    data_dir: Path, tmp: TmpArea, month: str, parts: list[_Input], target_bytes: int
) -> list[_Output]:
    outputs: list[_Output] = []
    writer: pq.ParquetWriter | None = None
    current: _Output | None = None
    written = 0

    def close() -> None:
        nonlocal writer
        if writer is not None:
            writer.close()
            with current.tmp_path.open("rb+") as f:  # type: ignore[union-attr]
                os.fsync(f.fileno())
            writer = None

    for part in parts:
        table = _month_rows(pq.read_table(data_dir / part.rel_path, schema=DOCUMENT_SCHEMA), month)
        if table.num_rows == 0:
            continue
        if writer is None or written >= target_bytes:
            close()
            current = _new_output(tmp, month)
            outputs.append(current)
            writer = pq.ParquetWriter(
                current.tmp_path, DOCUMENT_SCHEMA, compression=PARQUET_COMPRESSION
            )
            written = 0
        assert current is not None
        writer.write_table(table)
        # Compressed input size is a fair estimate of the output size.
        written += part.size_bytes
        current.rows.extend(
            zip(table["doc_id"].to_pylist(), table["revision"].to_pylist(), strict=True)
        )
        ingested = table["ingested_at"].to_pylist()
        lo, hi = min(ingested), max(ingested)
        current.min_ingested = lo if current.min_ingested is None else min(current.min_ingested, lo)
        current.max_ingested = hi if current.max_ingested is None else max(current.max_ingested, hi)
    close()
    return outputs


def compact(
    conn: sqlite3.Connection,
    data_dir: Path,
    tmp: TmpArea,
    now: datetime,
    target_bytes: int = TARGET_BYTES,
) -> CompactionReport:
    """Merge small parts month by month. Caller holds the writer lock.

    All months commit in one transaction: a hot part spanning a month boundary feeds
    two months, and committing them separately would, after a crash in between,
    leave its rows in two committed files."""
    report = CompactionReport()
    months = plan(conn, target_bytes)
    if not months:
        return report
    outputs = [
        o
        for month, parts in sorted(months.items())
        for o in _write_month(data_dir, tmp, month, parts, target_bytes)
    ]
    inputs = {p.file_id: p for parts in months.values() for p in parts}
    with transaction(conn):
        for o in outputs:
            install(o.tmp_path, data_dir / o.rel_path)
            conn.execute(
                "INSERT INTO table_files (file_id, table_name, table_schema_version, "
                "rel_path, row_count, size_bytes, sha256, min_ingested_at, "
                "max_ingested_at, state, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'committed', ?)",
                (
                    o.file_id,
                    DOCUMENTS,
                    SCHEMA_VERSION,
                    o.rel_path,
                    len(o.rows),
                    (data_dir / o.rel_path).stat().st_size,
                    file_sha256(data_dir / o.rel_path),
                    to_db(o.min_ingested) if o.min_ingested else None,
                    to_db(o.max_ingested) if o.max_ingested else None,
                    to_db(now),
                ),
            )
            conn.executemany(
                "UPDATE doc_revisions SET file_id = ? WHERE doc_id = ? AND revision = ?",
                [(o.file_id, d, r) for d, r in o.rows],
            )
        conn.executemany(
            "UPDATE table_files SET state = 'superseded', superseded_by = ? "
            "WHERE file_id = ? AND state = 'committed'",
            [(outputs[0].file_id if outputs else None, fid) for fid in inputs],
        )
    for p in inputs.values():
        path = data_dir / p.rel_path
        path.unlink(missing_ok=True)
        fsync_dir(path.parent)
    report.months = len(months)
    report.inputs = len(inputs)
    report.outputs = len(outputs)
    report.rows = sum(len(o.rows) for o in outputs)
    return report
