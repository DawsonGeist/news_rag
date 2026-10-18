"""The one way every stage records a StageEvent (contracts/v1/README.md, "Stage events")."""

from __future__ import annotations

import os
import socket
import sqlite3
from datetime import datetime

from ulid import ULID

from newsrag.contracts.v1 import Stage, StageEvent, StageStatus
from newsrag.timeutil import to_db


def worker_id(component: str, task: int = 0) -> str:
    """``host:component-pid:task``, e.g. ``tower2:writer-4121:0``."""
    return f"{socket.gethostname()}:{component}-{os.getpid()}:{task}"


def new_event(
    stage: Stage,
    status: StageStatus,
    *,
    worker: str,
    started_at: datetime,
    finished_at: datetime,
    pipeline_version: str,
    **fields: object,
) -> StageEvent:
    return StageEvent.model_validate(
        {
            "event_id": str(ULID()),
            "stage": stage,
            "status": status,
            "worker_id": worker,
            "started_at": started_at,
            "finished_at": finished_at,
            "pipeline_version": pipeline_version,
            **fields,
        }
    )


def record(conn: sqlite3.Connection, events: list[StageEvent]) -> None:
    """Append events. Call inside the transaction that commits the work they describe,
    so an event exists exactly when its effect does."""
    conn.executemany(
        "INSERT INTO stage_events (event_id, stage, status, url_id, doc_id, revision, feed_id, "
        "worker_id, attempt, enqueued_at, started_at, finished_at, error_code, detail, "
        "pipeline_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                e.event_id,
                e.stage.value,
                e.status.value,
                e.url_id,
                e.doc_id,
                e.revision,
                e.feed_id,
                e.worker_id,
                e.attempt,
                to_db(e.enqueued_at) if e.enqueued_at else None,
                to_db(e.started_at),
                to_db(e.finished_at),
                e.error_code,
                e.detail,
                e.pipeline_version,
            )
            for e in events
        ],
    )
