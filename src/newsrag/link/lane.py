"""The enricher's second queue: entities for revisions whose vectors exist.

Its pipeline version names the KB it links against (``ents-1+20261019.kb-3``), so a
new snapshot re-links the archive, newest first, without re-embedding anything.

Per micro-batch: claim revisions the vector queue has finished with; run the entity
stage; tag the revision's points with its entity IDs (idempotent ``set_payload``); then
in one transaction install the mentions, mention links, snippets and local entities
parts, mark the revisions done (recording ``entity_ids``, which the vector queue copies
into any points it writes later) and record ner and link events.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from newsrag.config import Settings
from newsrag.contracts.v1 import Document, Segment, Stage, StageEvent, StageStatus
from newsrag.enrich.embedder import Embedder
from newsrag.enrich.enricher import PIPELINE_VERSION as VECTORS_VERSION
from newsrag.enrich.enricher import BatchReport, ineligible_reason
from newsrag.enrich.state import EnrichClaim, claim, queue_new
from newsrag.enrich.vectors import VectorIndex
from newsrag.link.stage import DocEntities, EntityStage
from newsrag.link.tables import ENTITIES, MENTION_LINKS, MENTIONS, SNIPPETS, to_table
from newsrag.obs.events import new_event, record
from newsrag.segment.segmenter import segment
from newsrag.storage.fsio import TmpArea
from newsrag.storage.state import transaction
from newsrag.storage.tables import install_part, read_revisions, stage_part
from newsrag.timeutil import to_db, utcnow

log = logging.getLogger(__name__)

ENTITY_PIPELINE = "ents-1"


def entity_pipeline_version(kb_dir: Path) -> str:
    """``ents-1/link-1+<snapshot>.<kb_version>``: a new NER rule set, linker (weights) or
    KB each make a new version, and so a re-link of the archive, newest first."""
    from newsrag.link.linker import LINKER_VERSION

    meta = json.loads((kb_dir / "meta.json").read_text())
    return f"{ENTITY_PIPELINE}/{LINKER_VERSION}+{kb_dir.name}.{meta['kb_version']}"


def latest_kb(data_dir: Path) -> Path | None:
    """The newest snapshot with a built lookup index, or None."""
    root = data_dir / "kb"
    if not root.exists():
        return None
    snaps = sorted(
        p for p in root.iterdir() if (p / "kb.duckdb").exists() and (p / "meta.json").exists()
    )
    return snaps[-1] if snaps else None


def build_stage(
    settings: Settings, embedder: Embedder, index: VectorIndex
) -> tuple[EntityStage, str] | None:
    """The entity stage with the real models against the newest indexed KB snapshot, and
    its pipeline version; None if there's no snapshot yet."""
    from newsrag.kb.index import KbIndex
    from newsrag.link.coref import FastCoref
    from newsrag.link.ner import GlinerNer
    from newsrag.link.stage import VectorSource

    kb_dir = latest_kb(settings.data_dir)
    if kb_dir is None:
        return None
    version = entity_pipeline_version(kb_dir)
    stage = EntityStage(
        GlinerNer(settings.enricher.device),
        FastCoref(settings.enricher.device),
        KbIndex(kb_dir),
        embedder,
        VectorSource(embedder, index.vectors),
        version,
    )
    return stage, version


@dataclass
class _Outcome:
    claim: EnrichClaim
    state: str
    result: DocEntities | None = None
    detail: str | None = None


@dataclass
class EntityLane:
    settings: Settings
    conn: sqlite3.Connection
    tmp: TmpArea
    stage: EntityStage
    index: VectorIndex
    worker: str
    pipeline_version: str
    watermark: int = 0
    batch_docs: int = 64
    _states: dict[str, int] = field(default_factory=dict)

    def step(self) -> BatchReport:
        now = utcnow()
        self.watermark = queue_new(self.conn, self.pipeline_version, self.watermark, now)
        claims = claim(
            self.conn,
            self.pipeline_version,
            self.worker,
            self.batch_docs,
            self.settings.enricher.lease_s,
            now,
            after=VECTORS_VERSION,
        )
        return self.process(claims) if claims else BatchReport()

    def _vector_states(self, claims: Sequence[EnrichClaim]) -> dict[tuple[str, int], str]:
        out = {}
        for c in claims:
            row = self.conn.execute(
                "SELECT state FROM enrichments WHERE doc_id = ? AND revision = ? "
                "AND pipeline_version = ?",
                (c.doc_id, c.revision, VECTORS_VERSION),
            ).fetchone()
            out[(c.doc_id, c.revision)] = row["state"] if row else "missing"
        return out

    def process(self, claims: list[EnrichClaim]) -> BatchReport:
        t_start = utcnow()
        docs = read_revisions(
            self.conn, self.settings.data_dir, [(c.doc_id, c.revision) for c in claims]
        )
        vstate = self._vector_states(claims)
        outcomes: list[_Outcome] = []
        work: list[tuple[_Outcome, Document, list[Segment]]] = []
        for c in claims:
            doc = docs.get((c.doc_id, c.revision))
            vs = vstate[(c.doc_id, c.revision)]
            if doc is None:
                outcomes.append(_Outcome(c, "failed", detail="document_missing"))
            elif vs != "done":
                outcomes.append(_Outcome(c, "skipped", detail=f"no vectors ({vs})"))
            elif (reason := ineligible_reason(doc)) is not None:
                outcomes.append(_Outcome(c, "skipped", detail=reason))
            else:
                o = _Outcome(c, "done")
                outcomes.append(o)
                work.append((o, doc, segment(doc, self.pipeline_version)))

        results = self.stage.run([w[1] for w in work], [w[2] for w in work], t_start)
        for (o, _, _), r in zip(work, results, strict=True):
            o.result = r
        t_linked = utcnow()

        parts = []
        for name, attr in (
            (MENTIONS, "mentions"),
            (MENTION_LINKS, "links"),
            (SNIPPETS, "snippets"),
            (ENTITIES, "entities"),
        ):
            records = [x for r in results for x in getattr(r, attr)]
            if records:
                parts.append(stage_part(self.tmp, name, to_table(name, records)))
        for o, _, _ in work:
            assert o.result is not None
            self.index.set_entities(o.claim.doc_id, o.claim.revision, o.result.entity_ids)
        t_tagged = utcnow()

        with transaction(self.conn):
            now = utcnow()
            for part in parts:
                install_part(self.conn, self.settings.data_dir, part, now)
            ts = to_db(now)
            for o in outcomes:
                ent = json.dumps(o.result.entity_ids) if o.result is not None else None
                self.conn.execute(
                    "UPDATE enrichments SET state = ?, lease_owner = NULL, "
                    "lease_expires_at = NULL, last_error = ?, finished_at = ?, updated_at = ?, "
                    "entity_ids = ? "
                    "WHERE doc_id = ? AND revision = ? AND pipeline_version = ?",
                    (
                        o.state,
                        o.detail,
                        ts,
                        ts,
                        ent,
                        o.claim.doc_id,
                        o.claim.revision,
                        self.pipeline_version,
                    ),
                )
            record(self.conn, self._events(outcomes, t_start, t_linked, t_tagged))

        report = BatchReport(claimed=len(claims))
        for o in outcomes:
            report.states[o.state] = report.states.get(o.state, 0) + 1
        report.vectors = sum(len(o.result.mentions) for o in outcomes if o.result)  # mentions
        report.seconds = {
            "ner+link": (t_linked - t_start).total_seconds(),
            "tag": (t_tagged - t_linked).total_seconds(),
        }
        return report

    def _events(
        self,
        outcomes: list[_Outcome],
        t_start: datetime,
        t_linked: datetime,
        t_tagged: datetime,
    ) -> list[StageEvent]:
        events = []
        for o in outcomes:
            common: dict[str, Any] = {
                "worker": self.worker,
                "pipeline_version": self.pipeline_version,
                "doc_id": o.claim.doc_id,
                "revision": o.claim.revision,
                "attempt": o.claim.attempt,
            }
            if o.state != "done":
                events.append(
                    new_event(
                        Stage.NER,
                        StageStatus.SKIPPED if o.state == "skipped" else StageStatus.ERROR,
                        started_at=t_start,
                        finished_at=t_linked,
                        enqueued_at=o.claim.ingested_at,
                        detail=o.detail,
                        **common,
                    )
                )
                continue
            events.append(
                new_event(
                    Stage.NER,
                    StageStatus.OK,
                    started_at=t_start,
                    finished_at=t_linked,
                    enqueued_at=o.claim.ingested_at,
                    **common,
                )
            )
            events.append(
                new_event(
                    Stage.LINK,
                    StageStatus.OK,
                    started_at=t_linked,
                    finished_at=t_tagged,
                    enqueued_at=t_linked,
                    **common,
                )
            )
        return events
