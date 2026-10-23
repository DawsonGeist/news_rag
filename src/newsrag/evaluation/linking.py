"""The entity-linking test set (phase2-design.md, "Evaluation"; Gate 2, G2.2).

Workflow (Claude proposes, the author verifies, because the labels are ground truth):

1. ``sample``: run the entity stage over documents from many publishers and pick named
   mentions across entity types. Writes ``data/eval/linking-review.jsonl`` with each
   mention's context and its candidates. It stays local: it holds article text.
2. Claude fills in ``proposed`` for each item (``wd:Q…``, ``none`` = not in Wikidata,
   ``ner_error`` = not a name or a wrong span); ``review_page`` renders the items as a
   local HTML page where the author confirms or corrects each and exports the labels.
3. ``import_labels`` writes ``eval/linking.yaml`` (committed): document IDs, offsets,
   the short name and the gold label. No article text.
4. ``evaluate`` re-runs the stage on those documents and reports precision and recall
   of Wikidata links at a threshold; ``fit`` fits the linker's weights to the labels.
"""

from __future__ import annotations

import json
import math
import random
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from newsrag.contracts.v1 import Document, TextLocator
from newsrag.kb.index import KbIndex
from newsrag.link.linker import WEIGHTS, confidence
from newsrag.link.stage import DocEntities

GOLD_NONE, GOLD_NER_ERROR = "none", "ner_error"
FEATURES = [k for k in WEIGHTS if k != "bias"]


@dataclass(frozen=True)
class Item:
    doc_id: str
    revision: int
    source_id: str
    field: str
    start: int
    end: int
    surface: str
    ner_label: str
    context: str  # local review only, never committed
    candidates: list[dict[str, Any]]
    predicted: str  # the linker's entity id (wd:… or local:…)
    proposed: str | None = None


def _context(doc: Document, loc: TextLocator, width: int = 220) -> str:
    text = doc.title or "" if loc.field == "title" else doc.extracted_text
    lo, hi = max(0, loc.start - width), min(len(text), loc.end + width)
    return f"{text[lo : loc.start]}[[{text[loc.start : loc.end]}]]{text[loc.end : hi]}"


def sample_items(
    docs: Sequence[Document],
    results: Sequence[DocEntities],
    kb: KbIndex,
    n: int,
    seed: int,
    per_doc: int = 2,
) -> list[Item]:
    """Up to ``per_doc`` named mentions per document (one per name), balanced across
    entity types by round-robin."""
    rng = random.Random(seed)
    by_type: dict[str, list[tuple[Document, Any, Any]]] = defaultdict(list)
    for doc, res in zip(docs, results, strict=True):
        group_of = {mid: g for g in res.groups for mid in g.group.mention_ids}
        picked = [m for m in res.mentions if m.ner_label != "coref" and m.mention_id in group_of]
        rng.shuffle(picked)
        seen: set[int] = set()
        taken = 0
        for m in picked:
            g = group_of[m.mention_id]
            if id(g) in seen or taken >= per_doc:
                continue
            seen.add(id(g))
            taken += 1
            by_type[m.ner_label].append((doc, m, g))
    for v in by_type.values():
        rng.shuffle(v)
    chosen: list[tuple[Document, Any, Any]] = []
    while len(chosen) < n and any(by_type.values()):
        for label in sorted(by_type):
            if by_type[label] and len(chosen) < n:
                chosen.append(by_type[label].pop())
    ents = kb.entities(s.entity_id for _, _, g in chosen for s in g.scored[:6])
    items = []
    for doc, m, g in chosen:
        loc = m.locator
        cands = []
        for s in g.scored[:6]:
            e = ents.get(s.entity_id)
            if e is not None:
                cands.append(
                    {
                        "entity_id": s.entity_id,
                        "label": e.label,
                        "description": e.description,
                        "type": e.type,
                        "sitelinks": e.sitelinks,
                        "confidence": round(s.confidence, 3),
                    }
                )
        items.append(
            Item(
                doc_id=doc.doc_id,
                revision=doc.revision,
                source_id=doc.source_id,
                field=loc.field,
                start=loc.start,
                end=loc.end,
                surface=m.surface,
                ner_label=m.ner_label,
                context=_context(doc, loc),
                candidates=cands,
                predicted=g.entity_id,
            )
        )
    return items


def write_items(path: Path, items: Sequence[Item]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for it in items:
            f.write(json.dumps(it.__dict__, ensure_ascii=False) + "\n")


def read_items(path: Path) -> list[Item]:
    return [Item(**json.loads(line)) for line in path.read_text().splitlines() if line.strip()]


def review_page(items: Sequence[Item]) -> str:
    """A self-contained page: one card per mention with the proposal preselected. A
    label counts only once the author picks or confirms it (no bulk accept: these are
    the ground truth). Choices are kept in the browser and exported as labels.json."""
    data = json.dumps(
        [
            {
                "key": f"{it.doc_id}:{it.revision}:{it.field}:{it.start}:{it.end}",
                "surface": it.surface,
                "ner": it.ner_label,
                "source": it.source_id,
                "context": it.context,
                "candidates": it.candidates,
                "proposed": it.proposed,
            }
            for it in items
        ],
        ensure_ascii=False,
    ).replace("</", "<\\/")
    page = resources.files("newsrag.evaluation").joinpath("linking_review.html").read_text()
    return page.replace("__DATA__", data)


@dataclass(frozen=True)
class Gold:
    doc_id: str
    revision: int
    field: str
    start: int
    end: int
    surface: str
    ner_label: str
    gold: str


def import_labels(items: Sequence[Item], labels: dict[str, str], out: Path) -> list[Gold]:
    """Merge the author's exported choices into ``eval/linking.yaml`` (no article text)."""
    golds = []
    for it in items:
        key = f"{it.doc_id}:{it.revision}:{it.field}:{it.start}:{it.end}"
        if key not in labels:
            continue
        golds.append(
            Gold(
                it.doc_id,
                it.revision,
                it.field,
                it.start,
                it.end,
                it.surface,
                it.ner_label,
                labels[key],
            )
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        "# Entity-linking test set (G2.2). Author-verified; see evaluation/linking.py.\n"
        "# gold: wd:<QID> | none (not in Wikidata) | ner_error (not a name / wrong span)\n"
        + yaml.safe_dump([g.__dict__ for g in golds], sort_keys=False, allow_unicode=True)
    )
    return golds


def read_gold(path: Path) -> list[Gold]:
    return [Gold(**g) for g in yaml.safe_load(path.read_text()) or []]


# --- scoring and fitting ------------------------------------------------------------------


@dataclass
class Example:
    gold: str
    candidates: list[tuple[str, dict[str, float]]]  # (entity_id, features)


def examples(golds: Sequence[Gold], results: dict[tuple[str, int], DocEntities]) -> list[Example]:
    """Pair each gold mention with the stage's candidates for the overlapping mention."""
    out = []
    for g in golds:
        if g.gold == GOLD_NER_ERROR:
            continue
        res = results.get((g.doc_id, g.revision))
        if res is None:
            continue
        group_of = {mid: gl for gl in res.groups for mid in gl.group.mention_ids}
        match = None
        for m in res.mentions:
            loc = m.locator
            if (
                isinstance(loc, TextLocator)
                and loc.field == g.field
                and loc.start < g.end
                and g.start < loc.end
                and m.mention_id in group_of
            ):
                match = group_of[m.mention_id]
                break
        cands = [(s.entity_id, s.features) for s in match.scored] if match else []
        out.append(Example(g.gold, cands))
    return out


@dataclass
class LinkScore:
    threshold: float
    precision: float
    recall: float
    linked: int
    correct: int
    gold_in_kb: int


def score(exs: Sequence[Example], weights: dict[str, float], threshold: float) -> LinkScore:
    linked = correct = 0
    in_kb = sum(e.gold.startswith("wd:") for e in exs)
    for e in exs:
        if not e.candidates:
            continue
        best = max(e.candidates, key=lambda c: confidence(c[1], weights))
        if confidence(best[1], weights) >= threshold:
            linked += 1
            correct += best[0] == e.gold
    return LinkScore(
        threshold,
        correct / linked if linked else 0.0,
        correct / in_kb if in_kb else 0.0,
        linked,
        correct,
        in_kb,
    )


def fit(exs: Sequence[Example], l2: float = 0.01, steps: int = 3000) -> dict[str, float]:
    """Logistic regression over (candidate features -> is the gold entity), by gradient
    descent. Mentions whose gold is ``none`` contribute only negatives."""
    xs, ys = [], []
    for e in exs:
        for eid, f in e.candidates:
            xs.append([1.0] + [f.get(k, 0.0) for k in FEATURES])
            ys.append(1.0 if eid == e.gold else 0.0)
    if not xs:
        return dict(WEIGHTS)
    x, y = np.array(xs), np.array(ys)
    w = np.array([WEIGHTS["bias"]] + [WEIGHTS[k] for k in FEATURES], dtype=float)
    for _ in range(steps):
        p = 1 / (1 + np.exp(-(x @ w)))
        grad = x.T @ (p - y) / len(y) + l2 * np.r_[0.0, w[1:]]
        w -= 1.0 * grad
    return {"bias": float(w[0]), **{k: float(v) for k, v in zip(FEATURES, w[1:], strict=True)}}


def cross_validate(
    exs: Sequence[Example], threshold: float, folds: int = 5, seed: int = 0
) -> LinkScore:
    """Precision/recall with weights fitted on the other folds (honest on a small set)."""
    idx = list(range(len(exs)))
    random.Random(seed).shuffle(idx)
    linked = correct = in_kb = 0
    for k in range(folds):
        test = [exs[i] for i in idx[k::folds]]
        train = [exs[i] for j, i in enumerate(idx) if j % folds != k]
        s = score(test, fit(train), threshold)
        linked, correct, in_kb = linked + s.linked, correct + s.correct, in_kb + s.gold_in_kb
    return LinkScore(
        threshold,
        correct / linked if linked else 0.0,
        correct / in_kb if in_kb else 0.0,
        linked,
        correct,
        in_kb,
    )


def summary(golds: Sequence[Gold]) -> dict[str, Any]:
    return {
        "mentions": len(golds),
        "gold": dict(Counter("wd" if g.gold.startswith("wd:") else g.gold for g in golds)),
        "types": dict(Counter(g.ner_label for g in golds)),
    }


def is_finite(w: dict[str, float]) -> bool:
    return all(math.isfinite(v) for v in w.values())
