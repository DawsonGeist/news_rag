"""Linking test-set tooling: scoring, weight fitting, page and import round trip."""

import random
from pathlib import Path

from newsrag.evaluation.linking import (
    Example,
    Item,
    cross_validate,
    fit,
    import_labels,
    read_gold,
    review_page,
    score,
)
from newsrag.link.linker import WEIGHTS


def synthetic(n: int, seed: int = 0) -> list[Example]:
    """The gold entity has higher context; popularity is a distractor."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        gold = f"wd:Q{i}"
        cands = [
            (
                gold,
                {
                    "prior": rng.uniform(0, 0.5),
                    "type": 1.0,
                    "label": 1.0,
                    "context": rng.uniform(0.6, 1.0),
                    "coherence": 0.0,
                },
            ),
            (
                f"wd:X{i}",
                {
                    "prior": rng.uniform(0.5, 1.0),
                    "type": 1.0,
                    "label": 1.0,
                    "context": rng.uniform(0.0, 0.5),
                    "coherence": 0.0,
                },
            ),
        ]
        out.append(Example(gold if i % 5 else "none", cands))
    return out


def test_fit_learns_to_prefer_context() -> None:
    exs = synthetic(200)
    w = fit(exs)
    assert w["context"] > w["prior"]
    fitted = score(exs, w, 0.5)
    assert fitted.precision > 0.75 and fitted.linked > 0
    cv = cross_validate(exs, 0.5)
    assert cv.precision > 0.7


def test_score_counts_only_links_above_threshold() -> None:
    exs = synthetic(20)
    assert score(exs, WEIGHTS, 1.01).linked == 0


def test_page_and_import_round_trip(tmp_path: Path) -> None:
    item = Item(
        doc_id="a" * 32,
        revision=1,
        source_id="example.com",
        field="body",
        start=4,
        end=16,
        surface="Rosa Delgado",
        ner_label="person",
        context="Gov. [[Rosa Delgado]] said",
        candidates=[
            {
                "entity_id": "wd:Q1001",
                "label": "Rosa Delgado",
                "description": None,
                "type": "person",
                "sitelinks": 3,
                "confidence": 0.7,
            }
        ],
        predicted="wd:Q1001",
        proposed="wd:Q1001",
    )
    page = review_page([item])
    assert "Rosa Delgado" in page and "__DATA__" not in page
    out = tmp_path / "linking.yaml"
    import_labels([item], {f"{'a' * 32}:1:body:4:16": "wd:Q1001"}, out)
    gold = read_gold(out)
    assert gold[0].gold == "wd:Q1001" and gold[0].surface == "Rosa Delgado"
    assert "said" not in out.read_text()  # no article text in the committed file
