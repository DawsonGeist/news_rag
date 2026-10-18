from newsrag.testing.killtest import run_kill_test
from newsrag.testing.pipeline_killtest import run_pipeline_kill_test


def test_storage_kill_test_short() -> None:
    """A few iterations here; `newsrag test kill --storage-only` runs the full 20."""
    assert run_kill_test(iterations=3, kills_per_iteration=2, seed=7) == []


def test_pipeline_kill_test_short() -> None:
    """One iteration of the full `run all` pipeline; `newsrag test kill` runs 20 (G1.4)."""
    assert run_pipeline_kill_test(iterations=1, kills_per_iteration=2, seed=3) == []
