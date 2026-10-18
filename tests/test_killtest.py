from newsrag.testing.killtest import run_kill_test


def test_kill_test_short() -> None:
    """A few iterations here; `newsrag test kill` runs the full 20 for Gate 1."""
    assert run_kill_test(iterations=3, kills_per_iteration=2, seed=7) == []
