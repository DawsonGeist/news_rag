from pathlib import Path

import pytest

from newsrag.config import Settings, load_settings

REPO = Path(__file__).resolve().parents[1]


def test_example_config_matches_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NEWSRAG_DATA_DIR", raising=False)
    loaded = load_settings(REPO / "config" / "newsrag.example.toml")
    defaults = Settings()
    assert loaded.model_dump(exclude={"writer"}) == defaults.model_dump(exclude={"writer"})


def test_env_overrides_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("NEWSRAG_DATA_DIR", str(tmp_path))
    settings = load_settings(REPO / "config" / "newsrag.example.toml")
    assert settings.state_db == tmp_path / "state" / "crawl.sqlite3"
