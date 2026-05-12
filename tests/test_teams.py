import logging
from pathlib import Path

import pytest

from qb_notebook.teams import Teams, load


PEOPLE_YAML = """\
- name: Alice Example
  github: alice
- name: Bob Example
  github: BobUpper
- name: Carol Example
  github: carol
- name: Dave Nogh
  descr: someone with no github handle
"""

TEAMS_YAML = """\
- name: Maintainer team
  members:
    - Alice Example
    - Bob Example
- name: Mathlib reviewers
  members:
    - Alice Example
    - Bob Example
    - Carol Example
    - Ghost Person
- name: Some New Team
  members:
    - Carol Example
"""


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    data = tmp_path / "data"
    data.mkdir()
    (data / "people.yaml").write_text(PEOPLE_YAML)
    (data / "teams.yaml").write_text(TEAMS_YAML)
    return tmp_path


def test_load_maps_team_names_to_aliases(repo: Path) -> None:
    teams = load(repo)
    assert teams.maintainers == frozenset({"alice", "bobupper"})
    assert teams.reviewers == frozenset({"alice", "bobupper", "carol"})


def test_unaliased_team_uses_slugified_key(repo: Path) -> None:
    teams = load(repo)
    assert "some_new_team" in teams.by_team
    assert teams.by_team["some_new_team"] == frozenset({"carol"})


def test_unmatched_members_are_collected(repo: Path) -> None:
    teams = load(repo)
    assert ("Mathlib reviewers", "Ghost Person") in teams.unmatched


def test_unmatched_members_emit_warning(
    repo: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="qb_notebook.teams"):
        load(repo)
    assert any("Ghost Person" in rec.message for rec in caplog.records)


def test_warn_on_unmatched_can_be_disabled(
    repo: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="qb_notebook.teams"):
        load(repo, warn_on_unmatched=False)
    assert not any("Ghost Person" in rec.message for rec in caplog.records)


def test_all_known_is_union(repo: Path) -> None:
    teams = load(repo)
    assert teams.all_known() == frozenset({"alice", "bobupper", "carol"})


def test_get_returns_empty_set_by_default(repo: Path) -> None:
    teams = load(repo)
    assert teams.get("nonexistent") == frozenset()


def test_empty_yaml_files(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "people.yaml").write_text("")
    (data / "teams.yaml").write_text("")
    teams = load(tmp_path)
    assert isinstance(teams, Teams)
    assert teams.by_team == {}
    assert teams.unmatched == ()


def test_people_entries_without_github_handle_are_skipped(repo: Path) -> None:
    teams = load(repo)
    # Dave Nogh has no github handle, so any team listing him would be
    # unmatched; he is not in the team fixtures here, so the only
    # expectation is that he doesn't appear under any login.
    assert "dave" not in teams.all_known()
    assert "dave nogh" not in teams.name_to_login
