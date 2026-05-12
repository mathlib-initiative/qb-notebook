"""Load mathlib community team membership from the leanprover-community website.

The canonical source is the ``leanprover-community.github.io`` repo, which
keeps two YAML files under ``data/``:

- ``people.yaml`` — list of ``{name, github, descr, img}`` entries.
- ``teams.yaml`` — list of ``{name, members: [full name, ...], ...}`` entries.

The join is ``teams[*].members`` (full name) → ``people[*].name`` →
``people[*].github``. We surface team membership as sets of GitHub logins
(lowercased) so downstream analyses can intersect them with
``actor_login`` columns from the syncer.

The two relevant teams for review analytics are:

- **Maintainer team** (~30 people) — can trigger the final bors merge.
- **Mathlib reviewers** (~50 people) — can apply the ``maintainer-merge``
  label that flags a PR as approved.

Run as a script to dump a JSON snapshot:

    python -m qb_notebook.teams --repo ../leanprover-community.github.io \\
        --output data/teams.json
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping

import yaml

logger = logging.getLogger(__name__)


TEAM_KEY_ALIASES: Mapping[str, str] = {
    "Maintainer team": "maintainers",
    "Mathlib reviewers": "reviewers",
    "Admin team": "admins",
    "Continuous integration": "ci",
    "Code of conduct": "code_of_conduct",
    "Website/blog": "website",
}


@dataclass(frozen=True)
class Teams:
    """Snapshot of team membership keyed by short team name.

    ``by_team`` maps the short alias (e.g. ``"reviewers"``) to the set of
    GitHub logins. Logins are lowercased for case-insensitive comparison
    against ``actor_login`` columns (GitHub login matching is
    case-insensitive on apply, but the syncer preserves the casing the
    user signed up with).

    ``unmatched`` lists ``(team_name, full_name)`` pairs for team members
    that did not have a matching entry in ``people.yaml`` — surfaced so
    callers can decide whether to warn or fail.
    """

    by_team: Mapping[str, frozenset[str]]
    name_to_login: Mapping[str, str]
    unmatched: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __getitem__(self, key: str) -> frozenset[str]:
        return self.by_team[key]

    def get(self, key: str, default: frozenset[str] | None = None) -> frozenset[str]:
        if default is None:
            default = frozenset()
        return self.by_team.get(key, default)

    @property
    def reviewers(self) -> frozenset[str]:
        return self.get("reviewers")

    @property
    def maintainers(self) -> frozenset[str]:
        return self.get("maintainers")

    @property
    def admins(self) -> frozenset[str]:
        return self.get("admins")

    def all_known(self) -> frozenset[str]:
        """Union of every team's membership — useful as a coarse filter."""
        out: set[str] = set()
        for members in self.by_team.values():
            out |= members
        return frozenset(out)


def load(
    repo_path: str | Path,
    *,
    team_aliases: Mapping[str, str] = TEAM_KEY_ALIASES,
    warn_on_unmatched: bool = True,
) -> Teams:
    """Load team membership from a ``leanprover-community.github.io`` checkout.

    ``repo_path`` should point at the repo root; ``data/people.yaml`` and
    ``data/teams.yaml`` are read relative to it. Team names that don't
    appear in ``team_aliases`` are still loaded under their raw name (with
    spaces collapsed to underscores and lowercased) so callers can opt in
    to additional teams without editing this module.
    """
    repo = Path(repo_path)
    people_path = repo / "data" / "people.yaml"
    teams_path = repo / "data" / "teams.yaml"

    name_to_login = _load_people(people_path)
    by_team, unmatched = _load_teams(teams_path, name_to_login, team_aliases)

    if warn_on_unmatched and unmatched:
        for team, name in unmatched:
            logger.warning(
                "team %r lists %r but people.yaml has no matching entry",
                team,
                name,
            )

    return Teams(
        by_team=by_team,
        name_to_login=name_to_login,
        unmatched=tuple(unmatched),
    )


def _load_people(path: Path) -> dict[str, str]:
    raw = yaml.safe_load(path.read_text()) or []
    out: dict[str, str] = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        login = entry.get("github")
        if name and login:
            out[name] = str(login).lower()
    return out


def _load_teams(
    path: Path,
    name_to_login: Mapping[str, str],
    team_aliases: Mapping[str, str],
) -> tuple[dict[str, frozenset[str]], list[tuple[str, str]]]:
    raw = yaml.safe_load(path.read_text()) or []
    by_team: dict[str, frozenset[str]] = {}
    unmatched: list[tuple[str, str]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        team_name = entry.get("name")
        members = entry.get("members") or []
        if not team_name or not isinstance(members, Iterable):
            continue
        key = team_aliases.get(team_name, _slugify(team_name))
        logins: set[str] = set()
        for member in members:
            login = name_to_login.get(member)
            if login is None:
                unmatched.append((team_name, str(member)))
                continue
            logins.add(login)
        by_team[key] = frozenset(logins)
    return by_team, unmatched


def _slugify(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name.lower()).strip("_")


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--repo",
        required=True,
        help="Path to a leanprover-community.github.io checkout.",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Path to write the JSON snapshot to (default: stdout).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    teams = load(args.repo)
    payload = {
        "by_team": {k: sorted(v) for k, v in teams.by_team.items()},
        "unmatched": [list(pair) for pair in teams.unmatched],
    }
    text = json.dumps(payload, indent=2, sort_keys=True)
    if args.output == "-":
        print(text)
    else:
        Path(args.output).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
