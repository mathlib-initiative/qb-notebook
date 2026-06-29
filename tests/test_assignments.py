from datetime import datetime, timezone

import polars as pl

from qb_notebook.assignments import (
    ASSIGNMENT_BOT_ACTORS,
    assignment_policy_outcome,
    assignment_responses,
    classify_assignment_events,
    classify_unassign_events,
    pr_assignees_ever,
    review_request_responses,
)


def _events(rows: list[dict]) -> pl.DataFrame:
    """Build an events frame matching the syncer_prtimelineevent schema."""
    return pl.DataFrame(
        rows,
        schema={
            "id": pl.Int64,
            "pull_request_id": pl.Int64,
            "occurred_at": pl.Datetime("us", "UTC"),
            "type": pl.String,
            "actor_login": pl.String,
            "assignee_login": pl.String,
            "requested_reviewer_login": pl.String,
            "requested_team_slug": pl.String,
        },
    )


def _attr(rows: list[dict]) -> pl.DataFrame:
    """Build an attribute_label_events-shaped frame."""
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "label_at": pl.Datetime("us", "UTC"),
            "inferred_actor": pl.String,
        },
    )


def _dt(day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, minute, tzinfo=timezone.utc)


def _row(
    *,
    id: int,
    pr: int,
    when: datetime,
    type: str,
    actor: str | None = None,
    assignee: str | None = None,
    reviewer: str | None = None,
    team: str | None = None,
) -> dict:
    return {
        "id": id,
        "pull_request_id": pr,
        "occurred_at": when,
        "type": type,
        "actor_login": actor,
        "assignee_login": assignee,
        "requested_reviewer_login": reviewer,
        "requested_team_slug": team,
    }


# --------------------------------------------------------------------- assignment kind


def test_classify_assignment_events_bot_self_other() -> None:
    ev = _events(
        [
            _row(
                id=1,
                pr=1,
                when=_dt(1),
                type="ASSIGNED",
                actor="mathlib-triage",
                assignee="alice",
            ),
            _row(
                id=2,
                pr=1,
                when=_dt(2),
                type="ASSIGNED",
                actor="alice",
                assignee="alice",
            ),
            _row(
                id=3, pr=1, when=_dt(3), type="ASSIGNED", actor="bob", assignee="alice"
            ),
            # non-ASSIGNED event should drop out
            _row(id=4, pr=1, when=_dt(4), type="REVIEW_COMMENTED", actor="alice"),
        ]
    )
    out = classify_assignment_events(ev).sort("id")
    assert out.height == 3
    assert out.get_column("kind").to_list() == ["bot", "self", "other_human"]


def test_classify_unassign_events_bot_self_other() -> None:
    ev = _events(
        [
            _row(
                id=1,
                pr=1,
                when=_dt(1),
                type="UNASSIGNED",
                actor="mathlib-triage",
                assignee="alice",
            ),
            _row(
                id=2, pr=1, when=_dt(2), type="UNASSIGNED", actor="bob", assignee="bob"
            ),
            _row(
                id=3,
                pr=1,
                when=_dt(3),
                type="UNASSIGNED",
                actor="carol",
                assignee="bob",
            ),
        ]
    )
    out = classify_unassign_events(ev).sort("id")
    assert out.get_column("kind").to_list() == ["bot", "self", "other_human"]


def test_assignment_bot_actors_membership() -> None:
    # Sanity: the two known automation accounts are in the set, and the
    # bors-family accounts are NOT (they don't assign reviewers).
    assert "mathlib-triage" in ASSIGNMENT_BOT_ACTORS
    assert "leanprover-community-bot-assistant" in ASSIGNMENT_BOT_ACTORS
    assert "bors" not in ASSIGNMENT_BOT_ACTORS
    assert "mathlib-bors" not in ASSIGNMENT_BOT_ACTORS


# --------------------------------------------------------------------- review-request response


def test_review_request_responses_matches_first_response_only() -> None:
    ev = _events(
        [
            _row(
                id=10,
                pr=1,
                when=_dt(1, 9),
                type="REVIEW_REQUESTED",
                actor="author",
                reviewer="alice",
            ),
            # Earlier event by alice — should NOT match (before the request).
            _row(id=11, pr=1, when=_dt(1, 8), type="REVIEW_COMMENTED", actor="alice"),
            # First post-request response by alice.
            _row(id=12, pr=1, when=_dt(1, 10), type="REVIEW_COMMENTED", actor="alice"),
            # Later approval — should NOT shadow the earlier comment.
            _row(id=13, pr=1, when=_dt(2), type="REVIEW_APPROVED", actor="alice"),
            # Same PR, different reviewer — irrelevant.
            _row(id=14, pr=1, when=_dt(1, 9, 30), type="REVIEW_COMMENTED", actor="bob"),
        ]
    )
    out = review_request_responses(ev)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["request_event_id"] == 10
    assert row["responded"] is True
    assert row["responded_at"] == _dt(1, 10)
    assert row["response_event_type"] == "REVIEW_COMMENTED"
    assert row["response_gap_seconds"] == 3600.0


def test_review_request_responses_no_match_when_only_other_reviewer() -> None:
    ev = _events(
        [
            _row(
                id=20,
                pr=1,
                when=_dt(1),
                type="REVIEW_REQUESTED",
                actor="author",
                reviewer="alice",
            ),
            _row(id=21, pr=1, when=_dt(2), type="REVIEW_COMMENTED", actor="bob"),
        ]
    )
    out = review_request_responses(ev)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["responded"] is False
    assert row["responded_at"] is None
    assert row["response_gap_seconds"] is None


def test_review_request_responses_team_request_yields_null_response() -> None:
    # Team-targeted requests have null requested_reviewer_login, so no
    # individual responder can match.
    ev = _events(
        [
            _row(
                id=30,
                pr=1,
                when=_dt(1),
                type="REVIEW_REQUESTED",
                actor="author",
                reviewer=None,
                team="mathlib-reviewers",
            ),
            _row(id=31, pr=1, when=_dt(2), type="REVIEW_APPROVED", actor="alice"),
        ]
    )
    out = review_request_responses(ev)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["responded"] is False


def test_review_request_responses_empty_input() -> None:
    ev = _events([])
    out = review_request_responses(ev)
    assert out.height == 0
    assert "responded" in out.columns
    assert "response_gap_seconds" in out.columns


# --------------------------------------------------------------------- assignment response


def test_assignment_responses_basic_match_and_kind() -> None:
    ev = _events(
        [
            # PR 1: bot-assigns alice; alice comments two hours later.
            _row(
                id=1,
                pr=1,
                when=_dt(1, 9),
                type="ASSIGNED",
                actor="mathlib-triage",
                assignee="alice",
            ),
            # Earlier alice comment — must not match (pre-assignment).
            _row(id=2, pr=1, when=_dt(1, 8), type="REVIEW_COMMENTED", actor="alice"),
            _row(id=3, pr=1, when=_dt(1, 11), type="REVIEW_COMMENTED", actor="alice"),
            # Approval later — must not shadow the earlier comment.
            _row(id=4, pr=1, when=_dt(2), type="REVIEW_APPROVED", actor="alice"),
            # PR 2: self-assign with a self-unassign response.
            _row(
                id=5,
                pr=2,
                when=_dt(1, 9),
                type="ASSIGNED",
                actor="bob",
                assignee="bob",
            ),
            _row(
                id=6,
                pr=2,
                when=_dt(1, 12),
                type="UNASSIGNED",
                actor="bob",
                assignee="bob",
            ),
            # PR 3: maintainer-assigns carol who never responds.
            _row(
                id=7,
                pr=3,
                when=_dt(1),
                type="ASSIGNED",
                actor="dan",
                assignee="carol",
            ),
            # Comment by a non-assignee on PR 3 — should not match.
            _row(id=8, pr=3, when=_dt(2), type="ISSUE_COMMENTED", actor="eve"),
        ]
    )
    out = assignment_responses(ev).sort("assignment_event_id")
    assert out.height == 3

    pr1 = out.row(0, named=True)
    assert pr1["pull_request_id"] == 1
    assert pr1["kind"] == "bot"
    assert pr1["responded"] is True
    assert pr1["response_event_type"] == "REVIEW_COMMENTED"
    assert pr1["response_gap_seconds"] == 7200.0

    pr2 = out.row(1, named=True)
    assert pr2["kind"] == "self"
    assert pr2["response_event_type"] == "UNASSIGNED"
    assert pr2["response_gap_seconds"] == 10800.0

    pr3 = out.row(2, named=True)
    assert pr3["kind"] == "other_human"
    assert pr3["responded"] is False
    assert pr3["responded_at"] is None
    assert pr3["response_gap_seconds"] is None


def test_assignment_responses_re_assignment_each_row_matches_separately() -> None:
    # alice is assigned twice; each ASSIGNED gets its own row. The
    # earliest action by alice at-or-after each assignment is the match,
    # so the first re-assignment can match the same later comment.
    ev = _events(
        [
            _row(
                id=1, pr=1, when=_dt(1), type="ASSIGNED", actor="bot", assignee="alice"
            ),
            _row(
                id=2,
                pr=1,
                when=_dt(2),
                type="UNASSIGNED",
                actor="alice",
                assignee="alice",
            ),
            _row(
                id=3, pr=1, when=_dt(3), type="ASSIGNED", actor="bot", assignee="alice"
            ),
            _row(id=4, pr=1, when=_dt(5), type="REVIEW_COMMENTED", actor="alice"),
        ]
    )
    out = assignment_responses(ev).sort("assignment_event_id")
    assert out.height == 2
    # First ASSIGNED → first response is the self-UNASSIGNED.
    assert out.row(0, named=True)["response_event_type"] == "UNASSIGNED"
    # Second ASSIGNED → the post-reassignment comment.
    assert out.row(1, named=True)["response_event_type"] == "REVIEW_COMMENTED"


def test_assignment_responses_empty_input() -> None:
    ev = _events([])
    out = assignment_responses(ev)
    assert out.height == 0
    assert "responded" in out.columns
    assert "response_gap_seconds" in out.columns
    assert "kind" in out.columns


# --------------------------------------------------------------------- assignee sets


def test_pr_assignees_ever_dedupes_per_pr() -> None:
    ev = _events(
        [
            _row(
                id=1, pr=1, when=_dt(1), type="ASSIGNED", actor="bot", assignee="alice"
            ),
            _row(
                id=2, pr=1, when=_dt(2), type="ASSIGNED", actor="bot", assignee="alice"
            ),  # duplicate
            _row(id=3, pr=1, when=_dt(3), type="ASSIGNED", actor="bot", assignee="bob"),
            _row(
                id=4, pr=2, when=_dt(1), type="ASSIGNED", actor="bot", assignee="carol"
            ),
            # ASSIGNED with null assignee shouldn't crash; skipped.
            _row(id=5, pr=2, when=_dt(2), type="ASSIGNED", actor="bot", assignee=None),
        ]
    )
    out = pr_assignees_ever(ev).sort(["pull_request_id", "assignee_login"])
    assert out.height == 3
    assert out.row(0, named=True) == {"pull_request_id": 1, "assignee_login": "alice"}
    assert out.row(2, named=True) == {"pull_request_id": 2, "assignee_login": "carol"}


# --------------------------------------------------------------------- policy outcome


def test_assignment_policy_outcome_any_assignee_match() -> None:
    ev = _events(
        [
            _row(
                id=1, pr=1, when=_dt(1), type="ASSIGNED", actor="bot", assignee="alice"
            ),
            _row(id=2, pr=1, when=_dt(2), type="ASSIGNED", actor="bot", assignee="bob"),
            # PR 2: assignee mismatch with MM trigger.
            _row(
                id=3, pr=2, when=_dt(1), type="ASSIGNED", actor="bot", assignee="carol"
            ),
            # PR 3: no MM attribution; should drop out (inner join).
            _row(id=4, pr=3, when=_dt(1), type="ASSIGNED", actor="bot", assignee="dan"),
        ]
    )
    mm = _attr(
        [
            {"pull_request_id": 1, "label_at": _dt(5), "inferred_actor": "bob"},
            {"pull_request_id": 2, "label_at": _dt(5), "inferred_actor": "eve"},
            # PR 4 has MM but no ASSIGNED — should also drop out.
            {"pull_request_id": 4, "label_at": _dt(5), "inferred_actor": "frank"},
        ]
    )
    out = assignment_policy_outcome(ev, mm).sort("pull_request_id")
    assert out.height == 2  # only PRs with both ASSIGNED and MM attribution
    row1 = out.filter(pl.col("pull_request_id") == 1).row(0, named=True)
    row2 = out.filter(pl.col("pull_request_id") == 2).row(0, named=True)
    assert row1["assignee_triggered_mm"] is True
    assert row1["mm_trigger"] == "bob"
    assert row1["n_assignees"] == 2
    assert row2["assignee_triggered_mm"] is False


def test_assignment_policy_outcome_uses_first_mm_label_only() -> None:
    # Some PRs get maintainer-merge re-applied after a force-push. The
    # policy outcome is keyed off the FIRST application.
    ev = _events(
        [
            _row(
                id=1, pr=1, when=_dt(1), type="ASSIGNED", actor="bot", assignee="alice"
            ),
        ]
    )
    mm = _attr(
        [
            {"pull_request_id": 1, "label_at": _dt(5), "inferred_actor": "alice"},
            {"pull_request_id": 1, "label_at": _dt(6), "inferred_actor": "bob"},
        ]
    )
    out = assignment_policy_outcome(ev, mm)
    row = out.row(0, named=True)
    assert row["mm_trigger"] == "alice"
    assert row["mm_at"] == _dt(5)
    assert row["assignee_triggered_mm"] is True


def test_assignment_policy_outcome_null_mm_trigger_is_false() -> None:
    # Unattributed MM (no inferred_actor) cannot match any assignee.
    ev = _events(
        [
            _row(
                id=1, pr=1, when=_dt(1), type="ASSIGNED", actor="bot", assignee="alice"
            ),
        ]
    )
    mm = _attr(
        [
            {"pull_request_id": 1, "label_at": _dt(5), "inferred_actor": None},
        ]
    )
    out = assignment_policy_outcome(ev, mm)
    row = out.row(0, named=True)
    assert row["assignee_triggered_mm"] is False
