from datetime import datetime, timedelta, timezone

import polars as pl

from qb_notebook.review_states import (
    attribute_label_events,
    label_intervals,
    stage_timestamps,
)


def _events(rows: list[dict]) -> pl.DataFrame:
    """Build an events frame with the columns label_intervals reads."""
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "occurred_at": pl.Datetime("us", "UTC"),
            "type": pl.String,
            "label_name": pl.String,
            "actor_login": pl.String,
        },
    )


def _dt(day: int, hour: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, tzinfo=timezone.utc)


def test_simple_apply_remove_pair() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "bob",
            },
        ]
    )
    out = label_intervals(ev, "awaiting-review", asof=_dt(10))
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["start"] == _dt(1)
    assert row["end"] == _dt(2)
    assert row["applied_by"] == "alice"
    assert row["removed_by"] == "bob"
    assert row["is_open"] is False
    assert row["duration_days"] == 1.0


def test_open_interval_closed_by_asof() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
        ]
    )
    asof = _dt(5)
    out = label_intervals(ev, "awaiting-review", asof=asof)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["end"] is None
    assert row["end_effective"] == asof
    assert row["is_open"] is True
    assert row["removed_by"] is None


def test_repeated_labeled_ignored() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "carol",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "bob",
            },
        ]
    )
    out = label_intervals(ev, "awaiting-review", asof=_dt(10))
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["start"] == _dt(1)
    assert row["end"] == _dt(3)
    assert row["applied_by"] == "alice"


def test_unlabeled_without_prior_labeled_is_skipped() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "bob",
            },
        ]
    )
    out = label_intervals(ev, "awaiting-review", asof=_dt(10))
    assert out.height == 0


def test_ping_pong_two_intervals() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(4),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "carol",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(6),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "dan",
            },
        ]
    )
    out = label_intervals(ev, "awaiting-review", asof=_dt(10))
    assert out.height == 2
    assert out["duration_days"].to_list() == [1.0, 2.0]
    assert out["applied_by"].to_list() == ["alice", "carol"]
    assert out["removed_by"].to_list() == ["alice", "dan"]


def test_multiple_labels_in_one_call() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "LABELED",
                "label_name": "awaiting-author",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "UNLABELED",
                "label_name": "awaiting-author",
                "actor_login": "alice",
            },
        ]
    )
    out = label_intervals(
        ev, ["awaiting-review", "awaiting-author"], asof=_dt(10)
    ).sort(["pull_request_id", "label_name", "start"])
    assert out.height == 2
    assert set(out["label_name"].to_list()) == {"awaiting-review", "awaiting-author"}


def test_per_pr_isolation() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 2,
                "occurred_at": _dt(2),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "bob",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
        ]
    )
    out = label_intervals(ev, "awaiting-review", asof=_dt(10)).sort("pull_request_id")
    assert out.height == 2
    pr1 = out.row(0, named=True)
    pr2 = out.row(1, named=True)
    assert pr1["pull_request_id"] == 1
    assert pr1["is_open"] is False
    assert pr2["pull_request_id"] == 2
    assert pr2["is_open"] is True


def test_simultaneous_label_unlabel_ordered_label_first() -> None:
    """LABELED at the same instant as UNLABELED should still produce a (zero-duration) interval."""
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(1),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
        ]
    )
    out = label_intervals(ev, "awaiting-review", asof=_dt(10))
    assert out.height == 1
    assert out.row(0, named=True)["duration_days"] == 0.0


def test_stage_timestamps_first_application_only() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(5),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(6),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "bob",
            },
        ]
    )
    out = stage_timestamps(ev)
    row = out.row(0, named=True)
    assert row["first_awaiting_review"] == _dt(2)
    assert row["first_maintainer_merge"] == _dt(6)
    assert row["first_ready_to_merge"] is None


# ---- attribute_label_events ------------------------------------------------


def _ts(*, mins: int = 0, secs: int = 0) -> datetime:
    return datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        minutes=mins, seconds=secs
    )


def test_attribute_picks_most_recent_human_within_window() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=3),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["inferred_actor"] == "bob"
    assert row["trigger_event_type"] == "ISSUE_COMMENTED"
    assert row["gap_seconds"] == 60
    assert row["attributed"] is True


def test_attribute_returns_null_when_no_event_in_window() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=30),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["inferred_actor"] is None
    assert row["attributed"] is False


def test_attribute_skips_bot_actors() -> None:
    """A bot comment just before the label shouldn't be credited."""
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "github-actions",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=3),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600)
    row = out.row(0, named=True)
    assert row["inferred_actor"] == "alice"


def test_attribute_does_not_pick_future_events() -> None:
    """Events after the label shouldn't be considered triggers."""
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600)
    row = out.row(0, named=True)
    assert row["inferred_actor"] is None


def test_attribute_isolates_per_pr() -> None:
    """A comment on PR 2 shouldn't attribute a label on PR 1."""
    ev = _events(
        [
            {
                "pull_request_id": 2,
                "occurred_at": _ts(mins=0),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=1),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600)
    rows = {r["pull_request_id"]: r for r in out.iter_rows(named=True)}
    assert rows[1]["inferred_actor"] is None


def test_attribute_uses_review_approved_as_trigger() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "REVIEW_APPROVED",
                "label_name": None,
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=1),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600)
    row = out.row(0, named=True)
    assert row["inferred_actor"] == "alice"
    assert row["trigger_event_type"] == "REVIEW_APPROVED"


def test_attribute_handles_multiple_label_events_per_pr() -> None:
    """A PR that gets maintainer-merge twice (e.g. after force-push)
    should produce two attributions, one per LABELED event."""
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=1),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=30),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=31),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "github-actions",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600).sort(
        "label_at"
    )
    assert out.height == 2
    assert out["inferred_actor"].to_list() == ["alice", "bob"]


def test_attribute_returns_empty_when_label_absent() -> None:
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
        ]
    )
    out = attribute_label_events(ev, "maintainer-merge", window_seconds=600)
    assert out.height == 0
