from datetime import datetime, timedelta, timezone

import polars as pl

from qb_notebook.review_states import (
    DEFAULT_BOT_ACTORS,
    MACHINE_USER_NODE_IDS,
    MATHLIB_LABEL_RETIRED_AT,
    attribute_label_events,
    bot_actor_expr,
    first_review_touch,
    inline_comment_stats,
    label_intervals,
    label_overlap_seconds,
    labels_active_at,
    pipeline_stages,
    queue_window_intervals,
    reviewers_court_intervals,
    stage_timestamps,
)


def _queue_windows(rows: list[dict]) -> pl.DataFrame:
    """Build a queue_windows frame with the columns queue_window_intervals reads."""
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "rule_set_id": pl.Int64,
            "cycle_index": pl.Int64,
            "window_count": pl.Int64,
            "first_on_queue_ts": pl.Datetime("us", "UTC"),
            "from_ts": pl.Datetime("us", "UTC"),
            "to_ts": pl.Datetime("us", "UTC"),
            "opened_by_event_type": pl.String,
            "closed_by_event_type": pl.String,
        },
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


def test_label_asof_overrides_closes_retired_label_intervals() -> None:
    """Open intervals for a retired label clamp at the override; is_open flips."""
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
    retired = _dt(5)
    asof = _dt(20)
    out = label_intervals(
        ev,
        "awaiting-review",
        asof=asof,
        label_asof_overrides={"awaiting-review": retired},
    )
    row = out.row(0, named=True)
    assert row["is_open"] is False
    assert row["end"] is None  # No actual UNLABELED event was observed
    assert row["end_effective"] == retired
    assert row["duration_days"] == 4.0  # _dt(5) - _dt(1) = 4 days


def test_label_asof_overrides_does_not_touch_closed_intervals() -> None:
    """A label with an UNLABELED event keeps its real `end`, ignoring the override."""
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
                "occurred_at": _dt(3),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
        ]
    )
    out = label_intervals(
        ev,
        "awaiting-review",
        asof=_dt(20),
        label_asof_overrides={"awaiting-review": _dt(5)},
    )
    row = out.row(0, named=True)
    assert row["is_open"] is False
    assert row["end"] == _dt(3)
    assert row["end_effective"] == _dt(3)


def test_label_asof_overrides_only_affects_named_labels() -> None:
    """An override for label A leaves label B's open intervals alone."""
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
                "label_name": "WIP",
                "actor_login": "alice",
            },
        ]
    )
    out = label_intervals(
        ev,
        ["awaiting-review", "WIP"],
        asof=_dt(20),
        label_asof_overrides={"awaiting-review": _dt(5)},
    ).sort("label_name")
    ar = out.filter(pl.col("label_name") == "awaiting-review").row(0, named=True)
    wip = out.filter(pl.col("label_name") == "WIP").row(0, named=True)
    assert ar["is_open"] is False
    assert ar["end_effective"] == _dt(5)
    assert wip["is_open"] is True
    assert wip["end_effective"] == _dt(20)


def _pr_close(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "closed_at": pl.Datetime("us", "UTC"),
        },
    )


def test_df_pr_close_clamps_open_interval_at_pr_close() -> None:
    """An open interval on a closed PR clamps at closed_at and flips is_open."""
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
    closes = _pr_close([{"pull_request_id": 1, "closed_at": _dt(3)}])
    out = label_intervals(ev, "awaiting-review", asof=_dt(20), df_pr_close=closes)
    row = out.row(0, named=True)
    assert row["is_open"] is False
    assert row["end"] is None
    assert row["end_effective"] == _dt(3)
    assert row["duration_days"] == 2.0


def test_df_pr_close_leaves_open_for_still_open_pr() -> None:
    """A PR with closed_at = null leaves open intervals alone."""
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
    closes = pl.DataFrame(
        [{"pull_request_id": 1, "closed_at": None}],
        schema={
            "pull_request_id": pl.Int64,
            "closed_at": pl.Datetime("us", "UTC"),
        },
    )
    out = label_intervals(ev, "awaiting-review", asof=_dt(20), df_pr_close=closes)
    row = out.row(0, named=True)
    assert row["is_open"] is True
    assert row["end_effective"] == _dt(20)


def test_df_pr_close_does_not_clamp_labels_applied_after_close() -> None:
    """A label applied AFTER the PR closed (record-keeping) stays open."""
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(5),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "alice",
            },
        ]
    )
    closes = _pr_close([{"pull_request_id": 1, "closed_at": _dt(3)}])
    out = label_intervals(ev, "maintainer-merge", asof=_dt(20), df_pr_close=closes)
    row = out.row(0, named=True)
    # start (_dt(5)) > closed_at (_dt(3)) — clamp predicate fails, stays open.
    assert row["is_open"] is True
    assert row["end_effective"] == _dt(20)


def test_df_pr_close_does_not_alter_closed_intervals() -> None:
    """An interval with a real UNLABELED keeps its end regardless of close time."""
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
        ]
    )
    closes = _pr_close([{"pull_request_id": 1, "closed_at": _dt(10)}])
    out = label_intervals(ev, "awaiting-review", asof=_dt(20), df_pr_close=closes)
    row = out.row(0, named=True)
    assert row["end"] == _dt(2)
    assert row["end_effective"] == _dt(2)


def test_df_pr_close_takes_min_with_retirement_override() -> None:
    """If both close-clamp and retirement-override apply, the earlier wins."""
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
    closes = _pr_close([{"pull_request_id": 1, "closed_at": _dt(8)}])
    # Retirement at _dt(5) is earlier than close at _dt(8); end_effective should be _dt(5).
    out = label_intervals(
        ev,
        "awaiting-review",
        asof=_dt(20),
        label_asof_overrides={"awaiting-review": _dt(5)},
        df_pr_close=closes,
    )
    row = out.row(0, named=True)
    assert row["is_open"] is False
    assert row["end_effective"] == _dt(5)


def test_mathlib_label_retired_at_includes_awaiting_review() -> None:
    """Sanity check the exported constant the notebooks import."""
    assert "awaiting-review" in MATHLIB_LABEL_RETIRED_AT
    assert MATHLIB_LABEL_RETIRED_AT["awaiting-review"].tzinfo is timezone.utc


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


def test_attribute_skips_bors_family_bots() -> None:
    """``mathlib-bors`` / ``bors`` post replies to ``bors r+`` / ``bors delegate``
    commands seconds before the corresponding bot-applied label. Without
    excluding them, ``delegated`` attribution lands on the bors bot rather than
    the maintainer who issued the command. They must be in DEFAULT_BOT_ACTORS."""
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=0),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "kim-em",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=1),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "mathlib-bors",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _ts(mins=2),
                "type": "LABELED",
                "label_name": "delegated",
                "actor_login": "leanprover-community-mathlib4-bot",
            },
        ]
    )
    out = attribute_label_events(ev, "delegated", window_seconds=600)
    assert out.row(0, named=True)["inferred_actor"] == "kim-em"


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


# ---- label_overlap_seconds -------------------------------------------------


def _intervals(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "start": pl.Datetime("us", "UTC"),
            "end_effective": pl.Datetime("us", "UTC"),
        },
    )


def _windows(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "window_start": pl.Datetime("us", "UTC"),
            "window_end": pl.Datetime("us", "UTC"),
        },
    )


def test_overlap_full_containment() -> None:
    ivals = _intervals(
        [{"pull_request_id": 1, "start": _dt(2), "end_effective": _dt(3)}]
    )
    wins = _windows(
        [{"pull_request_id": 1, "window_start": _dt(1), "window_end": _dt(5)}]
    )
    out = label_overlap_seconds(ivals, wins)
    assert out["overlap_seconds"].to_list() == [86400.0]
    assert out["had_overlap"].to_list() == [True]


def test_overlap_partial_left_and_right() -> None:
    ivals = _intervals(
        [
            {"pull_request_id": 1, "start": _dt(1), "end_effective": _dt(3)},
            {"pull_request_id": 1, "start": _dt(5), "end_effective": _dt(7)},
        ]
    )
    wins = _windows(
        [{"pull_request_id": 1, "window_start": _dt(2), "window_end": _dt(6)}]
    )
    out = label_overlap_seconds(ivals, wins)
    # First interval contributes [_dt(2), _dt(3)] = 1 day; second contributes
    # [_dt(5), _dt(6)] = 1 day. Total = 2 days = 172800 s.
    assert out["overlap_seconds"].to_list() == [172800.0]
    assert out["had_overlap"].to_list() == [True]


def test_overlap_zero_when_disjoint() -> None:
    ivals = _intervals(
        [{"pull_request_id": 1, "start": _dt(1), "end_effective": _dt(2)}]
    )
    wins = _windows(
        [{"pull_request_id": 1, "window_start": _dt(5), "window_end": _dt(6)}]
    )
    out = label_overlap_seconds(ivals, wins)
    assert out["overlap_seconds"].to_list() == [0.0]
    assert out["had_overlap"].to_list() == [False]


def test_overlap_no_matching_pr() -> None:
    ivals = _intervals(
        [{"pull_request_id": 2, "start": _dt(1), "end_effective": _dt(5)}]
    )
    wins = _windows(
        [{"pull_request_id": 1, "window_start": _dt(2), "window_end": _dt(4)}]
    )
    out = label_overlap_seconds(ivals, wins)
    assert out["overlap_seconds"].to_list() == [0.0]
    assert out["had_overlap"].to_list() == [False]


def test_overlap_preserves_extra_window_columns() -> None:
    ivals = _intervals(
        [{"pull_request_id": 1, "start": _dt(2), "end_effective": _dt(3)}]
    )
    wins = _windows(
        [{"pull_request_id": 1, "window_start": _dt(1), "window_end": _dt(5)}]
    ).with_columns(pl.lit("hello").alias("note"))
    out = label_overlap_seconds(ivals, wins)
    assert "note" in out.columns
    assert out["note"].to_list() == ["hello"]


def test_overlap_drops_null_interval_endpoints() -> None:
    # A row whose start or end is null should be ignored (still-open intervals
    # are expected to come in as `end_effective` non-null).
    ivals = pl.DataFrame(
        {
            "pull_request_id": [1, 1],
            "start": [_dt(2), None],
            "end_effective": [_dt(3), _dt(4)],
        },
        schema={
            "pull_request_id": pl.Int64,
            "start": pl.Datetime("us", "UTC"),
            "end_effective": pl.Datetime("us", "UTC"),
        },
    )
    wins = _windows(
        [{"pull_request_id": 1, "window_start": _dt(1), "window_end": _dt(5)}]
    )
    out = label_overlap_seconds(ivals, wins)
    assert out["overlap_seconds"].to_list() == [86400.0]


# ---- labels_active_at ------------------------------------------------------


def _label_intervals_frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "label_name": pl.String,
            "start": pl.Datetime("us", "UTC"),
            "end_effective": pl.Datetime("us", "UTC"),
        },
    )


def _points_frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "at": pl.Datetime("us", "UTC"),
            "actor": pl.String,
        },
    )


def test_labels_active_at_single_match() -> None:
    ivals = _label_intervals_frame(
        [
            {
                "pull_request_id": 1,
                "label_name": "t-algebra",
                "start": _dt(1),
                "end_effective": _dt(5),
            },
        ]
    )
    points = _points_frame([{"pull_request_id": 1, "at": _dt(3), "actor": "alice"}])
    out = labels_active_at(ivals, points)
    assert out.height == 1
    assert out["label_name"].to_list() == ["t-algebra"]
    assert out["actor"].to_list() == ["alice"]


def test_labels_active_at_multiple_labels_per_point() -> None:
    # PR has two t-* labels both active at the point's timestamp; the helper
    # should emit one row per matching label (count-toward-each-area model).
    ivals = _label_intervals_frame(
        [
            {
                "pull_request_id": 1,
                "label_name": "t-algebra",
                "start": _dt(1),
                "end_effective": _dt(10),
            },
            {
                "pull_request_id": 1,
                "label_name": "t-ring-theory",
                "start": _dt(2),
                "end_effective": _dt(8),
            },
            {
                "pull_request_id": 1,
                "label_name": "t-analysis",
                "start": _dt(20),
                "end_effective": _dt(25),
            },
        ]
    )
    points = _points_frame([{"pull_request_id": 1, "at": _dt(5), "actor": "alice"}])
    out = labels_active_at(ivals, points).sort("label_name")
    assert out["label_name"].to_list() == ["t-algebra", "t-ring-theory"]


def test_labels_active_at_half_open_interval() -> None:
    # `start` is inclusive, `end_effective` is exclusive: a point exactly at
    # `end_effective` does NOT match, a point exactly at `start` does.
    ivals = _label_intervals_frame(
        [
            {
                "pull_request_id": 1,
                "label_name": "t-algebra",
                "start": _dt(2),
                "end_effective": _dt(4),
            },
        ]
    )
    points = _points_frame(
        [
            {"pull_request_id": 1, "at": _dt(2), "actor": "start_edge"},
            {"pull_request_id": 1, "at": _dt(4), "actor": "end_edge"},
        ]
    )
    out = labels_active_at(ivals, points)
    assert out["actor"].to_list() == ["start_edge"]


def test_labels_active_at_drops_points_with_no_match() -> None:
    ivals = _label_intervals_frame(
        [
            {
                "pull_request_id": 1,
                "label_name": "t-algebra",
                "start": _dt(1),
                "end_effective": _dt(2),
            },
        ]
    )
    points = _points_frame(
        [
            {"pull_request_id": 1, "at": _dt(5), "actor": "after"},
            {"pull_request_id": 2, "at": _dt(1, 12), "actor": "wrong_pr"},
        ]
    )
    out = labels_active_at(ivals, points)
    assert out.height == 0


def test_labels_active_at_isolates_per_pr() -> None:
    ivals = _label_intervals_frame(
        [
            {
                "pull_request_id": 1,
                "label_name": "t-algebra",
                "start": _dt(1),
                "end_effective": _dt(5),
            },
            {
                "pull_request_id": 2,
                "label_name": "t-analysis",
                "start": _dt(1),
                "end_effective": _dt(5),
            },
        ]
    )
    points = _points_frame(
        [
            {"pull_request_id": 1, "at": _dt(3), "actor": "alice"},
            {"pull_request_id": 2, "at": _dt(3), "actor": "bob"},
        ]
    )
    out = labels_active_at(ivals, points).sort("actor")
    assert out["actor"].to_list() == ["alice", "bob"]
    assert out["label_name"].to_list() == ["t-algebra", "t-analysis"]


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


def test_queue_window_intervals_closed_and_open() -> None:
    qw = _queue_windows(
        [
            {
                "pull_request_id": 1,
                "rule_set_id": 3,
                "cycle_index": 1,
                "window_count": 1,
                "first_on_queue_ts": _dt(1),
                "from_ts": _dt(1),
                "to_ts": _dt(3),
                "opened_by_event_type": "CI_PASSED",
                "closed_by_event_type": "CI_PASSED",
            },
            {
                "pull_request_id": 1,
                "rule_set_id": 3,
                "cycle_index": 2,
                "window_count": 2,
                "first_on_queue_ts": _dt(1),
                "from_ts": _dt(5),
                "to_ts": None,
                "opened_by_event_type": "CI_PASSED",
                "closed_by_event_type": None,
            },
        ]
    )
    out = queue_window_intervals(qw, asof=_dt(10)).sort("start")
    assert out.height == 2

    closed = out.row(0, named=True)
    assert closed["start"] == _dt(1)
    assert closed["end"] == _dt(3)
    assert closed["is_open"] is False
    assert closed["end_effective"] == _dt(3)
    assert closed["duration_days"] == 2.0
    assert closed["cycle_index"] == 1
    assert closed["rule_set_id"] == 3
    assert closed["closed_by_event_type"] == "CI_PASSED"

    open_row = out.row(1, named=True)
    assert open_row["start"] == _dt(5)
    assert open_row["end"] is None
    assert open_row["is_open"] is True
    assert open_row["end_effective"] == _dt(10)
    assert open_row["duration_days"] == 5.0


def test_queue_window_intervals_filters_by_ruleset() -> None:
    qw = _queue_windows(
        [
            {
                "pull_request_id": 1,
                "rule_set_id": 2,
                "cycle_index": 1,
                "window_count": 1,
                "first_on_queue_ts": _dt(1),
                "from_ts": _dt(1),
                "to_ts": _dt(2),
                "opened_by_event_type": "CI_PASSED",
                "closed_by_event_type": "CI_PASSED",
            },
            {
                "pull_request_id": 1,
                "rule_set_id": 3,
                "cycle_index": 1,
                "window_count": 1,
                "first_on_queue_ts": _dt(1),
                "from_ts": _dt(1),
                "to_ts": _dt(2),
                "opened_by_event_type": "CI_PASSED",
                "closed_by_event_type": "CI_PASSED",
            },
        ]
    )
    out_default = queue_window_intervals(qw, asof=_dt(10))
    assert out_default.height == 1
    assert out_default["rule_set_id"].to_list() == [3]

    out_all = queue_window_intervals(qw, rule_set_id=None, asof=_dt(10))
    assert out_all.height == 2
    assert sorted(out_all["rule_set_id"].to_list()) == [2, 3]


def test_reviewers_court_prefers_queue_window_per_pr() -> None:
    """When a PR has both signals, only queue-window rows survive."""
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
        ]
    )
    qw = _queue_windows(
        [
            {
                "pull_request_id": 1,
                "rule_set_id": 3,
                "cycle_index": 1,
                "window_count": 1,
                "first_on_queue_ts": _dt(1),
                "from_ts": _dt(1),
                "to_ts": _dt(3),
                "opened_by_event_type": "CI_PASSED",
                "closed_by_event_type": "CI_PASSED",
            },
        ]
    )
    out = reviewers_court_intervals(ev, qw, asof=_dt(10))
    assert out.height == 1
    assert out["source"].to_list() == ["queue_window"]
    assert out["end"].to_list() == [_dt(3)]


def test_reviewers_court_falls_back_to_label_when_queue_missing() -> None:
    """PRs with no queue-window rows use label intervals."""
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
        ]
    )
    qw = _queue_windows([])
    out = reviewers_court_intervals(ev, qw, asof=_dt(10))
    assert out.height == 1
    assert out["source"].to_list() == ["label"]
    assert out["duration_days"].to_list() == [1.0]


def test_reviewers_court_mixed_cohort() -> None:
    """Queue PR yields queue rows; label-only PR yields label rows."""
    ev = _events(
        [
            {
                "pull_request_id": 2,
                "occurred_at": _dt(1),
                "type": "LABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
            {
                "pull_request_id": 2,
                "occurred_at": _dt(2),
                "type": "UNLABELED",
                "label_name": "awaiting-review",
                "actor_login": "alice",
            },
        ]
    )
    qw = _queue_windows(
        [
            {
                "pull_request_id": 1,
                "rule_set_id": 3,
                "cycle_index": 1,
                "window_count": 1,
                "first_on_queue_ts": _dt(1),
                "from_ts": _dt(1),
                "to_ts": _dt(3),
                "opened_by_event_type": "CI_PASSED",
                "closed_by_event_type": "CI_PASSED",
            },
        ]
    )
    out = reviewers_court_intervals(ev, qw, asof=_dt(10)).sort("pull_request_id")
    assert out["pull_request_id"].to_list() == [1, 2]
    assert out["source"].to_list() == ["queue_window", "label"]


def test_reviewers_court_label_asof_clamps_open_label_intervals() -> None:
    """label_asof bounds end_effective for label intervals without UNLABELED."""
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
    qw = _queue_windows([])
    asof = _dt(20)
    label_asof = _dt(5)
    out = reviewers_court_intervals(ev, qw, asof=asof, label_asof=label_asof)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["source"] == "label"
    assert row["is_open"] is True
    assert row["end_effective"] == label_asof
    assert row["duration_days"] == 4.0


# ---- first_review_touch -----------------------------------------------------


def _prs_frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "id": pl.Int64,
            "gh_created_at": pl.Datetime("us", "UTC"),
            "author_login": pl.String,
        },
    )


def test_first_review_touch_basic() -> None:
    """A single qualifying event from a non-author non-bot becomes the touch."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
        ]
    )
    out = first_review_touch(prs, ev)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["pull_request_id"] == 1
    assert row["first_touch_at"] == _dt(2)
    assert row["first_touch_actor"] == "bob"
    assert row["first_touch_event_type"] == "REVIEW_COMMENTED"
    assert row["first_touch_seconds_from_open"] == 86400.0


def test_first_review_touch_skips_author() -> None:
    """The PR author's own comment doesn't count as a touch."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
        ]
    )
    out = first_review_touch(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_actor"] == "bob"
    assert row["first_touch_at"] == _dt(3)


def test_first_review_touch_skips_bots() -> None:
    """Bot comments are excluded even if they precede a human one."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "github-actions",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
        ]
    )
    out = first_review_touch(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_actor"] == "bob"


def test_first_review_touch_earliest_wins() -> None:
    """Multiple qualifying events → keep the earliest."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(5),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "carol",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "REVIEW_APPROVED",
                "label_name": None,
                "actor_login": "dan",
            },
        ]
    )
    out = first_review_touch(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_at"] == _dt(2)
    assert row["first_touch_actor"] == "bob"


def test_first_review_touch_no_qualifying_event_yields_nulls() -> None:
    """A PR with no qualifying event still appears in the output with null cols."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "LABELED",  # not a touch type
                "label_name": "awaiting-review",
                "actor_login": "bob",
            },
        ]
    )
    out = first_review_touch(prs, ev)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["first_touch_at"] is None
    assert row["first_touch_actor"] is None
    assert row["first_touch_seconds_from_open"] is None


def test_first_review_touch_isolates_per_pr() -> None:
    prs = _prs_frame(
        [
            {"id": 1, "gh_created_at": _dt(1), "author_login": "alice"},
            {"id": 2, "gh_created_at": _dt(1), "author_login": "bob"},
        ]
    )
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "carol",
            },
            {
                "pull_request_id": 2,
                "occurred_at": _dt(2),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "carol",
            },
        ]
    )
    out = first_review_touch(prs, ev).sort("pull_request_id")
    assert out["first_touch_at"].to_list() == [_dt(3), _dt(2)]


def test_first_review_touch_case_insensitive_author() -> None:
    """Author/actor login comparison is case-insensitive."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "Alice"}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",  # same person, different case
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(3),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "BOB",
            },
        ]
    )
    out = first_review_touch(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_actor"] == "BOB"


def test_first_review_touch_null_author_keeps_all_events() -> None:
    """Author login null (deleted GH user) silently no-ops the exclusion."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": None}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "alice",
            },
        ]
    )
    out = first_review_touch(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_actor"] == "alice"


def test_first_review_touch_custom_event_types() -> None:
    """Passing a stricter event_types drops ISSUE_COMMENTED touches."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(5),
                "type": "REVIEW_APPROVED",
                "label_name": None,
                "actor_login": "carol",
            },
        ]
    )
    strict = ("REVIEW_APPROVED", "REVIEW_COMMENTED", "REVIEW_CHANGES_REQUESTED")
    out = first_review_touch(prs, ev, event_types=strict)
    row = out.row(0, named=True)
    assert row["first_touch_event_type"] == "REVIEW_APPROVED"
    assert row["first_touch_at"] == _dt(5)


def test_queue_window_intervals_overlap_with_label_overlap_seconds() -> None:
    qw = _queue_windows(
        [
            {
                "pull_request_id": 1,
                "rule_set_id": 3,
                "cycle_index": 1,
                "window_count": 1,
                "first_on_queue_ts": _dt(1),
                "from_ts": _dt(2),
                "to_ts": _dt(4),
                "opened_by_event_type": "CI_PASSED",
                "closed_by_event_type": "CI_PASSED",
            },
        ]
    )
    intervals = queue_window_intervals(qw, asof=_dt(10))

    windows = pl.DataFrame(
        {
            "pull_request_id": [1],
            "window_start": [_dt(1)],
            "window_end": [_dt(3)],
        },
        schema={
            "pull_request_id": pl.Int64,
            "window_start": pl.Datetime("us", "UTC"),
            "window_end": pl.Datetime("us", "UTC"),
        },
    )
    out = label_overlap_seconds(intervals, windows)
    assert out["overlap_seconds"].to_list() == [86400.0]
    assert out["had_overlap"].to_list() == [True]


# --- inline_comment_stats ----------------------------------------------------


def _inline(rows: list[dict]) -> pl.DataFrame:
    """Build an inline-comment frame with the columns inline_comment_stats reads."""
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "author_login": pl.String,
            "gh_created_at": pl.Datetime("us", "UTC"),
            "path": pl.String,
            "thread_root_node_id": pl.String,
            "reply_to_node_id": pl.String,
        },
    )


def _prs(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"id": pl.Int64, "author_login": pl.String},
    )


def test_inline_comment_stats_counts_threads_and_authors() -> None:
    inline = _inline(
        [
            # PR 1: alice opens a thread, bob replies; charlie opens another thread
            {
                "pull_request_id": 1,
                "author_login": "alice",
                "gh_created_at": _dt(1, 10),
                "path": "a.lean",
                "thread_root_node_id": "T1",
                "reply_to_node_id": None,
            },
            {
                "pull_request_id": 1,
                "author_login": "bob",
                "gh_created_at": _dt(1, 11),
                "path": "a.lean",
                "thread_root_node_id": "T1",
                "reply_to_node_id": "T1",
            },
            {
                "pull_request_id": 1,
                "author_login": "charlie",
                "gh_created_at": _dt(1, 12),
                "path": "b.lean",
                "thread_root_node_id": "T2",
                "reply_to_node_id": None,
            },
        ]
    )
    prs = _prs([{"id": 1, "author_login": "dave"}])  # author is none of the reviewers
    out = inline_comment_stats(inline, prs)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["pull_request_id"] == 1
    assert row["n_inline_comments"] == 3
    assert row["n_inline_comments_by_others"] == 3
    assert row["n_inline_threads"] == 2
    assert row["n_inline_thread_replies"] == 1
    assert row["n_inline_authors"] == 3
    assert row["n_inline_files"] == 2
    assert row["first_inline_at"] == _dt(1, 10)
    assert row["last_inline_at"] == _dt(1, 12)


def test_inline_comment_stats_excludes_author_and_bots() -> None:
    inline = _inline(
        [
            # Author's own inline comment — not "by others".
            {
                "pull_request_id": 7,
                "author_login": "Alice",  # uppercase to test case-insensitivity
                "gh_created_at": _dt(2, 9),
                "path": "a.lean",
                "thread_root_node_id": "T1",
                "reply_to_node_id": None,
            },
            # Bot — excluded from "by others" and from author count.
            {
                "pull_request_id": 7,
                "author_login": "github-actions",
                "gh_created_at": _dt(2, 10),
                "path": "a.lean",
                "thread_root_node_id": "T2",
                "reply_to_node_id": None,
            },
            # Reviewer — counts.
            {
                "pull_request_id": 7,
                "author_login": "bob",
                "gh_created_at": _dt(2, 11),
                "path": "a.lean",
                "thread_root_node_id": "T3",
                "reply_to_node_id": None,
            },
        ]
    )
    prs = _prs([{"id": 7, "author_login": "alice"}])
    out = inline_comment_stats(inline, prs)
    row = out.row(0, named=True)
    assert row["n_inline_comments"] == 3
    assert row["n_inline_comments_by_others"] == 1
    assert row["n_inline_authors"] == 1  # only bob
    assert row["n_inline_threads"] == 3


def test_inline_comment_stats_omits_prs_with_no_comments() -> None:
    inline = _inline(
        [
            {
                "pull_request_id": 1,
                "author_login": "bob",
                "gh_created_at": _dt(1),
                "path": "a.lean",
                "thread_root_node_id": "T1",
                "reply_to_node_id": None,
            },
        ]
    )
    prs = _prs(
        [
            {"id": 1, "author_login": "alice"},
            {"id": 2, "author_login": "alice"},  # has no comments
        ]
    )
    out = inline_comment_stats(inline, prs)
    assert out["pull_request_id"].to_list() == [1]


def test_inline_comment_stats_groups_by_pr() -> None:
    inline = _inline(
        [
            {
                "pull_request_id": 1,
                "author_login": "bob",
                "gh_created_at": _dt(1, 9),
                "path": "a.lean",
                "thread_root_node_id": "T1",
                "reply_to_node_id": None,
            },
            {
                "pull_request_id": 2,
                "author_login": "bob",
                "gh_created_at": _dt(1, 10),
                "path": "b.lean",
                "thread_root_node_id": "T2",
                "reply_to_node_id": None,
            },
            {
                "pull_request_id": 2,
                "author_login": "carol",
                "gh_created_at": _dt(1, 11),
                "path": "b.lean",
                "thread_root_node_id": "T2",
                "reply_to_node_id": "T2",
            },
        ]
    )
    prs = _prs(
        [
            {"id": 1, "author_login": "alice"},
            {"id": 2, "author_login": "alice"},
        ]
    )
    out = inline_comment_stats(inline, prs).sort("pull_request_id")
    assert out["pull_request_id"].to_list() == [1, 2]
    assert out["n_inline_comments"].to_list() == [1, 2]
    assert out["n_inline_thread_replies"].to_list() == [0, 1]
    assert out["n_inline_authors"].to_list() == [1, 2]


def test_inline_comment_stats_orphan_pr_treats_all_as_others() -> None:
    """Comments on a PR not in df_prs lose the author exclusion (null author),
    so every non-bot comment counts as "by others"."""
    inline = _inline(
        [
            {
                "pull_request_id": 99,
                "author_login": "bob",
                "gh_created_at": _dt(1),
                "path": "a.lean",
                "thread_root_node_id": "T1",
                "reply_to_node_id": None,
            },
        ]
    )
    prs = _prs([])  # no PRs known
    out = inline_comment_stats(inline, prs)
    row = out.row(0, named=True)
    assert row["n_inline_comments"] == 1
    assert row["n_inline_comments_by_others"] == 1
    assert row["n_inline_authors"] == 1


# ---- pipeline_stages --------------------------------------------------------


def _prs_pipeline(rows: list[dict]) -> pl.DataFrame:
    """Build a prs frame with the columns pipeline_stages reads."""
    return pl.DataFrame(
        rows,
        schema={
            "id": pl.Int64,
            "gh_created_at": pl.Datetime("us", "UTC"),
            "author_login": pl.String,
            "merged_at": pl.Datetime("us", "UTC"),
        },
    )


_SECONDS_PER_DAY = 86400.0


def test_pipeline_stages_linear_merge() -> None:
    """Open → touch → MM → RTM → merged produces four positive deltas."""
    prs = _prs_pipeline(
        [
            {
                "id": 1,
                "gh_created_at": _dt(1),
                "author_login": "alice",
                "merged_at": _dt(10),
            }
        ]
    )
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(4),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "bot",
            },
            {
                "pull_request_id": 1,
                "occurred_at": _dt(7),
                "type": "LABELED",
                "label_name": "ready-to-merge",
                "actor_login": "bot",
            },
        ]
    )
    out = pipeline_stages(prs, ev)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["opened_at"] == _dt(1)
    assert row["first_touch_at"] == _dt(2)
    assert row["first_maintainer_merge_at"] == _dt(4)
    assert row["first_ready_to_merge_at"] == _dt(7)
    assert row["merged_at_effective"] == _dt(10)
    assert row["seconds_open_to_first_touch"] == _SECONDS_PER_DAY
    assert row["seconds_first_touch_to_maintainer_merge"] == 2 * _SECONDS_PER_DAY
    assert row["seconds_maintainer_merge_to_ready_to_merge"] == 3 * _SECONDS_PER_DAY
    assert row["seconds_ready_to_merge_to_merged"] == 3 * _SECONDS_PER_DAY
    assert row["seconds_open_to_merged"] == 9 * _SECONDS_PER_DAY


def test_pipeline_stages_unmerged_pr_keeps_partial_milestones() -> None:
    """A closed-unmerged PR with a touch but no MM/RTM nulls the downstream deltas."""
    prs = _prs_pipeline(
        [
            {
                "id": 7,
                "gh_created_at": _dt(1),
                "author_login": "alice",
                "merged_at": None,
            }
        ]
    )
    ev = _events(
        [
            {
                "pull_request_id": 7,
                "occurred_at": _dt(3),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
        ]
    )
    out = pipeline_stages(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_at"] == _dt(3)
    assert row["first_maintainer_merge_at"] is None
    assert row["first_ready_to_merge_at"] is None
    assert row["merged_at_effective"] is None
    assert row["seconds_open_to_first_touch"] == 2 * _SECONDS_PER_DAY
    assert row["seconds_first_touch_to_maintainer_merge"] is None
    assert row["seconds_maintainer_merge_to_ready_to_merge"] is None
    assert row["seconds_ready_to_merge_to_merged"] is None
    assert row["seconds_open_to_merged"] is None


def test_pipeline_stages_no_touch_no_mm_direct_bors_merge() -> None:
    """Direct bors merge: RTM applied without a prior touch or MM; downstream
    deltas null since their start endpoints are missing, but the total
    open→merged delta is still computed from `gh_created_at` and `merged_at`."""
    prs = _prs_pipeline(
        [
            {
                "id": 2,
                "gh_created_at": _dt(1),
                "author_login": "alice",
                "merged_at": _dt(5),
            }
        ]
    )
    ev = _events(
        [
            {
                "pull_request_id": 2,
                "occurred_at": _dt(4),
                "type": "LABELED",
                "label_name": "ready-to-merge",
                "actor_login": "bot",
            },
        ]
    )
    out = pipeline_stages(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_at"] is None
    assert row["first_maintainer_merge_at"] is None
    assert row["first_ready_to_merge_at"] == _dt(4)
    assert row["seconds_open_to_first_touch"] is None
    assert row["seconds_first_touch_to_maintainer_merge"] is None
    assert row["seconds_maintainer_merge_to_ready_to_merge"] is None
    assert row["seconds_ready_to_merge_to_merged"] == _SECONDS_PER_DAY
    assert row["seconds_open_to_merged"] == 4 * _SECONDS_PER_DAY


def test_pipeline_stages_nonmonotonic_rtm_before_mm_yields_null_delta() -> None:
    """Rare ordering where RTM is applied before MM: the MM→RTM delta nulls
    rather than going negative so log-scale plots don't choke."""
    prs = _prs_pipeline(
        [
            {
                "id": 3,
                "gh_created_at": _dt(1),
                "author_login": "alice",
                "merged_at": _dt(10),
            }
        ]
    )
    ev = _events(
        [
            {
                "pull_request_id": 3,
                "occurred_at": _dt(2),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
            {
                "pull_request_id": 3,
                "occurred_at": _dt(4),
                "type": "LABELED",
                "label_name": "ready-to-merge",
                "actor_login": "bot",
            },
            {
                "pull_request_id": 3,
                "occurred_at": _dt(6),
                "type": "LABELED",
                "label_name": "maintainer-merge",
                "actor_login": "bot",
            },
        ]
    )
    out = pipeline_stages(prs, ev)
    row = out.row(0, named=True)
    assert row["first_maintainer_merge_at"] == _dt(6)
    assert row["first_ready_to_merge_at"] == _dt(4)
    assert row["seconds_maintainer_merge_to_ready_to_merge"] is None
    assert row["seconds_first_touch_to_maintainer_merge"] == 4 * _SECONDS_PER_DAY


def test_pipeline_stages_returns_one_row_per_input_pr() -> None:
    """PRs absent from events still appear with all-null stage columns."""
    prs = _prs_pipeline(
        [
            {
                "id": 10,
                "gh_created_at": _dt(1),
                "author_login": "alice",
                "merged_at": None,
            },
            {
                "id": 11,
                "gh_created_at": _dt(1),
                "author_login": "alice",
                "merged_at": _dt(2),
            },
        ]
    )
    ev = _events([])
    out = pipeline_stages(prs, ev).sort("pull_request_id")
    assert out.height == 2
    silent, merged = out.row(0, named=True), out.row(1, named=True)
    assert silent["first_touch_at"] is None
    assert silent["seconds_open_to_merged"] is None
    # PR 11 has merged_at but no events → only open→merged is populated.
    assert merged["seconds_open_to_merged"] == _SECONDS_PER_DAY
    assert merged["seconds_open_to_first_touch"] is None


def test_pipeline_stages_without_merged_column() -> None:
    """Passing pr_merged_col=None yields a null merged_at_effective so the
    helper works for "all candidate PRs" frames that haven't joined merge
    metadata yet."""
    prs = pl.DataFrame(
        [{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}],
        schema={
            "id": pl.Int64,
            "gh_created_at": pl.Datetime("us", "UTC"),
            "author_login": pl.String,
        },
    )
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "REVIEW_COMMENTED",
                "label_name": None,
                "actor_login": "bob",
            },
        ]
    )
    out = pipeline_stages(prs, ev, pr_merged_col=None)
    row = out.row(0, named=True)
    assert row["merged_at_effective"] is None
    assert row["seconds_open_to_first_touch"] == _SECONDS_PER_DAY
    assert row["seconds_ready_to_merge_to_merged"] is None
    assert row["seconds_open_to_merged"] is None


# ---------------------------------------------------------------------------
# bot_actor_expr — the three-leg predicate and the three artifact shapes
# ---------------------------------------------------------------------------

# A real machine-user node id (leanprover-community-bot-assistant), so the
# test breaks if the constant is edited without thinking.
_MACHINE_NODE_ID = "U_kgDOBcsTTQ"


def _typed_events(
    rows: list[dict],
    *,
    type_dtype: pl.DataType = pl.String,
    node_dtype: pl.DataType = pl.String,
) -> pl.DataFrame:
    """Events frame carrying ``actor_type`` / ``actor_node_id``.

    ``type_dtype`` / ``node_dtype`` let a test reproduce the mid-drain export
    shape, where the columns exist but arrive as all-null ``Float64``.
    """
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "occurred_at": pl.Datetime("us", "UTC"),
            "type": pl.String,
            "label_name": pl.String,
            "actor_login": pl.String,
            "actor_type": type_dtype,
            "actor_node_id": node_dtype,
        },
    )


def _row(
    login: str | None,
    actor_type: str | None = None,
    node_id: str | None = None,
    *,
    pr: int = 1,
    day: int = 2,
    ev_type: str = "ISSUE_COMMENTED",
) -> dict:
    return {
        "pull_request_id": pr,
        "occurred_at": _dt(day),
        "type": ev_type,
        "label_name": None,
        "actor_login": login,
        "actor_type": actor_type,
        "actor_node_id": node_id,
    }


def _flags(df: pl.DataFrame) -> list[bool]:
    return df.select(bot_actor_expr(df).alias("b"))["b"].to_list()


def test_bot_actor_expr_type_leg_catches_unlisted_app() -> None:
    """A ``Bot``-typed account absent from the login list is still a bot.

    This is the whole point of consuming ``actor_type``: the next GitHub App
    the maintainers add is classified with no code change.
    """
    ev = _typed_events(
        [
            _row("brand-new-app", "Bot", "BOT_kgDOsomething"),
            _row("alice", "User", "MDQ6VXNlcjE="),
        ]
    )
    assert "brand-new-app" not in DEFAULT_BOT_ACTORS
    assert _flags(ev) == [True, False]


def test_bot_actor_expr_node_id_leg_catches_machine_user() -> None:
    """Machine users report ``User``; only the node-id leg can catch them."""
    ev = _typed_events([_row("renamed-since-ingest", "User", _MACHINE_NODE_ID)])
    assert _MACHINE_NODE_ID in MACHINE_USER_NODE_IDS
    # Neither the type leg nor the login leg would fire here.
    assert "renamed-since-ingest" not in DEFAULT_BOT_ACTORS
    assert _flags(ev) == [True]


def test_bot_actor_expr_login_leg_catches_untyped_bot() -> None:
    """``actor_type IS NULL`` + a known automation login is still a bot.

    Mirrors ``leanprover-community-mathlib4-bot``: 41,329 events with no
    ``actor_type`` *and* no ``actor_node_id``, so login is the only key.
    """
    ev = _typed_events([_row("leanprover-community-mathlib4-bot", None, None)])
    assert _flags(ev) == [True]


def test_bot_actor_expr_null_type_is_not_user() -> None:
    """A null ``actor_type`` on an unknown actor means unknown, not bot.

    The complement of the previous test: ``fill_null(False)`` must make the
    predicate false rather than null, or ``~expr`` would silently drop the row.
    """
    ev = _typed_events([_row("some-human", None, None)])
    flags = _flags(ev)
    assert flags == [False]
    assert flags[0] is not None


def test_bot_actor_expr_null_login_with_bot_type() -> None:
    """A typed ``Bot`` with no login at all is still classified."""
    ev = _typed_events([_row(None, "Bot", "BOT_kgDOx")])
    assert _flags(ev) == [True]


def test_bot_actor_expr_login_leg_is_case_insensitive() -> None:
    ev = _typed_events([_row("GitHub-Actions", None, None)])
    assert _flags(ev) == [True]


# --- the three artifact shapes ---------------------------------------------


def test_bot_actor_expr_shape_1_columns_absent() -> None:
    """Shape 1: an export predating the deploy has neither column."""
    ev = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": login,
            }
            for login in ("mathlib-bors", "alice")
        ]
    )
    assert "actor_type" not in ev.columns
    assert _flags(ev) == [True, False]


def test_bot_actor_expr_shape_2_all_null_float_columns() -> None:
    """Shape 2: a mid-drain export types the columns ``Float64``, all null.

    ``pl.Float64`` on ``actor_type`` means "no data", not "no bots" — the
    leg must be dropped and the login list must still classify. Comparing
    a ``Float64`` column against the string ``"Bot"`` would otherwise raise
    or silently yield null for every row.
    """
    ev = _typed_events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": login,
                "actor_type": None,
                "actor_node_id": None,
            }
            for login in ("mathlib-bors", "alice")
        ],
        type_dtype=pl.Float64,
        node_dtype=pl.Float64,
    )
    assert ev.schema["actor_type"] == pl.Float64
    assert _flags(ev) == [True, False]


def test_bot_actor_expr_shape_3_string_columns() -> None:
    """Shape 3: the post-drain target — all three legs live."""
    ev = _typed_events(
        [
            _row("brand-new-app", "Bot", "BOT_kgDOz"),
            _row("whatever", "User", _MACHINE_NODE_ID),
            _row("mathlib-bors", None, None),
            _row("alice", "User", "MDQ6VXNlcjE="),
        ]
    )
    assert _flags(ev) == [True, True, True, False]


def test_bot_actor_expr_no_usable_columns_yields_false() -> None:
    """With no login column and no typed columns, nothing can be classified."""
    ev = pl.DataFrame({"pull_request_id": [1]}, schema={"pull_request_id": pl.Int64})
    assert _flags(ev) == [False]


def test_bot_actor_expr_empty_bot_actors_keeps_type_leg() -> None:
    """Passing ``bot_actors=[]`` disables only the login leg."""
    ev = _typed_events(
        [_row("brand-new-app", "Bot", "BOT_kgDOz"), _row("mathlib-bors", None, None)]
    )
    flags = ev.select(bot_actor_expr(ev, bot_actors=[]).alias("b"))["b"].to_list()
    assert flags == [True, False]


# --- end-to-end through the consumers --------------------------------------


def test_first_review_touch_skips_bot_typed_actor() -> None:
    """An unlisted ``Bot`` no longer counts as the first human touch."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _typed_events(
        [
            _row("brand-new-app", "Bot", "BOT_kgDOz", day=2),
            _row("bob", "User", "MDQ6VXNlcjE=", day=3),
        ]
    )
    out = first_review_touch(prs, ev)
    row = out.row(0, named=True)
    assert row["first_touch_actor"] == "bob"
    assert row["first_touch_at"] == _dt(3)


def test_first_review_touch_skips_untyped_known_bot() -> None:
    """The regression the login leg exists to prevent.

    A ``mathlib-dependent-issues`` comment carries ``actor_type IS NULL``
    because the bot deletes and reposts its own comment, so the node id can
    never be re-resolved. Filtering on ``actor_type == "Bot"`` alone would
    count it as a human review touch.
    """
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    ev = _typed_events(
        [
            _row("mathlib-dependent-issues", None, None, day=2),
            _row("bob", "User", "MDQ6VXNlcjE=", day=3),
        ]
    )
    out = first_review_touch(prs, ev)
    assert out.row(0, named=True)["first_touch_actor"] == "bob"


def test_first_review_touch_unchanged_without_typed_columns() -> None:
    """Backward compatibility: the login-only path still behaves identically."""
    prs = _prs_frame([{"id": 1, "gh_created_at": _dt(1), "author_login": "alice"}])
    rows = [
        {
            "pull_request_id": 1,
            "occurred_at": _dt(day),
            "type": "ISSUE_COMMENTED",
            "label_name": None,
            "actor_login": login,
        }
        for day, login in ((2, "mathlib-bors"), (3, "bob"))
    ]
    out = first_review_touch(prs, _events(rows))
    assert out.row(0, named=True)["first_touch_actor"] == "bob"
