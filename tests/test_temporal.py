from datetime import datetime, timezone

import polars as pl

from qb_notebook.temporal import (
    MONTH_LABELS,
    WEEKDAY_LABELS,
    WEEKEND_DAYS,
    actor_activity_window,
    hour_set_overlap,
    weekday_hour_histogram,
    with_temporal_columns,
)


def _events(rows: list[dict]) -> pl.DataFrame:
    """Minimal events frame with the columns the temporal helpers read."""
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


def _dt(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


# ---------- constants --------------------------------------------------------


def test_label_constants_have_expected_shape() -> None:
    assert len(WEEKDAY_LABELS) == 7
    assert WEEKDAY_LABELS[0] == "Mon"
    assert WEEKDAY_LABELS[6] == "Sun"
    assert len(MONTH_LABELS) == 12
    assert MONTH_LABELS[0] == "Jan"
    assert MONTH_LABELS[11] == "Dec"
    assert WEEKEND_DAYS == {5, 6}


# ---------- with_temporal_columns -------------------------------------------


def test_with_temporal_columns_known_timestamp() -> None:
    # 2024-07-10 was a Wednesday; weekday() returns 3 -> 0-indexed 2.
    df = pl.DataFrame(
        {"occurred_at": [_dt(2024, 7, 10, 14)]},
        schema={"occurred_at": pl.Datetime("us", "UTC")},
    )
    out = with_temporal_columns(df, "occurred_at")
    row = out.row(0, named=True)
    assert row["hour_utc"] == 14
    assert row["weekday"] == 2  # Wed
    assert row["is_weekend"] is False
    assert row["month"] == 7
    assert row["year"] == 2024
    # year_month is the Date for the first of that month.
    assert str(row["year_month"]) == "2024-07-01"


def test_with_temporal_columns_weekend_flag() -> None:
    # 2024-07-13 = Saturday (weekday-1 = 5), 2024-07-14 = Sunday (6).
    df = pl.DataFrame(
        {
            "ts": [
                _dt(2024, 7, 12, 9),  # Fri
                _dt(2024, 7, 13, 9),  # Sat
                _dt(2024, 7, 14, 9),  # Sun
                _dt(2024, 7, 15, 9),  # Mon
            ]
        },
        schema={"ts": pl.Datetime("us", "UTC")},
    )
    out = with_temporal_columns(df, "ts")
    assert out["is_weekend"].to_list() == [False, True, True, False]
    assert out["weekday"].to_list() == [4, 5, 6, 0]


def test_with_temporal_columns_null_ts_propagates() -> None:
    df = pl.DataFrame(
        {"occurred_at": [_dt(2024, 1, 1), None]},
        schema={"occurred_at": pl.Datetime("us", "UTC")},
    )
    out = with_temporal_columns(df, "occurred_at")
    null_row = out.row(1, named=True)
    assert null_row["hour_utc"] is None
    assert null_row["weekday"] is None
    assert null_row["is_weekend"] is None
    assert null_row["month"] is None
    assert null_row["year"] is None
    assert null_row["year_month"] is None


def test_with_temporal_columns_prefix_isolates_two_timestamps() -> None:
    df = pl.DataFrame(
        {
            "open_ts": [_dt(2024, 7, 10, 14)],
            "merge_ts": [_dt(2024, 7, 13, 3)],
        },
        schema={
            "open_ts": pl.Datetime("us", "UTC"),
            "merge_ts": pl.Datetime("us", "UTC"),
        },
    )
    out = with_temporal_columns(df, "open_ts", prefix="open_")
    out = with_temporal_columns(out, "merge_ts", prefix="merge_")
    row = out.row(0, named=True)
    assert row["open_hour_utc"] == 14
    assert row["open_weekday"] == 2
    assert row["merge_hour_utc"] == 3
    assert row["merge_weekday"] == 5
    assert row["merge_is_weekend"] is True


# ---------- weekday_hour_histogram ------------------------------------------


def test_weekday_hour_histogram_always_emits_168_cells() -> None:
    out = weekday_hour_histogram(
        pl.DataFrame(
            schema={"occurred_at": pl.Datetime("us", "UTC")},
        )
    )
    assert out.height == 7 * 24
    assert out["count"].sum() == 0
    # First row is (weekday=0, hour=0); last is (weekday=6, hour=23).
    assert out.row(0, named=True) == {"weekday": 0, "hour_utc": 0, "count": 0}
    assert out.row(-1, named=True) == {"weekday": 6, "hour_utc": 23, "count": 0}


def test_weekday_hour_histogram_counts_match() -> None:
    # 2024-07-10 14:00 = Wed (weekday 2), hour 14, two events
    # 2024-07-13 03:00 = Sat (weekday 5), hour 3, one event
    df = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2024, 7, 10, 14),
                "type": "LABELED",
                "label_name": "x",
                "actor_login": "a",
            },
            {
                "pull_request_id": 2,
                "occurred_at": _dt(2024, 7, 10, 14),
                "type": "LABELED",
                "label_name": "x",
                "actor_login": "b",
            },
            {
                "pull_request_id": 3,
                "occurred_at": _dt(2024, 7, 13, 3),
                "type": "LABELED",
                "label_name": "x",
                "actor_login": "c",
            },
        ]
    )
    out = weekday_hour_histogram(df)
    assert (
        out.filter((pl.col("weekday") == 2) & (pl.col("hour_utc") == 14))[
            "count"
        ].item()
        == 2
    )
    assert (
        out.filter((pl.col("weekday") == 5) & (pl.col("hour_utc") == 3))["count"].item()
        == 1
    )
    # Sanity: everything else is zero.
    assert out["count"].sum() == 3


def test_weekday_hour_histogram_drops_null_timestamps() -> None:
    df = _events(
        [
            {
                "pull_request_id": 1,
                "occurred_at": _dt(2024, 7, 10, 14),
                "type": "LABELED",
                "label_name": "x",
                "actor_login": "a",
            },
            {
                "pull_request_id": 2,
                "occurred_at": None,
                "type": "LABELED",
                "label_name": "x",
                "actor_login": "b",
            },
        ]
    )
    out = weekday_hour_histogram(df)
    assert out["count"].sum() == 1


# ---------- actor_activity_window -------------------------------------------


def _evenly_spread(actor: str, hours: list[int], start_day: int = 1) -> list[dict]:
    """Generate one event per (day, hour) for an actor; day rolls forward."""
    rows: list[dict] = []
    pr_id = 1
    for i, hour in enumerate(hours):
        rows.append(
            {
                "pull_request_id": pr_id,
                "occurred_at": _dt(2025, 1, start_day + (i // 24), hour),
                "type": "ISSUE_COMMENTED",
                "label_name": None,
                "actor_login": actor,
            }
        )
        pr_id += 1
    return rows


def test_actor_activity_window_basic() -> None:
    # alice has 10 events at hour 14, 10 at hour 15, 10 at hour 16.
    # Best 8-hour window covers some range containing 14-16; with
    # everything stacked in [14,16], the best window starts no later than
    # 14 - (8 - 3) = 9 and no earlier than 14. Either way it must
    # contain hours 14, 15, 16 and active_hours_share must be 1.0.
    rows = (
        _evenly_spread("alice", [14] * 10)
        + _evenly_spread("alice", [15] * 10, start_day=2)
        + _evenly_spread("alice", [16] * 10, start_day=3)
    )
    out = actor_activity_window(_events(rows), window_hours=8, min_events=5)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["actor_login"] == "alice"
    assert row["n_events"] == 30
    assert row["peak_hour"] in (14, 15, 16)
    assert set(row["active_hours"]) >= {14, 15, 16}
    assert row["active_hours_share"] == 1.0


def test_actor_activity_window_min_events_filters_low_volume() -> None:
    rows = _evenly_spread("alice", [10] * 30) + _evenly_spread(
        "bob", [10] * 5, start_day=2
    )
    out = actor_activity_window(_events(rows), window_hours=4, min_events=20)
    assert out["actor_login"].to_list() == ["alice"]


def test_actor_activity_window_handles_midnight_wrap() -> None:
    # Events at hours 22, 23, 0, 1, 2 — best contiguous 5-hour window
    # must wrap midnight (start=22, end=3 mod 24).
    hours = [22] * 5 + [23] * 5 + [0] * 5 + [1] * 5 + [2] * 5
    rows = _evenly_spread("alice", hours)
    out = actor_activity_window(_events(rows), window_hours=5, min_events=5)
    row = out.row(0, named=True)
    assert row["window_start"] == 22
    assert row["window_end"] == 3
    assert set(row["active_hours"]) == {22, 23, 0, 1, 2}
    assert row["active_hours_share"] == 1.0


def test_actor_activity_window_excludes_bots() -> None:
    rows = _evenly_spread("alice", [10] * 30) + _evenly_spread(
        "GitHub-Actions", [12] * 30, start_day=5
    )
    out = actor_activity_window(
        _events(rows),
        window_hours=4,
        min_events=20,
        bot_actors=frozenset({"github-actions"}),
    )
    assert out["actor_login"].to_list() == ["alice"]


def test_actor_activity_window_event_types_filter() -> None:
    rows = (
        _evenly_spread("alice", [10] * 30)
        + [
            {
                "pull_request_id": 999,
                "occurred_at": _dt(2025, 2, 1, 22),
                "type": "LABELED",
                "label_name": "x",
                "actor_login": "alice",
            }
        ]
        * 20
    )
    out = actor_activity_window(
        _events(rows),
        window_hours=4,
        min_events=10,
        event_types=("LABELED",),
    )
    row = out.row(0, named=True)
    # All qualifying events are at hour 22 (LABELED only).
    assert row["peak_hour"] == 22
    assert row["n_events"] == 20


def test_actor_activity_window_empty_input_returns_schema() -> None:
    out = actor_activity_window(
        _events([]),
        window_hours=8,
        min_events=20,
    )
    assert out.height == 0
    assert set(out.columns) == {
        "actor_login",
        "n_events",
        "peak_hour",
        "window_start",
        "window_end",
        "active_hours",
        "active_hours_share",
    }


# ---------- hour_set_overlap ------------------------------------------------


def test_hour_set_overlap_basic() -> None:
    assert hour_set_overlap([0, 1, 2, 3], [2, 3, 4, 5]) == 2
    assert hour_set_overlap([0, 1, 2], [10, 11, 12]) == 0
    assert hour_set_overlap([0, 1, 2], []) == 0
    assert hour_set_overlap([7, 7, 7], [7]) == 1


# ---------- bot exclusion via the typed columns ------------------------------


def _typed(rows: list[dict], actor_type: str | None, node_id: str | None) -> list[dict]:
    """Attach ``actor_type`` / ``actor_node_id`` to generated rows."""
    return [{**r, "actor_type": actor_type, "actor_node_id": node_id} for r in rows]


def _typed_events(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "occurred_at": pl.Datetime("us", "UTC"),
            "type": pl.String,
            "label_name": pl.String,
            "actor_login": pl.String,
            "actor_type": pl.String,
            "actor_node_id": pl.String,
        },
    )


def test_actor_activity_window_excludes_bot_typed_actor() -> None:
    """A ``Bot``-typed account is dropped even when its login isn't listed.

    Regression guard for the concrete case this change fixed: the
    dependency-bump bots showed up as ordinary actors in the timezone table,
    where their perfectly uniform schedule is meaningless.
    """
    rows = _typed(_evenly_spread("alice", [14] * 25), "User", "MDQ6VXNlcjE=")
    rows += _typed(_evenly_spread("brand-new-app", [3] * 25), "Bot", "BOT_kgDOz")
    out = actor_activity_window(_typed_events(rows), min_events=20)
    assert out["actor_login"].to_list() == ["alice"]


def test_actor_activity_window_excludes_machine_user_by_node_id() -> None:
    """Machine users report ``User`` — only the node-id leg catches them."""
    rows = _typed(_evenly_spread("alice", [14] * 25), "User", "MDQ6VXNlcjE=")
    rows += _typed(_evenly_spread("renamed-bot", [3] * 25), "User", "U_kgDOBcsTTQ")
    out = actor_activity_window(_typed_events(rows), min_events=20)
    assert out["actor_login"].to_list() == ["alice"]


def test_actor_activity_window_bot_exclusion_without_typed_columns() -> None:
    """Backward compatibility: login-only exclusion still works untyped."""
    rows = _evenly_spread("alice", [14] * 25)
    rows += _evenly_spread("mathlib-bors", [3] * 25)
    out = actor_activity_window(_events(rows), min_events=20)
    assert out["actor_login"].to_list() == ["alice"]


def test_actor_activity_window_exclude_bots_false_keeps_bots() -> None:
    """``exclude_bots=False`` bypasses the predicate entirely."""
    rows = _typed(_evenly_spread("alice", [14] * 25), "User", "MDQ6VXNlcjE=")
    rows += _typed(_evenly_spread("brand-new-app", [3] * 25), "Bot", "BOT_kgDOz")
    out = actor_activity_window(_typed_events(rows), min_events=20, exclude_bots=False)
    assert sorted(out["actor_login"].to_list()) == ["alice", "brand-new-app"]
