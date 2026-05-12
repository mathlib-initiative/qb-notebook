"""Reconstruct PR review-state intervals from labels or queue windows.

Mathlib's review handoff is encoded as labels (`awaiting-author`,
`WIP`, `maintainer-merge`, `ready-to-merge`, ...). :func:`label_intervals`
turns `LABELED` / `UNLABELED` timeline events into per-PR per-label
intervals so downstream analyses can talk in terms of state durations
and transitions instead of raw events.

The legacy `awaiting-review` label (2021-08 → 2024-07) is still
reconstructable here for historical cohorts, but is no longer applied
to current PRs — the "in reviewers' court" state has been implicit
since mid-2024 (PR open + not `awaiting-author` / `WIP`), with
`analyzer_prqueuewindow` ruleset 3 as the closest machine-defined
proxy. :func:`queue_window_intervals` exposes those windows in the
same shape as :func:`label_intervals` so the two regimes can be
analyzed and compared with the same downstream helpers.

This module also exposes :func:`attribute_label_events`, which
attributes a bot-applied label (e.g. `maintainer-merge`,
`ready-to-merge`) to the human who triggered it via a comment or
review event shortly before the label was applied,
:func:`label_overlap_seconds`, a generic per-PR interval-vs-window
overlap helper used wherever a "did state X cover interval Y" question
shows up, and :func:`labels_active_at`, which given a set of
``(pull_request_id, timestamp)`` points returns the label intervals
that were active at each point — the "which area / state was this PR
in when event E fired" lookup used by per-area attribution.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import polars as pl

from qb_notebook.intervals import _resolve_asof

# Bot accounts that apply labels in response to human comments. Used by
# :func:`attribute_label_events` to skip the bot when looking for the
# human trigger.
DEFAULT_BOT_ACTORS: frozenset[str] = frozenset(
    {
        "github-actions",
        "leanprover-community-mathlib4-bot",
        "leanprover-community-bot-assistant",
        "mathlib-triage",
        "mathlib4-merge-conflict-bot",
        "mathlib4-dependent-issues-bot",
        "dependabot",
    }
)

# Timeline event types that count as a human "trigger" preceding a
# bot-applied label. Top-level PR comments (`ISSUE_COMMENTED`) carry the
# `bors merge` / `maintainer merge` commands; the review-event types are
# included so explicit GitHub reviews also count.
DEFAULT_TRIGGER_EVENT_TYPES: tuple[str, ...] = (
    "ISSUE_COMMENTED",
    "REVIEW_APPROVED",
    "REVIEW_COMMENTED",
    "REVIEW_CHANGES_REQUESTED",
)


def label_intervals(
    df_events: pl.DataFrame,
    label_name: str | Iterable[str],
    *,
    asof: datetime | None = None,
) -> pl.DataFrame:
    """Per-PR, per-label intervals reconstructed from LABELED/UNLABELED events.

    For each PR and each requested label, walks the timeline and pairs each
    apply event with its next remove event. The state machine only
    transitions on the leading edge: a redundant LABELED while the label is
    already on (or UNLABELED while it is already off) is a no-op. This
    matches how GitHub renders the label history and is robust to
    occasional duplicate events in the syncer dump.

    Open intervals (still applied at ``asof``) keep a null ``end`` and a
    non-null ``end_effective`` equal to ``asof``.

    Returns columns:
        ``pull_request_id``, ``label_name``, ``start``, ``end``,
        ``applied_by``, ``removed_by``, ``is_open``, ``end_effective``,
        ``duration``, ``duration_hours``, ``duration_days``.
    """
    asof_dt = _resolve_asof(asof)
    labels = [label_name] if isinstance(label_name, str) else list(label_name)

    sorted_events = (
        df_events.filter(pl.col("type").is_in(["LABELED", "UNLABELED"]))
        .filter(pl.col("label_name").is_in(labels))
        .select(
            [
                "pull_request_id",
                "label_name",
                "occurred_at",
                "type",
                "actor_login",
            ]
        )
        .drop_nulls(["pull_request_id", "occurred_at", "label_name"])
        .with_columns(
            # If LABELED and UNLABELED share occurred_at, LABELED first.
            pl.when(pl.col("type") == "LABELED")
            .then(0)
            .otherwise(1)
            .cast(pl.Int32)
            .alias("order")
        )
        .sort(["pull_request_id", "label_name", "occurred_at", "order"])
    )

    # Collapse consecutive same-type events (e.g. L L U → L U), then strip
    # any leading UNLABELED per (PR, label). After this pass each
    # (PR, label) group is strictly alternating L, U, L, U, … starting with L,
    # so the i-th LABELED pairs naturally with the i-th UNLABELED.
    dedup = (
        sorted_events.with_columns(
            pl.col("type")
            .shift(1)
            .over(["pull_request_id", "label_name"])
            .alias("prev_type")
        )
        .filter(pl.col("prev_type").is_null() | (pl.col("type") != pl.col("prev_type")))
        .with_columns(
            pl.int_range(pl.len())
            .over(["pull_request_id", "label_name"])
            .alias("group_pos")
        )
        .filter(~((pl.col("group_pos") == 0) & (pl.col("type") == "UNLABELED")))
    )

    indexed = dedup.with_columns(
        [
            pl.when(pl.col("type") == "LABELED")
            .then(1)
            .otherwise(0)
            .cast(pl.Int32)
            .cum_sum()
            .over(["pull_request_id", "label_name"])
            .alias("labeled_idx"),
            pl.when(pl.col("type") == "UNLABELED")
            .then(1)
            .otherwise(0)
            .cast(pl.Int32)
            .cum_sum()
            .over(["pull_request_id", "label_name"])
            .alias("unlabeled_idx"),
        ]
    )

    starts = indexed.filter(pl.col("type") == "LABELED").select(
        [
            "pull_request_id",
            "label_name",
            pl.col("labeled_idx").alias("interval_idx"),
            pl.col("occurred_at").alias("start"),
            pl.col("actor_login").alias("applied_by"),
        ]
    )
    ends = indexed.filter(pl.col("type") == "UNLABELED").select(
        [
            "pull_request_id",
            "label_name",
            pl.col("unlabeled_idx").alias("interval_idx"),
            pl.col("occurred_at").alias("end"),
            pl.col("actor_login").alias("removed_by"),
        ]
    )

    return (
        starts.join(
            ends,
            on=["pull_request_id", "label_name", "interval_idx"],
            how="left",
        )
        .drop("interval_idx")
        .sort(["pull_request_id", "label_name", "start"])
        .with_columns(
            [
                pl.col("end").is_null().alias("is_open"),
                pl.coalesce([pl.col("end"), pl.lit(asof_dt)]).alias("end_effective"),
            ]
        )
        .with_columns((pl.col("end_effective") - pl.col("start")).alias("duration"))
        .with_columns(
            [
                (pl.col("duration").dt.total_seconds() / 3600.0).alias(
                    "duration_hours"
                ),
                (pl.col("duration").dt.total_seconds() / 86400.0).alias(
                    "duration_days"
                ),
            ]
        )
    )


def stage_timestamps(
    df_events: pl.DataFrame,
    *,
    label_order: Iterable[str] = (
        "awaiting-review",
        "maintainer-merge",
        "ready-to-merge",
    ),
) -> pl.DataFrame:
    """First-application timestamp per PR for a sequence of review labels.

    Returns one row per ``pull_request_id`` with one ``first_<label>``
    column per label in ``label_order``. Useful for stage-by-stage
    cumulative-latency plots; the caller pairs these against
    ``gh_created_at`` and ``merged_at`` from the PR table.
    """
    labels = list(label_order)
    firsts = (
        df_events.filter(pl.col("type") == "LABELED")
        .filter(pl.col("label_name").is_in(labels))
        .drop_nulls(["pull_request_id", "occurred_at", "label_name"])
        .group_by(["pull_request_id", "label_name"])
        .agg(pl.col("occurred_at").min().alias("first_at"))
    )
    wide = firsts.pivot(
        on="label_name",
        index="pull_request_id",
        values="first_at",
        aggregate_function="first",
    )
    for label in labels:
        if label not in wide.columns:
            wide = wide.with_columns(
                pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(label)
            )
    rename = {label: f"first_{label.replace('-', '_')}" for label in labels}
    return wide.rename(rename).select(
        ["pull_request_id", *[rename[label] for label in labels]]
    )


def attribute_label_events(
    df_events: pl.DataFrame,
    label_name: str,
    *,
    window_seconds: int = 600,
    bot_actors: Iterable[str] = DEFAULT_BOT_ACTORS,
    trigger_types: Iterable[str] = DEFAULT_TRIGGER_EVENT_TYPES,
) -> pl.DataFrame:
    """Attribute bot-applied ``LABELED`` events to the human who triggered them.

    For each ``LABELED(label_name)`` event in ``df_events``, looks
    backward up to ``window_seconds`` seconds on the same PR for the most
    recent non-bot timeline event whose ``type`` is in ``trigger_types``,
    and returns that actor as the inferred trigger.

    Rationale: mathlib's review and merge workflows are bot-driven —
    a reviewer comments ``maintainer merge`` (or ``bors r+`` for merges)
    and a bot applies the label seconds later. ``actor_login`` on the
    ``LABELED`` event is therefore the bot. The most recent human
    activity on the PR within a short window is a high-quality proxy
    for the trigger.

    Empirically, a 10-minute window attributes >97 % of
    ``maintainer-merge`` and ``ready-to-merge`` labels in mathlib4 to a
    real reviewer / maintainer.

    Returns one row per ``LABELED(label_name)`` event with columns:

    - ``pull_request_id``, ``label_at`` (event timestamp),
      ``label_actor`` (the bot or human who applied the label),
    - ``inferred_actor`` — null if no qualifying event found in window,
    - ``trigger_event_type`` (``ISSUE_COMMENTED`` / ``REVIEW_APPROVED`` …),
    - ``trigger_at`` (timestamp of the trigger),
    - ``gap_seconds`` (``label_at - trigger_at`` in seconds),
    - ``attributed`` (bool — ``inferred_actor`` is not null).
    """
    bots = list(bot_actors)
    trigger_list = list(trigger_types)

    labels = (
        df_events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == label_name)
        )
        .drop_nulls(["pull_request_id", "occurred_at"])
        .select(
            [
                "pull_request_id",
                pl.col("occurred_at").alias("label_at"),
                pl.col("actor_login").alias("label_actor"),
            ]
        )
    )

    triggers = (
        df_events.filter(pl.col("type").is_in(trigger_list))
        .filter(
            ~pl.col("actor_login").is_in(bots) & pl.col("actor_login").is_not_null()
        )
        .drop_nulls(["pull_request_id", "occurred_at"])
        .select(
            [
                "pull_request_id",
                pl.col("occurred_at").alias("trigger_at"),
                pl.col("actor_login").alias("inferred_actor"),
                pl.col("type").alias("trigger_event_type"),
            ]
        )
    )

    # In-window candidates only: rank by smallest non-negative gap and keep
    # one per label event. Labels with no qualifying candidate are reattached
    # via left-join below so they still appear in the output with null
    # inferred_actor.
    in_window = (
        labels.join(triggers, on="pull_request_id", how="inner")
        .with_columns(
            ((pl.col("label_at") - pl.col("trigger_at")).dt.total_seconds()).alias(
                "gap_seconds"
            )
        )
        .filter(
            (pl.col("gap_seconds") >= 0) & (pl.col("gap_seconds") <= window_seconds)
        )
        .sort(["pull_request_id", "label_at", "gap_seconds"])
        .unique(subset=["pull_request_id", "label_at"], keep="first")
        .select(
            [
                "pull_request_id",
                "label_at",
                "inferred_actor",
                "trigger_event_type",
                "trigger_at",
                "gap_seconds",
            ]
        )
    )

    return (
        labels.join(in_window, on=["pull_request_id", "label_at"], how="left")
        .with_columns(pl.col("inferred_actor").is_not_null().alias("attributed"))
        .select(
            [
                "pull_request_id",
                "label_at",
                "label_actor",
                "inferred_actor",
                "trigger_event_type",
                "trigger_at",
                "gap_seconds",
                "attributed",
            ]
        )
    )


def label_overlap_seconds(
    df_intervals: pl.DataFrame,
    df_windows: pl.DataFrame,
    *,
    interval_pr_col: str = "pull_request_id",
    interval_start_col: str = "start",
    interval_end_col: str = "end_effective",
    window_pr_col: str = "pull_request_id",
    window_start_col: str = "window_start",
    window_end_col: str = "window_end",
    overlap_col: str = "overlap_seconds",
    had_overlap_col: str = "had_overlap",
) -> pl.DataFrame:
    """Sum interval-window overlap seconds per row of ``df_windows``.

    ``df_intervals`` carries zero or more intervals per ``pull_request_id``
    (typically the output of :func:`label_intervals` for a single label,
    using ``end_effective`` so open intervals are closed at ``asof``).
    ``df_windows`` has one row per (PR, window) pair with non-null
    ``window_start`` and ``window_end``.

    For each window row, the helper restricts ``df_intervals`` to the same
    PR, intersects each interval with ``[window_start, window_end]``, and
    sums the positive intersections in seconds. Rows with no matching
    interval, or whose intersections are all non-positive, get ``0.0``.

    Returns ``df_windows`` with two added columns: ``overlap_seconds``
    (float, total overlap in seconds) and ``had_overlap`` (bool,
    ``overlap_seconds > 0``).
    """
    ints = df_intervals.select(
        [
            pl.col(interval_pr_col).alias("_pr"),
            pl.col(interval_start_col).alias("_istart"),
            pl.col(interval_end_col).alias("_iend"),
        ]
    ).drop_nulls(["_pr", "_istart", "_iend"])

    wins_indexed = df_windows.with_row_index("_row_idx")
    wins = wins_indexed.select(
        [
            "_row_idx",
            pl.col(window_pr_col).alias("_pr"),
            pl.col(window_start_col).alias("_wstart"),
            pl.col(window_end_col).alias("_wend"),
        ]
    )

    pairs = wins.join(ints, on="_pr", how="inner")
    overlap = (
        pairs.with_columns(
            [
                pl.max_horizontal("_istart", "_wstart").alias("_lo"),
                pl.min_horizontal("_iend", "_wend").alias("_hi"),
            ]
        )
        .with_columns(
            pl.when(pl.col("_hi") > pl.col("_lo"))
            .then((pl.col("_hi") - pl.col("_lo")).dt.total_seconds().cast(pl.Float64))
            .otherwise(0.0)
            .alias("_overlap")
        )
        .group_by("_row_idx")
        .agg(pl.col("_overlap").sum().alias(overlap_col))
    )

    return (
        wins_indexed.join(overlap, on="_row_idx", how="left")
        .with_columns(pl.col(overlap_col).fill_null(0.0))
        .with_columns((pl.col(overlap_col) > 0).alias(had_overlap_col))
        .drop("_row_idx")
    )


def labels_active_at(
    df_intervals: pl.DataFrame,
    df_points: pl.DataFrame,
    *,
    point_pr_col: str = "pull_request_id",
    point_time_col: str = "at",
    interval_pr_col: str = "pull_request_id",
    interval_start_col: str = "start",
    interval_end_col: str = "end_effective",
    interval_label_col: str = "label_name",
) -> pl.DataFrame:
    """Find label intervals active at each ``(pull_request_id, timestamp)`` point.

    Joins ``df_points`` to ``df_intervals`` on ``pull_request_id`` and keeps
    rows where ``interval_start_col <= point_time_col < interval_end_col``.
    The interval frame is typically the output of :func:`label_intervals`
    for one or more labels (using ``end_effective`` closes any still-open
    intervals at ``asof``); the points frame is one row per event you want
    to attribute to a label state — e.g. a `LABELED(maintainer-merge)`
    timestamp, or a PR's effective merge time, for the "which `t-*` area
    was this PR in when X happened" question.

    A point with N active intervals produces N output rows (so callers can
    decide whether to count each area or pick one). Points with no active
    interval get no row in the output; left-join to recover them if
    needed.

    Returns ``df_points`` columns plus a ``label_name`` column carrying
    the interval label. Other columns from ``df_intervals`` are dropped to
    keep the output small.
    """
    points_indexed = df_points.with_row_index("_point_idx")
    pts = points_indexed.select(
        [
            "_point_idx",
            pl.col(point_pr_col).alias("_pr"),
            pl.col(point_time_col).alias("_at"),
        ]
    ).drop_nulls(["_pr", "_at"])

    ints = df_intervals.select(
        [
            pl.col(interval_pr_col).alias("_pr"),
            pl.col(interval_start_col).alias("_istart"),
            pl.col(interval_end_col).alias("_iend"),
            pl.col(interval_label_col).alias("label_name"),
        ]
    ).drop_nulls(["_pr", "_istart", "_iend", "label_name"])

    matched = (
        pts.join(ints, on="_pr", how="inner")
        .filter(
            (pl.col("_at") >= pl.col("_istart")) & (pl.col("_at") < pl.col("_iend"))
        )
        .select(["_point_idx", "label_name"])
    )
    return points_indexed.join(matched, on="_point_idx", how="inner").drop("_point_idx")


_COURT_COMMON_COLS = (
    "pull_request_id",
    "source",
    "start",
    "end",
    "is_open",
    "end_effective",
    "duration",
    "duration_hours",
    "duration_days",
)


def reviewers_court_intervals(
    df_events: pl.DataFrame,
    df_queue_windows: pl.DataFrame,
    *,
    rule_set_id: int | None = 3,
    label: str = "awaiting-review",
    asof: datetime | None = None,
    label_asof: datetime | None = None,
) -> pl.DataFrame:
    """Unified per-PR "in reviewers' court" intervals.

    The reviewer-court state on mathlib4 is recorded by two sources
    with overlapping but non-identical coverage:

    - `analyzer_prqueuewindow` ruleset 3 — machine-defined queue
      windows, available 2021-05 → present, the analyzer's primary
      definition of "PR is on the queue".
    - The retired `awaiting-review` label — explicit reviewer-court
      state used 2021-08 → 2024-07; ~98 % median Jaccard with the
      queue window where both exist, but ~219 PRs in the dump have
      label intervals without a corresponding queue-window row.

    This helper combines them with queue-windows as primary and the
    label as fallback per-PR (i.e. label intervals contribute only
    for PRs that do not appear in the ruleset). The result is one row
    per interval with a uniform shape that downstream helpers like
    :func:`label_overlap_seconds` can consume directly.

    Parameters
    ----------
    label_asof:
        Clamp open label-source intervals at this timestamp instead of
        ``asof``. Needed when ``label`` has been retired: deleting a
        label from the repo does not emit ``UNLABELED`` events, so a
        handful of historical applications stay "open" forever. For
        mathlib's retired ``awaiting-review`` pass
        ``datetime(2024, 7, 10, tzinfo=timezone.utc)``.

    Returns columns:
        ``pull_request_id``, ``source`` (``"queue_window"`` or
        ``"label"``), ``start``, ``end``, ``is_open``,
        ``end_effective``, ``duration``, ``duration_hours``,
        ``duration_days``.
    """
    qw = queue_window_intervals(
        df_queue_windows, rule_set_id=rule_set_id, asof=asof
    ).with_columns(pl.lit("queue_window").alias("source"))

    lbl_asof_arg = label_asof if label_asof is not None else asof
    lbl = label_intervals(df_events, label, asof=lbl_asof_arg).with_columns(
        pl.lit("label").alias("source")
    )

    qw_prs = qw.select("pull_request_id").unique()
    lbl_fallback = lbl.join(qw_prs, on="pull_request_id", how="anti")

    common = list(_COURT_COMMON_COLS)
    return pl.concat([qw.select(common), lbl_fallback.select(common)]).sort(
        ["pull_request_id", "start"]
    )


def queue_window_intervals(
    df_queue_windows: pl.DataFrame,
    *,
    rule_set_id: int | None = 3,
    asof: datetime | None = None,
) -> pl.DataFrame:
    """Queue-window intervals reshaped to match :func:`label_intervals` output.

    The analyzer's `analyzer_prqueuewindow` table records, per ruleset,
    each window during which a PR was "on the queue" — i.e. eligible for
    reviewer attention. For mathlib4 ruleset 3 is the one driving the
    dashboard. Each row is one (PR, window) pair; windows with a null
    `to_ts` are still open at the artifact snapshot time.

    This helper rewrites those rows into the same column convention as
    :func:`label_intervals` so downstream tools (e.g.
    :func:`label_overlap_seconds`) can consume label-derived and
    queue-window-derived state interchangeably. Queue-window-specific
    metadata is preserved alongside.

    Open windows (still on queue at ``asof``) keep a null ``end`` and a
    non-null ``end_effective`` equal to ``asof``.

    Returns columns:
        ``pull_request_id``, ``rule_set_id``, ``cycle_index``,
        ``window_count``, ``first_on_queue_ts``, ``opened_by_event_type``,
        ``closed_by_event_type``, ``start``, ``end``, ``is_open``,
        ``end_effective``, ``duration``, ``duration_hours``,
        ``duration_days``.
    """
    asof_dt = _resolve_asof(asof)

    df = df_queue_windows
    if rule_set_id is not None:
        df = df.filter(pl.col("rule_set_id") == rule_set_id)

    return (
        df.drop_nulls(["pull_request_id", "from_ts"])
        .select(
            [
                "pull_request_id",
                "rule_set_id",
                "cycle_index",
                "window_count",
                "first_on_queue_ts",
                "opened_by_event_type",
                "closed_by_event_type",
                pl.col("from_ts").alias("start"),
                pl.col("to_ts").alias("end"),
            ]
        )
        .with_columns(
            [
                pl.col("end").is_null().alias("is_open"),
                pl.coalesce([pl.col("end"), pl.lit(asof_dt)]).alias("end_effective"),
            ]
        )
        .with_columns((pl.col("end_effective") - pl.col("start")).alias("duration"))
        .with_columns(
            [
                (pl.col("duration").dt.total_seconds() / 3600.0).alias(
                    "duration_hours"
                ),
                (pl.col("duration").dt.total_seconds() / 86400.0).alias(
                    "duration_days"
                ),
            ]
        )
        .sort(["pull_request_id", "start"])
    )
