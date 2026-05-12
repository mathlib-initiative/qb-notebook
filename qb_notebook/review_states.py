"""Reconstruct label-state intervals from PR timeline events.

Mathlib's review handoff is encoded as labels (`awaiting-review`,
`awaiting-author`, `WIP`, `maintainer-merge`, ...). This module turns
`LABELED` / `UNLABELED` timeline events into per-PR per-label intervals
so downstream analyses can talk in terms of state durations and
transitions instead of raw events.

It also exposes :func:`attribute_label_events`, which attributes a
bot-applied label (e.g. `maintainer-merge`, `ready-to-merge`) to the
human who triggered it via a comment or review event shortly before
the label was applied.
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
