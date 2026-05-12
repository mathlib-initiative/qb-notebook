"""Reconstruct label-state intervals from PR timeline events.

Mathlib's review handoff is encoded as labels (`awaiting-review`,
`awaiting-author`, `WIP`, `maintainer-merge`, ...). This module turns
`LABELED` / `UNLABELED` timeline events into per-PR per-label intervals
so downstream analyses can talk in terms of state durations and
transitions instead of raw events.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import polars as pl

from qb_notebook.intervals import _resolve_asof


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
