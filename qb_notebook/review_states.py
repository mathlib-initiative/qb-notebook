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
:func:`first_review_touch`, which finds the earliest non-author,
non-bot review/comment event per PR for first-touch-latency analyses,
:func:`label_overlap_seconds`, a generic per-PR interval-vs-window
overlap helper used wherever a "did state X cover interval Y" question
shows up, :func:`labels_active_at`, which given a set of
``(pull_request_id, timestamp)`` points returns the label intervals
that were active at each point — the "which area / state was this PR
in when event E fired" lookup used by per-area attribution, and
:func:`inline_comment_stats`, which rolls
``syncer_prreviewinlinecomment`` rows up to per-PR review-depth
counts (comments, threads, distinct non-author reviewers, files
touched) for the inline-comment-volume cross-cuts, and
:func:`pipeline_stages`, which combines :func:`first_review_touch`
and :func:`stage_timestamps` into a single wide per-PR frame with the
four sequential stage-delta seconds the "anatomy of a merge" story
keys off.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Mapping

import polars as pl

from qb_notebook.intervals import _resolve_asof

# Canonical names for the two bot-applied labels used in the review workflow.
# Defined as constants so notebooks reference a typed symbol instead of repeating
# string literals (which are otherwise hand-typed across multiple notebooks).
MAINTAINER_MERGE_LABEL: str = "maintainer-merge"
READY_TO_MERGE_LABEL: str = "ready-to-merge"

# Default look-back window (seconds) for :func:`attribute_label_events`.
# Empirical coverage on mathlib4 with the TimelineEvent ingest:
# >99 % for `maintainer-merge`, ~98 % for `ready-to-merge`.
DEFAULT_ATTRIBUTION_WINDOW_SECONDS: int = 600

# Mathlib labels that have been retired from the repo. Label deletion does
# **not** emit ``UNLABELED`` events, so any interval whose apply event
# precedes the deletion stays "open" forever unless we clamp it. Notebooks
# that care about the current-state semantics (summary counts of
# `currently_open`, stuck-PR tables) should pass this map as
# ``label_asof_overrides`` to :func:`label_intervals`.
MATHLIB_LABEL_RETIRED_AT: Mapping[str, datetime] = {
    "awaiting-review": datetime(2024, 7, 10, tzinfo=timezone.utc),
}

# Bot accounts that apply labels in response to human comments. Used by
# :func:`attribute_label_events` to skip the bot when looking for the
# human trigger. The bors-family accounts (``mathlib-bors``, ``bors``,
# ``leanprover-radar``) post bot replies to ``bors r+`` / ``bors delegate``
# commands and would otherwise be picked up as the most-recent comment
# before the corresponding label was applied — important for ``delegated``
# attribution (where the bors reply normally lands seconds before the
# label) and also responsible for ~1 % of ``ready-to-merge`` mis-attributions.
# On **2026-02-03** the ``mathlib4-*`` bot logins stop dead and the
# ``mathlib-*`` ones start the next day. This was originally recorded here as
# a rename; it was not. Resolved against the live GraphQL API on 2026-08-15:
#
#   mathlib4-merge-conflict-bot    User  U_kgDODVl3LA
#   mathlib-merge-conflicts        Bot   BOT_kgDOD2_IkQ
#   mathlib4-dependent-issues-bot  User  U_kgDOCsITAQ
#   mathlib-dependent-issues       Bot   BOT_kgDOD2_cBQ
#
# The old logins still resolve to their original accounts, and the new ones
# are different accounts of a different kind — the machine-user bots were
# **replaced by GitHub Apps**. So both spellings must stay: the old accounts
# own all pre-2026-02-03 history and no key, node id included, bridges the
# substitution.
#
# Nothing in the exported data marks an actor as a bot *yet*, so this list is
# currently the only bot signal we have and it goes stale silently.
# queueboard-core design doc 051 adds ``actor_type`` / ``actor_node_id`` to
# ``syncer_prtimelineevent``; once a post-backfill export carries them, the
# ``Bot``-typed accounts here drop out of this list entirely and the residual
# machine users (``leanprover-community-*``, ``leanprover-radar``, and the two
# retired ``mathlib4-*`` accounts, all of which report ``User``) get keyed on
# node id instead of login. Until then: adding a name here changes every
# first-touch-derived metric, so prefer over- to under-inclusion, but never
# add a human — ``bottine`` and ``guptbot`` are human contributors whose logins
# merely look bot-like.
DEFAULT_BOT_ACTORS: frozenset[str] = frozenset(
    {
        "github-actions",
        "leanprover-community-mathlib4-bot",
        "leanprover-community-bot-assistant",
        "mathlib-triage",
        "mathlib4-merge-conflict-bot",
        "mathlib4-dependent-issues-bot",
        "dependabot",
        "mathlib-bors",
        "bors",
        "leanprover-radar",
        # GitHub Apps that took over from the ``mathlib4-*`` machine users
        # above on 2026-02-03. Separate accounts, not renames.
        "mathlib-merge-conflicts",
        "mathlib-dependent-issues",
        # Later additions and older accounts that were never listed.
        "mathlib-auto-merge",
        "mathlib-splicebot",
        "leanprover-bot",
        "mergify",
        "downstream-reports-automation",
        "botbaki-review",
        # Automated reviewers: genuine review signal, but not human.
        "copilot-pull-request-reviewer",
        "copilot-swe-agent",
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

# Timeline event types that count as a non-author "touch" on a PR — i.e.
# evidence that someone other than the author has engaged with the PR.
# Used by :func:`first_review_touch`. ``REVIEW_DISMISSED`` is included
# because a maintainer dismissing a stale review is a meaningful touch
# even if the dismisser doesn't simultaneously leave a comment.
# Callers can drop ``ISSUE_COMMENTED`` for a stricter "substantive review"
# variant that excludes top-level comments.
DEFAULT_TOUCH_EVENT_TYPES: tuple[str, ...] = (
    "REVIEW_APPROVED",
    "REVIEW_COMMENTED",
    "REVIEW_CHANGES_REQUESTED",
    "REVIEW_DISMISSED",
    "ISSUE_COMMENTED",
)


def label_intervals(
    df_events: pl.DataFrame,
    label_name: str | Iterable[str],
    *,
    asof: datetime | None = None,
    label_asof_overrides: Mapping[str, datetime] | None = None,
    df_pr_close: pl.DataFrame | None = None,
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

    ``label_asof_overrides`` lets the caller specify a per-label
    retirement timestamp for labels that were deleted from the repo (label
    deletion does **not** emit ``UNLABELED`` events). For each label in
    the mapping, intervals whose apply event precedes the override are
    treated as closed at the override: ``end_effective`` becomes the
    override and ``is_open`` becomes ``False``. ``end`` stays null because
    no real UNLABELED event was observed. Pass
    :data:`MATHLIB_LABEL_RETIRED_AT` to clamp mathlib4's retired labels
    (currently `awaiting-review`).

    ``df_pr_close`` lets the caller clamp open intervals at the PR's
    actual close time. GitHub does not auto-remove labels when a PR is
    closed (bors-merged or otherwise), so a label that was applied but
    never explicitly removed before the PR closed shows up as a phantom
    "open" interval running to ``asof``. Pass a frame with columns
    ``pull_request_id`` and ``closed_at`` (e.g.
    ``prs.select("id", "closed_at").rename({"id": "pull_request_id"})``):
    for each interval whose ``end`` is null and ``start <= closed_at``,
    ``end_effective`` becomes ``min(closed_at, end_effective)`` and
    ``is_open`` becomes ``False``. Intervals applied to a PR *after* the
    close (e.g. record-keeping re-labels of merged PRs) keep their open
    state, since the label genuinely is applied post-close.

    Returns columns:
        ``pull_request_id``, ``label_name``, ``start``, ``end``,
        ``applied_by``, ``removed_by``, ``is_open``, ``end_effective``,
        ``duration``, ``duration_hours``, ``duration_days``.
    """
    asof_dt = _resolve_asof(asof)
    labels = [label_name] if isinstance(label_name, str) else list(label_name)
    overrides = {k: v for k, v in (label_asof_overrides or {}).items() if k in labels}

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

    if overrides:
        # Per-label asof: open intervals for retired labels close at the
        # override timestamp instead of the global asof.
        override_expr = pl.lit(asof_dt)
        for _lbl, _ts in overrides.items():
            override_expr = (
                pl.when(pl.col("label_name") == _lbl)
                .then(pl.lit(_ts))
                .otherwise(override_expr)
            )
        end_eff_expr = pl.coalesce([pl.col("end"), override_expr]).alias(
            "end_effective"
        )
        # `is_open` is False for retired-label open intervals — the label state
        # is no longer applicable, even though no UNLABELED event was emitted.
        is_open_expr = (
            pl.col("end").is_null()
            & ~pl.col("label_name").is_in(list(overrides.keys()))
        ).alias("is_open")
    else:
        end_eff_expr = pl.coalesce([pl.col("end"), pl.lit(asof_dt)]).alias(
            "end_effective"
        )
        is_open_expr = pl.col("end").is_null().alias("is_open")

    out = (
        starts.join(
            ends,
            on=["pull_request_id", "label_name", "interval_idx"],
            how="left",
        )
        .drop("interval_idx")
        .sort(["pull_request_id", "label_name", "start"])
        .with_columns([is_open_expr, end_eff_expr])
    )

    if df_pr_close is not None:
        # Clamp open intervals at the PR's close time. Only intervals that
        # were applied before close get clamped — a label applied *after* a
        # PR closed (e.g. post-merge record-keeping) is left open.
        closes = df_pr_close.select(
            pl.col("pull_request_id"),
            pl.col("closed_at").alias("_pr_closed_at"),
        ).drop_nulls(["pull_request_id", "_pr_closed_at"])
        out = out.join(closes, on="pull_request_id", how="left")
        clamp_pred = (
            pl.col("is_open")
            & pl.col("_pr_closed_at").is_not_null()
            & (pl.col("start") <= pl.col("_pr_closed_at"))
        )
        out = out.with_columns(
            [
                pl.when(clamp_pred)
                .then(
                    pl.min_horizontal(pl.col("end_effective"), pl.col("_pr_closed_at"))
                )
                .otherwise(pl.col("end_effective"))
                .alias("end_effective"),
                pl.when(clamp_pred)
                .then(False)
                .otherwise(pl.col("is_open"))
                .alias("is_open"),
            ]
        ).drop("_pr_closed_at")

    return out.with_columns(
        (pl.col("end_effective") - pl.col("start")).alias("duration")
    ).with_columns(
        [
            (pl.col("duration").dt.total_seconds() / 3600.0).alias("duration_hours"),
            (pl.col("duration").dt.total_seconds() / 86400.0).alias("duration_days"),
        ]
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
    window_seconds: int = DEFAULT_ATTRIBUTION_WINDOW_SECONDS,
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


def first_review_touch(
    df_prs: pl.DataFrame,
    df_events: pl.DataFrame,
    *,
    event_types: Iterable[str] = DEFAULT_TOUCH_EVENT_TYPES,
    bot_actors: Iterable[str] = DEFAULT_BOT_ACTORS,
    pr_id_col: str = "id",
    pr_open_col: str = "gh_created_at",
    pr_author_col: str = "author_login",
) -> pl.DataFrame:
    """Per-PR earliest non-author, non-bot review-or-comment event.

    For each row in ``df_prs``, finds the earliest event in ``df_events``
    whose ``type`` is in ``event_types`` and whose ``actor_login`` is
    neither the PR author nor a known bot. Returns one row per PR; PRs
    with no qualifying event get null touch columns.

    ``df_prs`` must carry the PR id (``pr_id_col``), the PR open
    timestamp (``pr_open_col``), and the author's GitHub login
    (``pr_author_col``). The author login isn't on ``syncer_pullrequest``
    natively — callers should join ``core_user.github_login`` onto
    ``prs.author_id`` upstream::

        users = data["users"]  # core_user.parquet
        prs_with_author = df_prs.join(
            users.select(
                pl.col("id").alias("author_id"),
                pl.col("github_login").alias("author_login"),
            ),
            on="author_id",
            how="left",
        )

    Comparison is case-insensitive on both sides. PRs whose author login
    is null (e.g. deleted GitHub account) keep all events as candidate
    touches — the author exclusion silently no-ops.

    Returns columns: ``pull_request_id``, ``first_touch_at``,
    ``first_touch_actor``, ``first_touch_event_type``,
    ``first_touch_seconds_from_open`` (float seconds; null when no touch).
    """
    bots = frozenset(b.lower() for b in bot_actors)
    types = list(event_types)

    pr_keys = df_prs.select(
        pl.col(pr_id_col).alias("pull_request_id"),
        pl.col(pr_open_col).alias("_pr_open_at"),
        pl.col(pr_author_col).str.to_lowercase().alias("_author_lc"),
    )

    # Tie-break on actor_login so the test order is deterministic when
    # two qualifying events share occurred_at on the same PR.
    candidates = (
        df_events.filter(pl.col("type").is_in(types))
        .drop_nulls(["pull_request_id", "occurred_at", "actor_login"])
        .with_columns(pl.col("actor_login").str.to_lowercase().alias("_actor_lc"))
        .filter(~pl.col("_actor_lc").is_in(list(bots)))
        .join(pr_keys, on="pull_request_id", how="inner")
        .filter(
            (pl.col("_actor_lc") != pl.col("_author_lc"))
            | pl.col("_author_lc").is_null()
        )
        .sort(["pull_request_id", "occurred_at", "_actor_lc"])
        .unique(subset=["pull_request_id"], keep="first")
        .select(
            "pull_request_id",
            pl.col("occurred_at").alias("first_touch_at"),
            pl.col("actor_login").alias("first_touch_actor"),
            pl.col("type").alias("first_touch_event_type"),
            "_pr_open_at",
        )
        .with_columns(
            (pl.col("first_touch_at") - pl.col("_pr_open_at"))
            .dt.total_seconds()
            .cast(pl.Float64)
            .alias("first_touch_seconds_from_open")
        )
        .drop("_pr_open_at")
    )

    return (
        pr_keys.select("pull_request_id")
        .join(candidates, on="pull_request_id", how="left")
        .select(
            "pull_request_id",
            "first_touch_at",
            "first_touch_actor",
            "first_touch_event_type",
            "first_touch_seconds_from_open",
        )
    )


def inline_comment_stats(
    df_inline: pl.DataFrame,
    df_prs: pl.DataFrame,
    *,
    bot_actors: Iterable[str] = DEFAULT_BOT_ACTORS,
    pr_id_col: str = "id",
    pr_author_col: str = "author_login",
) -> pl.DataFrame:
    """Per-PR aggregates over inline review comments.

    ``syncer_prreviewinlinecomment.parquet`` is one row per inline
    review comment (loaded as ``data["inline_comments"]`` when present).
    Comment **bodies are not exported** — only metadata + actor +
    timestamps. This helper rolls those rows up to per-PR depth signals
    suitable for cuts by ``lines_bucket`` / ``pr_type`` and correlation
    with end-to-end TTM or ping-pong cycle counts.

    ``df_prs`` is used only to attribute self-comments: the per-PR
    author login is joined in, lowercased, and excluded from the
    "by others" counts. Like :func:`first_review_touch`, the author
    login is not on ``syncer_pullrequest`` natively — join
    ``core_user.github_login`` onto ``prs.author_id`` upstream first.

    Returned columns (one row per PR with at least one inline comment;
    PRs with none are absent — left-join + ``fill_null(0)`` to attach):

    - ``pull_request_id``
    - ``n_inline_comments`` — raw count of comments on the PR.
    - ``n_inline_comments_by_others`` — comments excluding the PR
      author and known bots. The headline "review depth" signal.
    - ``n_inline_threads`` — distinct ``thread_root_node_id`` values
      (one per conversation; replies don't add a new thread).
    - ``n_inline_thread_replies`` — comments whose ``reply_to_node_id``
      is non-null (back-and-forth volume within threads).
    - ``n_inline_authors`` — distinct non-author non-bot logins.
    - ``n_inline_files`` — distinct ``path`` values touched by inline
      comments.
    - ``first_inline_at`` / ``last_inline_at`` — earliest / latest
      ``gh_created_at`` over the PR's inline comments.
    """
    bots_lc = [b.lower() for b in bot_actors]

    pr_keys = df_prs.select(
        pl.col(pr_id_col).alias("pull_request_id"),
        pl.col(pr_author_col).str.to_lowercase().alias("_author_lc"),
    )

    enriched = (
        df_inline.drop_nulls(["pull_request_id"])
        .with_columns(pl.col("author_login").str.to_lowercase().alias("_actor_lc"))
        .join(pr_keys, on="pull_request_id", how="left")
        .with_columns(
            (
                pl.col("_actor_lc").is_not_null()
                & ~pl.col("_actor_lc").is_in(bots_lc)
                & (
                    pl.col("_author_lc").is_null()
                    | (pl.col("_actor_lc") != pl.col("_author_lc"))
                )
            ).alias("_is_other"),
        )
    )

    return (
        enriched.group_by("pull_request_id")
        .agg(
            pl.len().cast(pl.Int64).alias("n_inline_comments"),
            pl.col("_is_other")
            .sum()
            .cast(pl.Int64)
            .alias("n_inline_comments_by_others"),
            pl.col("thread_root_node_id")
            .n_unique()
            .cast(pl.Int64)
            .alias("n_inline_threads"),
            pl.col("reply_to_node_id")
            .is_not_null()
            .sum()
            .cast(pl.Int64)
            .alias("n_inline_thread_replies"),
            pl.col("author_login")
            .filter(pl.col("_is_other"))
            .n_unique()
            .cast(pl.Int64)
            .alias("n_inline_authors"),
            pl.col("path").n_unique().cast(pl.Int64).alias("n_inline_files"),
            pl.col("gh_created_at").min().alias("first_inline_at"),
            pl.col("gh_created_at").max().alias("last_inline_at"),
        )
        .sort("pull_request_id")
    )


def pipeline_stages(
    df_prs: pl.DataFrame,
    df_events: pl.DataFrame,
    *,
    asof: datetime | None = None,
    touch_event_types: Iterable[str] = DEFAULT_TOUCH_EVENT_TYPES,
    bot_actors: Iterable[str] = DEFAULT_BOT_ACTORS,
    pr_id_col: str = "id",
    pr_open_col: str = "gh_created_at",
    pr_author_col: str = "author_login",
    pr_merged_col: str | None = "merged_at",
    maintainer_merge_label: str = MAINTAINER_MERGE_LABEL,
    ready_to_merge_label: str = READY_TO_MERGE_LABEL,
) -> pl.DataFrame:
    """Per-PR five-stage milestone frame: open → first touch → MM → RTM → merged.

    Combines :func:`first_review_touch` and :func:`stage_timestamps` into a
    single wide per-PR frame with the four sequential stage deltas the
    "anatomy of a merge" story needs:

    - **stage 1** open → first non-author, non-bot touch
    - **stage 2** first touch → first ``maintainer-merge`` label
    - **stage 3** first ``maintainer-merge`` → first ``ready-to-merge`` label
    - **stage 4** first ``ready-to-merge`` → merge

    A stage delta is null when either endpoint is null or the endpoints are
    in non-monotonic order (e.g. RTM applied before MM — happens rarely
    when ``bors r+`` is run without a prior ``maintainer merge`` comment).
    The total ``seconds_open_to_merged`` follows the same convention so
    log-scale histograms can drop nulls cleanly.

    Parameters
    ----------
    df_prs:
        One row per candidate PR. Must carry ``pr_id_col`` (PR id),
        ``pr_open_col`` (PR creation time), and ``pr_author_col`` (GitHub
        login; join from ``core_user`` upstream — see
        :func:`first_review_touch` for the canonical pattern).
        ``pr_merged_col`` is optional; pass ``None`` (or a column of
        nulls) for an "all candidates" frame that includes
        closed-unmerged PRs. Mathlib callers typically pass the bors-aware
        effective merge timestamp built with
        :func:`qb_notebook.filters.expr_merged_to_master` +
        :func:`qb_notebook.filters.expr_merged_at_effective` before
        calling.
    df_events:
        Timeline events frame; must include LABELED events for the
        ``maintainer-merge`` and ``ready-to-merge`` labels plus the
        touch event types.

    Returns one row per PR in ``df_prs`` with columns:

    - ``pull_request_id``
    - ``opened_at``, ``first_touch_at``, ``first_maintainer_merge_at``,
      ``first_ready_to_merge_at``, ``merged_at_effective``
    - ``seconds_open_to_first_touch``,
      ``seconds_first_touch_to_maintainer_merge``,
      ``seconds_maintainer_merge_to_ready_to_merge``,
      ``seconds_ready_to_merge_to_merged``
    - ``seconds_open_to_merged`` — total TTM when both endpoints are
      present and ``merged_at_effective >= opened_at``.

    The ``asof`` parameter is accepted for API symmetry with the other
    review-state helpers but is not currently used: stage timestamps come
    from observed events, not from clamping open intervals.
    """
    _ = asof  # reserved for future use; included for API symmetry

    touch = first_review_touch(
        df_prs,
        df_events,
        event_types=touch_event_types,
        bot_actors=bot_actors,
        pr_id_col=pr_id_col,
        pr_open_col=pr_open_col,
        pr_author_col=pr_author_col,
    )

    label_firsts = stage_timestamps(
        df_events,
        label_order=(maintainer_merge_label, ready_to_merge_label),
    )
    mm_col = f"first_{maintainer_merge_label.replace('-', '_')}"
    rtm_col = f"first_{ready_to_merge_label.replace('-', '_')}"

    merged_expr: pl.Expr
    if pr_merged_col is None or pr_merged_col not in df_prs.columns:
        merged_expr = pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(
            "merged_at_effective"
        )
    else:
        merged_expr = pl.col(pr_merged_col).alias("merged_at_effective")

    base = df_prs.select(
        pl.col(pr_id_col).alias("pull_request_id"),
        pl.col(pr_open_col).alias("opened_at"),
        merged_expr,
    )

    wide = base.join(
        touch.select("pull_request_id", "first_touch_at"),
        on="pull_request_id",
        how="left",
    ).join(
        label_firsts.select(
            "pull_request_id",
            pl.col(mm_col).alias("first_maintainer_merge_at"),
            pl.col(rtm_col).alias("first_ready_to_merge_at"),
        ),
        on="pull_request_id",
        how="left",
    )

    def _delta(start: str, end: str, out: str) -> pl.Expr:
        diff = (pl.col(end) - pl.col(start)).dt.total_seconds().cast(pl.Float64)
        return pl.when(diff >= 0).then(diff).otherwise(None).alias(out)

    return wide.with_columns(
        _delta("opened_at", "first_touch_at", "seconds_open_to_first_touch"),
        _delta(
            "first_touch_at",
            "first_maintainer_merge_at",
            "seconds_first_touch_to_maintainer_merge",
        ),
        _delta(
            "first_maintainer_merge_at",
            "first_ready_to_merge_at",
            "seconds_maintainer_merge_to_ready_to_merge",
        ),
        _delta(
            "first_ready_to_merge_at",
            "merged_at_effective",
            "seconds_ready_to_merge_to_merged",
        ),
        _delta("opened_at", "merged_at_effective", "seconds_open_to_merged"),
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
