"""Helpers for analyzing assignment and review-request events.

Mathlib's ``ASSIGNED`` / ``UNASSIGNED`` events come from two sources:

- Automation: ``leanprover-community-bot-assistant`` rotates assignment
  to keep the reviewer queue moving; ``mathlib-triage`` performs the
  recent inactivity-driven *unassignment* sweep.
- Humans: maintainers, reviewers, or the PR author (self-assign /
  self-unassign).

This module exposes three helpers built on that distinction plus the
review-request stream:

- :func:`classify_assignment_events` / :func:`classify_unassign_events`
  attach a ``kind`` column (``bot`` / ``self`` / ``other_human``) to
  ``ASSIGNED`` / ``UNASSIGNED`` events so downstream cuts don't have to
  re-derive the actor membership inline.
- :func:`review_request_responses` matches each ``REVIEW_REQUESTED``
  event to the earliest subsequent ``REVIEW_*`` event by the requested
  reviewer on the same PR, giving a per-request response-latency frame
  (or null when no response landed).
- :func:`assignment_policy_outcome` joins per-PR assignee sets against
  the inferred ``maintainer-merge`` trigger (from
  :func:`qb_notebook.review_states.attribute_label_events`) to measure
  conformance to the "assignee sees the PR through to maintainer-merge"
  policy. Resolution is *any assignee ever* — see the
  ``request_effectiveness`` notebook (§3) for the rationale.
"""

from __future__ import annotations

from typing import Iterable

import polars as pl

# Bot actors observed to drive automatic assignment / unassignment on
# mathlib4. Subset of ``review_states.DEFAULT_BOT_ACTORS`` — we keep a
# narrow alias here because the assignment automation set is a strict
# subset of the broader bot list (``mathlib-bors`` / ``bors`` / etc. do
# *not* assign reviewers, so it would be misleading to count them as
# "bot assigners").
ASSIGNMENT_BOT_ACTORS: frozenset[str] = frozenset(
    {
        "leanprover-community-bot-assistant",
        "mathlib-triage",
    }
)

# Event types that count as a reviewer "responding" to a review request.
# ``REVIEW_DISMISSED`` is intentionally excluded — dismissing a prior
# review is a meta-action, not a substantive reply to a fresh request.
REVIEW_RESPONSE_EVENT_TYPES: tuple[str, ...] = (
    "REVIEW_APPROVED",
    "REVIEW_COMMENTED",
    "REVIEW_CHANGES_REQUESTED",
)

# Event types that count as an assignee "responding" to being assigned.
# Broader than :data:`REVIEW_RESPONSE_EVENT_TYPES` because the assignment
# policy doesn't require a formal review — a comment or a self-unassign
# (decline) is also a meaningful first action.
ASSIGNMENT_RESPONSE_EVENT_TYPES: tuple[str, ...] = (
    "REVIEW_APPROVED",
    "REVIEW_COMMENTED",
    "REVIEW_CHANGES_REQUESTED",
    "ISSUE_COMMENTED",
    "UNASSIGNED",
)


def _classify_kind(actor: pl.Expr, counterpart: pl.Expr) -> pl.Expr:
    """Shared kind classifier for ASSIGNED / UNASSIGNED events.

    ``bot`` if the actor is in :data:`ASSIGNMENT_BOT_ACTORS`, ``self``
    if actor == counterpart (assignee_login for both event types),
    otherwise ``other_human``.
    """
    return (
        pl.when(actor.is_in(ASSIGNMENT_BOT_ACTORS))
        .then(pl.lit("bot"))
        .when(actor == counterpart)
        .then(pl.lit("self"))
        .otherwise(pl.lit("other_human"))
    )


def classify_assignment_events(
    df_events: pl.DataFrame,
    *,
    bot_actors: Iterable[str] = ASSIGNMENT_BOT_ACTORS,
) -> pl.DataFrame:
    """Return ``ASSIGNED`` events with a ``kind`` column attached.

    ``kind`` is one of ``"bot"``, ``"self"``, ``"other_human"``.
    Pass a custom ``bot_actors`` set when the automation roster
    diverges from :data:`ASSIGNMENT_BOT_ACTORS`.
    """
    bots = frozenset(bot_actors)
    asg = df_events.filter(pl.col("type") == "ASSIGNED")
    return asg.with_columns(
        pl.when(pl.col("actor_login").is_in(bots))
        .then(pl.lit("bot"))
        .when(pl.col("actor_login") == pl.col("assignee_login"))
        .then(pl.lit("self"))
        .otherwise(pl.lit("other_human"))
        .alias("kind"),
    )


def classify_unassign_events(
    df_events: pl.DataFrame,
    *,
    bot_actors: Iterable[str] = ASSIGNMENT_BOT_ACTORS,
) -> pl.DataFrame:
    """Return ``UNASSIGNED`` events with a ``kind`` column attached.

    ``kind`` is one of ``"bot"``, ``"self"``, ``"other_human"``.
    The bot bucket isolates the ``mathlib-triage`` inactivity-sweep
    automation; ``self`` is the assignee opting out; ``other_human``
    is a maintainer or third party removing the assignment.
    """
    bots = frozenset(bot_actors)
    una = df_events.filter(pl.col("type") == "UNASSIGNED")
    return una.with_columns(
        pl.when(pl.col("actor_login").is_in(bots))
        .then(pl.lit("bot"))
        .when(pl.col("actor_login") == pl.col("assignee_login"))
        .then(pl.lit("self"))
        .otherwise(pl.lit("other_human"))
        .alias("kind"),
    )


def review_request_responses(
    df_events: pl.DataFrame,
    *,
    response_event_types: Iterable[str] = REVIEW_RESPONSE_EVENT_TYPES,
) -> pl.DataFrame:
    """One row per ``REVIEW_REQUESTED`` event with response latency attached.

    For each ``REVIEW_REQUESTED`` event we look for the earliest event
    on the same PR whose ``type`` is in ``response_event_types`` and
    whose ``actor_login`` equals the request's
    ``requested_reviewer_login``. The match must occur at or after the
    request timestamp.

    Team-targeted requests (``requested_team_slug`` set,
    ``requested_reviewer_login`` null) yield null response rows — they
    can't match an individual responder. Callers should filter on
    ``requested_reviewer_login`` if they want to exclude team requests.

    Returns one row per ``REVIEW_REQUESTED`` event with:

    - ``request_event_id`` — the ``id`` column from the event row,
    - ``pull_request_id``, ``requested_at``,
    - ``requested_by`` (the actor who made the request),
    - ``requested_reviewer_login`` (target reviewer, nullable),
    - ``responded_at`` (nullable),
    - ``response_event_type`` (nullable),
    - ``response_gap_seconds`` — float seconds from request to first
      response, null if no response found,
    - ``responded`` (bool) — convenience flag.
    """
    response_types = list(response_event_types)

    requests = df_events.filter(pl.col("type") == "REVIEW_REQUESTED").select(
        pl.col("id").alias("request_event_id"),
        pl.col("pull_request_id"),
        pl.col("occurred_at").alias("requested_at"),
        pl.col("actor_login").alias("requested_by"),
        pl.col("requested_reviewer_login"),
    )

    if requests.is_empty():
        return requests.with_columns(
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("responded_at"),
            pl.lit(None, dtype=pl.Utf8).alias("response_event_type"),
            pl.lit(None, dtype=pl.Float64).alias("response_gap_seconds"),
            pl.lit(False, dtype=pl.Boolean).alias("responded"),
        )

    responses = df_events.filter(pl.col("type").is_in(response_types)).select(
        pl.col("pull_request_id"),
        pl.col("occurred_at").alias("responded_at"),
        pl.col("actor_login").alias("login"),
        pl.col("type").alias("response_event_type"),
    )

    # Match on (pull_request_id, reviewer login) via join_asof with
    # strategy="forward" — for each request, the nearest response at-or-
    # after the request time. Team requests (null reviewer login) join
    # against null and never match, which is the desired behaviour.
    requests_keyed = requests.rename({"requested_reviewer_login": "login"}).sort(
        "requested_at"
    )
    responses_sorted = responses.sort("responded_at")

    matched = requests_keyed.join_asof(
        responses_sorted,
        left_on="requested_at",
        right_on="responded_at",
        by=["pull_request_id", "login"],
        strategy="forward",
    )

    return (
        matched.rename({"login": "requested_reviewer_login"})
        .with_columns(
            (pl.col("responded_at") - pl.col("requested_at"))
            .dt.total_seconds()
            .cast(pl.Float64)
            .alias("response_gap_seconds"),
            pl.col("responded_at").is_not_null().alias("responded"),
        )
        .select(
            "request_event_id",
            "pull_request_id",
            "requested_at",
            "requested_by",
            "requested_reviewer_login",
            "responded_at",
            "response_event_type",
            "response_gap_seconds",
            "responded",
        )
    )


def assignment_responses(
    df_events: pl.DataFrame,
    *,
    response_event_types: Iterable[str] = ASSIGNMENT_RESPONSE_EVENT_TYPES,
    bot_actors: Iterable[str] = ASSIGNMENT_BOT_ACTORS,
) -> pl.DataFrame:
    """One row per ``ASSIGNED`` event with first action by the assignee.

    Analog of :func:`review_request_responses`: for each ``ASSIGNED``
    event we look for the earliest event on the same PR whose ``type``
    is in ``response_event_types`` and whose ``actor_login`` equals the
    ``ASSIGNED``'s ``assignee_login``. The match must occur at or after
    the assignment timestamp.

    Default ``response_event_types`` includes ``UNASSIGNED`` because a
    self-unassign ("decline") is a meaningful response to being
    assigned; downstream cuts can filter on ``response_event_type`` to
    distinguish review / comment / decline.

    ``kind`` mirrors :func:`classify_assignment_events`
    (``"bot"`` / ``"self"`` / ``"other_human"``) so callers can split
    the latency distribution by how the assignment was made without
    rejoining the source frame.

    Returns one row per ``ASSIGNED`` event with:

    - ``assignment_event_id`` — the ``id`` column from the event row,
    - ``pull_request_id``, ``assigned_at``,
    - ``assigned_by`` (the actor who made the assignment),
    - ``assignee_login``,
    - ``kind`` (``bot`` / ``self`` / ``other_human``),
    - ``responded_at`` (nullable),
    - ``response_event_type`` (nullable),
    - ``response_gap_seconds`` — float seconds from assignment to first
      action, null if no match found,
    - ``responded`` (bool) — convenience flag.
    """
    response_types = list(response_event_types)
    bots = frozenset(bot_actors)

    assignments = (
        df_events.filter(pl.col("type") == "ASSIGNED")
        .select(
            pl.col("id").alias("assignment_event_id"),
            pl.col("pull_request_id"),
            pl.col("occurred_at").alias("assigned_at"),
            pl.col("actor_login").alias("assigned_by"),
            pl.col("assignee_login"),
        )
        .with_columns(
            pl.when(pl.col("assigned_by").is_in(bots))
            .then(pl.lit("bot"))
            .when(pl.col("assigned_by") == pl.col("assignee_login"))
            .then(pl.lit("self"))
            .otherwise(pl.lit("other_human"))
            .alias("kind"),
        )
    )

    null_response_cols = [
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("responded_at"),
        pl.lit(None, dtype=pl.Utf8).alias("response_event_type"),
        pl.lit(None, dtype=pl.Float64).alias("response_gap_seconds"),
        pl.lit(False, dtype=pl.Boolean).alias("responded"),
    ]

    if assignments.is_empty():
        return assignments.with_columns(null_response_cols)

    # Match each ASSIGNED to the earliest event by the same assignee on
    # the same PR at or after the assignment timestamp. UNASSIGNED is in
    # the response set, so a self-unassign that follows the assignment
    # is captured as the "response".
    responses = df_events.filter(pl.col("type").is_in(response_types)).select(
        pl.col("pull_request_id"),
        pl.col("occurred_at").alias("responded_at"),
        pl.col("actor_login").alias("assignee_login"),
        pl.col("type").alias("response_event_type"),
    )

    assignments_sorted = assignments.sort("assigned_at")
    responses_sorted = responses.sort("responded_at")

    matched = assignments_sorted.join_asof(
        responses_sorted,
        left_on="assigned_at",
        right_on="responded_at",
        by=["pull_request_id", "assignee_login"],
        strategy="forward",
    )

    return matched.with_columns(
        (pl.col("responded_at") - pl.col("assigned_at"))
        .dt.total_seconds()
        .cast(pl.Float64)
        .alias("response_gap_seconds"),
        pl.col("responded_at").is_not_null().alias("responded"),
    ).select(
        "assignment_event_id",
        "pull_request_id",
        "assigned_at",
        "assigned_by",
        "assignee_login",
        "kind",
        "responded_at",
        "response_event_type",
        "response_gap_seconds",
        "responded",
    )


def pr_assignees_ever(
    df_events: pl.DataFrame,
    *,
    pr_id_col: str = "pull_request_id",
) -> pl.DataFrame:
    """Distinct ``(pull_request_id, assignee_login)`` from all ASSIGNED events.

    A PR with three rotating assignees yields three rows. Callers
    typically aggregate this onward (``group_by`` PR to get a list of
    assignees, or join against a per-PR fact like an inferred
    maintainer-merge trigger).
    """
    return (
        df_events.filter(
            (pl.col("type") == "ASSIGNED") & pl.col("assignee_login").is_not_null()
        )
        .select(pr_id_col, "assignee_login")
        .unique()
    )


def assignment_policy_outcome(
    df_events: pl.DataFrame,
    mm_attribution: pl.DataFrame,
    *,
    pr_id_col: str = "pull_request_id",
    inferred_actor_col: str = "inferred_actor",
    label_at_col: str = "label_at",
) -> pl.DataFrame:
    """Per-PR check: did any ever-assignee trigger the first MM label?

    ``mm_attribution`` is the output of
    :func:`qb_notebook.review_states.attribute_label_events` for the
    ``maintainer-merge`` label (one row per ``LABELED`` event, with
    ``inferred_actor`` set to the attributed human).

    Resolution: *any assignee ever*. A PR rotates through assignees;
    the policy is satisfied if any of them ends up triggering the
    first ``maintainer-merge`` label.

    Returns one row per PR that has **both** at least one ``ASSIGNED``
    event and a maintainer-merge attribution. Columns:

    - ``pull_request_id``
    - ``mm_trigger`` — attributed human for the first MM label,
    - ``mm_at`` — timestamp of the first MM label,
    - ``n_assignees`` — distinct assignees seen on the PR,
    - ``assignees`` — list of distinct assignee logins,
    - ``assignee_triggered_mm`` (bool) — policy outcome.
    """
    assignees = pr_assignees_ever(df_events, pr_id_col=pr_id_col)
    per_pr = assignees.group_by(pr_id_col).agg(
        pl.col("assignee_login").alias("assignees"),
        pl.col("assignee_login").n_unique().alias("n_assignees"),
    )

    mm_first = (
        mm_attribution.sort(label_at_col)
        .group_by(pr_id_col, maintain_order=True)
        .agg(
            pl.col(inferred_actor_col).first().alias("mm_trigger"),
            pl.col(label_at_col).first().alias("mm_at"),
        )
    )

    return (
        per_pr.join(mm_first, on=pr_id_col, how="inner")
        .with_columns(
            pl.col("assignees")
            .list.contains(pl.col("mm_trigger"))
            .fill_null(False)
            .alias("assignee_triggered_mm"),
        )
        .select(
            pr_id_col,
            "mm_trigger",
            "mm_at",
            "n_assignees",
            "assignees",
            "assignee_triggered_mm",
        )
    )
