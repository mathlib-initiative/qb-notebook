"""Declarative PR-cohort specs + the cross-cohort aggregates built on them.

`anatomy_of_a_merge.py` studies **one** cohort at a time: a date range plus
topic / PR-type filters, funnelled through :func:`review_states.pipeline_stages`.
This module generalizes that to *N* cohorts so they can be compared side by
side (see `marimo/cohort_comparison.py`).

The unit is :class:`CohortSpec` — a declarative description of "which PRs",
with an empty tuple meaning "no filter on this dimension". :func:`filter_cohort`
applies one to a per-PR frame, and the aggregate helpers
(:func:`milestone_summary`, :func:`stage_quantiles`) turn a
``{name: frame}`` mapping into one tidy row per cohort (or per cohort × stage)
that a table or grouped-bar chart can consume directly.

Deliberately *not* here: the pipeline computation itself. Stage timestamps are
per-PR and cohort-invariant, so callers should run
:func:`review_states.pipeline_stages` **once** over the whole PR corpus and
then slice it with :func:`filter_cohort` — recomputing it per cohort is both
slower and, since ``pipeline_stages`` is a pure per-PR transform, identical in
result.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Mapping, Sequence

import polars as pl

# Pseudo-bucket in the topic filter for PRs that never carried any `t-*`
# label. Selecting it alongside real labels keeps unlabeled PRs in the
# cohort; selecting only real labels drops them. Same convention (and same
# literal) as the topic filter in `anatomy_of_a_merge.py`.
TOPIC_NONE: str = "(none)"

# Author-cohort filter values. "First-time" is relative to the frame the
# `pr_shape.author_cohort` decorator was run over — for the mathlib
# notebooks that's every human PR to master, so it means "author's first
# such PR", not "first PR in this cohort's window".
AUTHOR_ANY: str = "any"
AUTHOR_FIRST_TIME: str = "first_time"
AUTHOR_RETURNING: str = "returning"
AUTHOR_COHORT_VALUES: tuple[str, ...] = (
    AUTHOR_ANY,
    AUTHOR_FIRST_TIME,
    AUTHOR_RETURNING,
)

# Floor (seconds) that separates the *iterated* review regime from the
# *maintainer-first* one on stage 2. When the very first non-author,
# non-bot event is itself the sign-off, the `github-actions` bot applies
# `maintainer-merge` within ~10-20 s and the stage collapses to the bot
# latency floor. See `anatomy_of_a_merge.py` §2 for the bimodality.
MAINTAINER_FIRST_SECONDS: float = 60.0

_SECONDS_PER_DAY: float = 86400.0


@dataclass(frozen=True)
class Stage:
    """One pipeline stage: which duration column, how to label it, and the
    minimum duration (seconds) to keep.

    ``min_seconds`` is a *strict* lower bound. It defaults to 0, which still
    drops exact-zero durations — every consumer here plots on log-spaced bins
    or quantiles them alongside such plots, and 0 has no place on a log axis.
    """

    column: str
    label: str
    min_seconds: float = 0.0


STAGE_OPEN_TO_FIRST_TOUCH = Stage("seconds_open_to_first_touch", "open → first review")
STAGE_FIRST_TOUCH_TO_MM = Stage(
    "seconds_first_touch_to_maintainer_merge", "first review → maintainer-merge"
)
STAGE_MM_TO_RTM = Stage(
    "seconds_maintainer_merge_to_ready_to_merge", "maintainer-merge → ready-to-merge"
)
STAGE_RTM_TO_MERGED = Stage(
    "seconds_ready_to_merge_to_merged", "ready-to-merge → merged"
)
STAGE_OPEN_TO_MERGED = Stage("seconds_open_to_merged", "open → merged (total)")

#: The four sequential stages plus the end-to-end total, in funnel order.
DEFAULT_STAGES: tuple[Stage, ...] = (
    STAGE_OPEN_TO_FIRST_TOUCH,
    STAGE_FIRST_TOUCH_TO_MM,
    STAGE_MM_TO_RTM,
    STAGE_RTM_TO_MERGED,
    STAGE_OPEN_TO_MERGED,
)

#: Milestone columns counted by :func:`milestone_summary`, as
#: ``(summary key, pipeline column)`` pairs in funnel order.
MILESTONE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("first_touch", "first_touch_at"),
    ("maintainer_merge", "first_maintainer_merge_at"),
    ("ready_to_merge", "first_ready_to_merge_at"),
)


@dataclass(frozen=True)
class CohortSpec:
    """A declarative PR-cohort definition.

    Every filter dimension is optional and an **empty tuple means "no filter"**
    — a spec with no fields set selects the whole frame. Multiple values within
    one dimension are OR-ed; the dimensions themselves are AND-ed.

    Attributes
    ----------
    name:
        Display label. Uniquified across a set of specs by
        :func:`with_unique_names` (the aggregate helpers key their output on
        it, so duplicates would collide).
    start / end:
        **Inclusive** PR-open days. ``None`` means "open-ended on this side".
    topics:
        `t-*` label names, plus optionally :data:`TOPIC_NONE` for PRs that
        never carried one. A PR passes if it *ever* carried a selected label
        (label intervals, not merge-time attribution).
    pr_types:
        Parsed conventional-commit prefixes (`qb_notebook.pr_shape.pr_type`).
    lines_buckets:
        Size-bucket labels (`qb_notebook.pr_shape.size_buckets`).
    author_cohort:
        One of :data:`AUTHOR_COHORT_VALUES`. PRs with a null `is_first_pr`
        (no author on record) are excluded by both non-``any`` settings.
    color:
        Series color for plots. Carried on the spec so a cohort keeps its
        color when other cohorts are added or removed.
    """

    name: str
    start: date | None = None
    end: date | None = None
    topics: tuple[str, ...] = ()
    pr_types: tuple[str, ...] = ()
    lines_buckets: tuple[str, ...] = ()
    author_cohort: str = AUTHOR_ANY
    color: str = "#2a78d6"

    def describe(self) -> str:
        """One-line human summary of the filters, for captions."""
        lo = self.start.isoformat() if self.start else "…"
        hi = self.end.isoformat() if self.end else "…"
        parts = [f"opened {lo} → {hi}"]
        if self.topics:
            parts.append(f"topics: {', '.join(self.topics)}")
        if self.pr_types:
            parts.append(f"types: {', '.join(self.pr_types)}")
        if self.lines_buckets:
            parts.append(f"size: {', '.join(self.lines_buckets)}")
        if self.author_cohort != AUTHOR_ANY:
            parts.append(f"authors: {self.author_cohort.replace('_', '-')}")
        return "; ".join(parts)


def window_bounds(spec: CohortSpec) -> tuple[datetime | None, datetime | None]:
    """Half-open UTC ``[lo, hi)`` timestamps for a spec's inclusive day bounds.

    The upper bound becomes ``end + 1 day`` so a same-day ``start == end``
    range selects that whole UTC day. Either side is ``None`` when open.
    """
    lo = (
        None
        if spec.start is None
        else datetime(
            spec.start.year, spec.start.month, spec.start.day, tzinfo=timezone.utc
        )
    )
    hi = (
        None
        if spec.end is None
        else datetime(spec.end.year, spec.end.month, spec.end.day, tzinfo=timezone.utc)
        + timedelta(days=1)
    )
    return lo, hi


def filter_cohort(
    df: pl.DataFrame,
    spec: CohortSpec,
    *,
    t_intervals: pl.DataFrame | None = None,
    id_col: str = "pull_request_id",
    created_col: str = "opened_at",
    pr_type_col: str = "pr_type",
    lines_bucket_col: str = "lines_bucket",
    first_pr_col: str = "is_first_pr",
    topic_id_col: str = "pull_request_id",
    topic_label_col: str = "label_name",
) -> pl.DataFrame:
    """Apply one :class:`CohortSpec` to a per-PR frame.

    ``df`` is expected to be a wide per-PR frame — typically the output of
    :func:`review_states.pipeline_stages` joined to the `pr_shape` decorator
    columns — carrying ``id_col``, ``created_col``, and whichever attribute
    columns the spec filters on. Columns for dimensions the spec leaves empty
    are never touched, so a minimal frame works for a minimal spec.

    ``t_intervals`` is the label-interval frame from
    :func:`review_states.label_intervals` restricted to `t-*` labels; it is
    required only when ``spec.topics`` is non-empty.
    """
    out = df

    lo, hi = window_bounds(spec)
    if lo is not None:
        out = out.filter(pl.col(created_col) >= lo)
    if hi is not None:
        out = out.filter(pl.col(created_col) < hi)

    if spec.topics:
        if t_intervals is None:
            raise ValueError(
                "filter_cohort: spec.topics is set but t_intervals was not supplied"
            )
        selected = [t for t in spec.topics if t != TOPIC_NONE]
        # `.implode()` per the `filters.pr_ids_with_any_labels` idiom: a bare
        # Series on the right of `is_in` is deprecated-ambiguous in polars 1.x.
        keep = pl.col(id_col).is_in(
            t_intervals.filter(pl.col(topic_label_col).is_in(selected))
            .get_column(topic_id_col)
            .unique()
            .implode()
        )
        if TOPIC_NONE in spec.topics:
            keep = keep | ~pl.col(id_col).is_in(
                t_intervals.get_column(topic_id_col).unique().implode()
            )
        out = out.filter(keep)

    if spec.pr_types:
        out = out.filter(pl.col(pr_type_col).is_in(list(spec.pr_types)))
    if spec.lines_buckets:
        out = out.filter(pl.col(lines_bucket_col).is_in(list(spec.lines_buckets)))

    if spec.author_cohort == AUTHOR_FIRST_TIME:
        out = out.filter(pl.col(first_pr_col))
    elif spec.author_cohort == AUTHOR_RETURNING:
        out = out.filter(~pl.col(first_pr_col))
    elif spec.author_cohort != AUTHOR_ANY:
        raise ValueError(
            f"filter_cohort: unknown author_cohort {spec.author_cohort!r} "
            f"(expected one of {AUTHOR_COHORT_VALUES})"
        )

    return out


def with_unique_names(specs: Sequence[CohortSpec]) -> list[CohortSpec]:
    """Return the specs with blank / duplicate names resolved.

    Blank names become ``cohort N`` (1-indexed); a repeated name gets a
    ``" (2)"``, ``" (3)"``, … suffix. Idempotent, so it is safe to call on
    an already-uniquified list.
    """
    used: set[str] = set()
    out: list[CohortSpec] = []
    for i, spec in enumerate(specs):
        base = (spec.name or "").strip() or f"cohort {i + 1}"
        name = base
        n = 1
        while name in used:
            n += 1
            name = f"{base} ({n})"
        used.add(name)
        out.append(spec if name == spec.name else replace(spec, name=name))
    return out


def cohort_frames(
    df: pl.DataFrame,
    specs: Sequence[CohortSpec],
    **filter_kwargs: object,
) -> dict[str, pl.DataFrame]:
    """Slice ``df`` once per spec, keyed by (uniquified) cohort name.

    Keyword arguments are forwarded to :func:`filter_cohort`. Pass the specs
    through :func:`with_unique_names` first if you also need to zip colors or
    captions against the returned keys.
    """
    return {
        spec.name: filter_cohort(df, spec, **filter_kwargs)  # type: ignore[arg-type]
        for spec in with_unique_names(specs)
    }


_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "cohort": pl.String,
    "n_prs": pl.Int64,
    **{
        col: dtype
        for key, _ in MILESTONE_COLUMNS
        for col, dtype in ((f"n_{key}", pl.Int64), (f"pct_{key}", pl.Float64))
    },
    "n_merged": pl.Int64,
    "pct_merged": pl.Float64,
    "n_closed_unmerged": pl.Int64,
    "pct_closed_unmerged": pl.Float64,
    "n_still_open": pl.Int64,
    "pct_still_open": pl.Float64,
    "median_ttm_days": pl.Float64,
    "p90_ttm_days": pl.Float64,
}


def milestone_summary(
    frames: Mapping[str, pl.DataFrame],
    *,
    merged_col: str = "is_merged",
    closed_col: str = "is_closed",
    total_seconds_col: str = "seconds_open_to_merged",
) -> pl.DataFrame:
    """One row per cohort: milestone counts, shares, and headline TTM.

    Shares are fractions in ``[0, 1]`` (null for an empty cohort), not
    preformatted strings, so the same frame drives both the table and the
    funnel chart. ``median_ttm_days`` / ``p90_ttm_days`` are over merged PRs
    only — ``total_seconds_col`` is null unless the PR merged.
    """
    rows: list[dict[str, object]] = []
    for name, frame in frames.items():
        n = frame.height
        row: dict[str, object] = {"cohort": name, "n_prs": n}
        for key, col in MILESTONE_COLUMNS:
            reached = int(frame.get_column(col).is_not_null().sum())
            row[f"n_{key}"] = reached
            row[f"pct_{key}"] = reached / n if n else None

        merged = frame.get_column(merged_col).fill_null(False)
        closed = frame.get_column(closed_col).fill_null(False)
        n_merged = int(merged.sum())
        n_closed_unmerged = int((closed & ~merged).sum())
        n_still_open = n - n_merged - n_closed_unmerged
        for key, count in (
            ("merged", n_merged),
            ("closed_unmerged", n_closed_unmerged),
            ("still_open", n_still_open),
        ):
            row[f"n_{key}"] = count
            row[f"pct_{key}"] = count / n if n else None

        ttm = frame.get_column(total_seconds_col).drop_nulls()
        row["median_ttm_days"] = (
            float(ttm.quantile(0.5, interpolation="linear")) / _SECONDS_PER_DAY
            if ttm.len()
            else None
        )
        row["p90_ttm_days"] = (
            float(ttm.quantile(0.9, interpolation="linear")) / _SECONDS_PER_DAY
            if ttm.len()
            else None
        )
        rows.append(row)

    return pl.DataFrame(rows, schema=_SUMMARY_SCHEMA)


def stage_series(frame: pl.DataFrame, stage: Stage) -> pl.Series:
    """Stage durations in **days**, dropping nulls and sub-floor values.

    Null deltas mean "this PR never traversed the stage" (a missing endpoint,
    or endpoints in non-monotonic order — see
    :func:`review_states.pipeline_stages`), so every consumer works off the
    subset that actually did.
    """
    values = frame.get_column(stage.column).drop_nulls()
    floor = max(stage.min_seconds, 0.0)
    return (values.filter(values > floor) / _SECONDS_PER_DAY).rename("days")


def stage_quantiles(
    frames: Mapping[str, pl.DataFrame],
    *,
    stages: Sequence[Stage] = DEFAULT_STAGES,
    quantiles: Sequence[float] = (0.5, 0.9),
) -> pl.DataFrame:
    """Long-form per cohort × stage quantile table (durations in days).

    Columns: ``cohort``, ``stage``, ``n``, then one ``pNN_days`` per requested
    quantile. ``n`` is the number of PRs that traversed the stage *after* the
    stage's ``min_seconds`` floor, so it can be well below the cohort size.
    """
    quantile_cols = {f"p{round(q * 100)}_days": pl.Float64 for q in quantiles}
    schema: dict[str, pl.DataType] = {
        "cohort": pl.String,
        "stage": pl.String,
        "n": pl.Int64,
        **quantile_cols,
    }

    rows: list[dict[str, object]] = []
    for name, frame in frames.items():
        for stage in stages:
            days = stage_series(frame, stage)
            row: dict[str, object] = {
                "cohort": name,
                "stage": stage.label,
                "n": days.len(),
            }
            for q, col in zip(quantiles, quantile_cols):
                row[col] = (
                    float(days.quantile(q, interpolation="linear"))
                    if days.len()
                    else None
                )
            rows.append(row)

    return pl.DataFrame(rows, schema=schema)
