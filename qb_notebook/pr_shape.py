"""PR-shape attributes: size buckets, author cohort, draft history.

These three helpers underpin Theme 5 (`marimo/pr_shape_effects.py`):
do bigger PRs take disproportionately longer, do first-time contributors
wait longer, do PRs that start as drafts behave differently? Each
function adds attribute columns to a copy of the input PR frame; the
notebook joins them onto merge / court-exit metrics computed via the
existing `qb_notebook.review_states` helpers.

The cuts are deliberately keyed off attributes that are stable across
the PR's life (size at merge, author identity, draft state at open) so
the same per-PR row can drive multiple sub-analyses without
recomputation.
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

# Lines-changed bucket breaks chosen to roughly equalize bucket sizes on
# the current mathlib4 distribution while keeping round-number cutoffs.
# Inclusive upper bounds: x in [0, 10] → "0-10", x in [11, 50] → "11-50", ...
DEFAULT_LINES_BREAKS: tuple[int, ...] = (10, 50, 200, 1000)
DEFAULT_FILES_BREAKS: tuple[int, ...] = (1, 3, 10, 30)


def bucket_labels(breaks: Sequence[int]) -> list[str]:
    """Human-readable labels for a sequence of inclusive upper-bound breaks.

    `breaks=(10, 50, 200, 1000)` →
    `['0-10', '11-50', '51-200', '201-1000', '1001+']`.
    The returned list has `len(breaks) + 1` entries; the last is the
    overflow bucket.
    """
    labels: list[str] = []
    prev = -1  # so the first label starts at 0
    for b in breaks:
        lo = prev + 1
        labels.append(f"{lo}-{b}")
        prev = b
    labels.append(f"{prev + 1}+")
    return labels


def _bucket_expr(
    col_expr: pl.Expr, breaks: Sequence[int], labels: Sequence[str]
) -> pl.Expr:
    """when/then chain mapping a numeric expression to a bucket label.

    `breaks=(10, 50)`, `labels=("A", "B", "C")` produces
    `when(x <= 10) -> A, when(x <= 50) -> B, otherwise C`. Nulls in
    `col_expr` produce null labels (explicit `is_null` branch up front).
    """
    if len(labels) != len(breaks) + 1:
        raise ValueError("labels must have len(breaks) + 1 entries")
    expr = pl.when(col_expr.is_null()).then(pl.lit(None, dtype=pl.String))
    for b, lbl in zip(breaks, labels[:-1]):
        expr = expr.when(col_expr <= b).then(pl.lit(lbl))
    return expr.otherwise(pl.lit(labels[-1]))


def size_buckets(
    df_prs: pl.DataFrame,
    *,
    lines_breaks: Sequence[int] = DEFAULT_LINES_BREAKS,
    files_breaks: Sequence[int] = DEFAULT_FILES_BREAKS,
    additions_col: str = "additions",
    deletions_col: str = "deletions",
    files_col: str = "changed_files_count",
) -> pl.DataFrame:
    """Add `lines_changed`, `lines_bucket`, `files_bucket` columns.

    `lines_changed = additions + deletions`. Bucket labels are produced by
    :func:`bucket_labels`; pass the same `lines_breaks` / `files_breaks`
    to :func:`bucket_labels` to recover the canonical ordering for plots.

    Rows with null `additions` or `deletions` get null `lines_changed`
    and null `lines_bucket`; similarly for `files_col`.
    """
    lines_labels = bucket_labels(lines_breaks)
    files_labels = bucket_labels(files_breaks)
    return df_prs.with_columns(
        (pl.col(additions_col) + pl.col(deletions_col)).alias("lines_changed")
    ).with_columns(
        [
            _bucket_expr(pl.col("lines_changed"), lines_breaks, lines_labels).alias(
                "lines_bucket"
            ),
            _bucket_expr(pl.col(files_col), files_breaks, files_labels).alias(
                "files_bucket"
            ),
        ]
    )


def author_cohort(
    df_prs: pl.DataFrame,
    *,
    author_col: str = "author_id",
    created_col: str = "gh_created_at",
    id_col: str = "id",
) -> pl.DataFrame:
    """Augment a PR frame with per-author sequence info.

    Adds columns:

    - `author_first_pr_at`: timestamp of the author's earliest PR in
      `df_prs` (by `created_col`).
    - `author_pr_seq`: 1-indexed position of this PR in the author's
      history within `df_prs` (ordered by `created_col`, tiebroken by
      `id_col`).
    - `is_first_pr`: `author_pr_seq == 1`.

    "First-time" is relative to the frame supplied — pass the full PR
    table for project-wide first-time cohorts, or a filtered frame
    (e.g. merged-to-master only) for "first merged PR" cohorts.

    Rows with null `author_id` get null cohort columns rather than being
    bucketed together as a single phantom author.
    """
    has_author = pl.col(author_col).is_not_null()
    sorted_df = df_prs.sort([author_col, created_col, id_col])
    return sorted_df.with_columns(
        [
            pl.when(has_author)
            .then(pl.col(created_col).min().over(author_col))
            .alias("author_first_pr_at"),
            pl.when(has_author)
            .then(pl.col(id_col).cum_count().over(author_col).cast(pl.Int64))
            .alias("author_pr_seq"),
        ]
    ).with_columns(
        pl.when(has_author).then(pl.col("author_pr_seq") == 1).alias("is_first_pr")
    )


def started_as_draft(
    df_prs: pl.DataFrame,
    df_events: pl.DataFrame,
    *,
    pr_id_col: str = "id",
    draft_col: str = "is_draft",
    draft_true: str = "t",
) -> pl.DataFrame:
    """Add `started_as_draft` (bool) to a PR frame.

    Logic:

    - If the PR has any `READY_FOR_REVIEW` or `CONVERT_TO_DRAFT` event
      in `df_events`, the *first* such event determines the state at
      open: `READY_FOR_REVIEW` first → started as draft (had to be a
      draft to emit RFR before any CTD); `CONVERT_TO_DRAFT` first →
      started non-draft.
    - If no such events exist, fall back to the current `prs.is_draft`
      column: `draft_true` → started as draft, else False. This catches
      PRs that opened as draft and never marked ready (mostly currently
      open).

    The `is_draft` column comes out of the queueboard parquet export as
    a Postgres-style `"t"` / `"f"` string, not a real boolean. Override
    `draft_true` if the source schema changes.
    """
    draft_events = (
        df_events.filter(pl.col("type").is_in(["READY_FOR_REVIEW", "CONVERT_TO_DRAFT"]))
        .drop_nulls(["pull_request_id", "occurred_at"])
        .sort(["pull_request_id", "occurred_at"])
        .group_by("pull_request_id", maintain_order=True)
        .agg(pl.col("type").first().alias("_first_draft_event"))
    )
    return (
        df_prs.join(
            draft_events,
            left_on=pr_id_col,
            right_on="pull_request_id",
            how="left",
        )
        .with_columns(
            pl.when(pl.col("_first_draft_event").is_not_null())
            .then(pl.col("_first_draft_event") == "READY_FOR_REVIEW")
            .otherwise(pl.col(draft_col) == draft_true)
            .alias("started_as_draft")
        )
        .drop("_first_draft_event")
    )
