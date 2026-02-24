from __future__ import annotations

from datetime import datetime, timezone
from typing import Union

import pandas as pd
import polars as pl

DateLike = Union[str, datetime]


def expr_title_contains(
    substr: str,
    *,
    case_insensitive: bool = True,
    title_col: str = "title",
) -> pl.Expr:
    """Literal substring match on a title-like column."""
    col = pl.col(title_col).fill_null("")
    if case_insensitive:
        return col.str.to_lowercase().str.contains(substr.lower(), literal=True)
    return col.str.contains(substr, literal=True)


def expr_title_any(
    substrs: list[str],
    *,
    case_insensitive: bool = True,
    title_col: str = "title",
) -> pl.Expr:
    e = pl.lit(False)
    for s in substrs:
        e = e | expr_title_contains(
            s,
            case_insensitive=case_insensitive,
            title_col=title_col,
        )
    return e


def expr_title_regex(
    pattern: str,
    *,
    case_insensitive: bool = True,
    title_col: str = "title",
) -> pl.Expr:
    """Regex match on a title-like column."""
    if case_insensitive:
        pattern = "(?i)" + pattern
    return pl.col(title_col).fill_null("").str.contains(pattern, literal=False)


def expr_title_exclude_any(
    substrs: list[str],
    *,
    case_insensitive: bool = True,
    title_col: str = "title",
) -> pl.Expr:
    return ~expr_title_any(
        substrs,
        case_insensitive=case_insensitive,
        title_col=title_col,
    )


def expr_repo_in(repo_ids: list[int], *, repo_col: str = "repository_id") -> pl.Expr:
    return pl.col(repo_col).is_in(repo_ids)


def expr_author_in(author_ids: list[int], *, author_col: str = "author_id") -> pl.Expr:
    return pl.col(author_col).is_in(author_ids)


def expr_base_branch_in(
    branches: list[str], *, base_branch_col: str = "base_ref_name"
) -> pl.Expr:
    return pl.col(base_branch_col).is_in(branches)


def expr_state_is(state: str, *, state_col: str = "state") -> pl.Expr:
    return pl.col(state_col) == state


def expr_is_draft(is_draft: bool = True, *, draft_col: str = "is_draft") -> pl.Expr:
    return pl.col(draft_col) == is_draft


def expr_additions_between(
    lo: int | None = None,
    hi: int | None = None,
    *,
    additions_col: str = "additions",
) -> pl.Expr:
    e = pl.lit(True)
    if lo is not None:
        e = e & (pl.col(additions_col).fill_null(0) >= lo)
    if hi is not None:
        e = e & (pl.col(additions_col).fill_null(0) <= hi)
    return e


def expr_churn_between(
    lo: int | None = None,
    hi: int | None = None,
    *,
    additions_col: str = "additions",
    deletions_col: str = "deletions",
) -> pl.Expr:
    churn = pl.col(additions_col).fill_null(0) + pl.col(deletions_col).fill_null(0)
    e = pl.lit(True)
    if lo is not None:
        e = e & (churn >= lo)
    if hi is not None:
        e = e & (churn <= hi)
    return e


def _to_utc_datetime(x: DateLike | None) -> datetime | None:
    if x is None:
        return None
    if isinstance(x, datetime):
        if x.tzinfo is None:
            return x.replace(tzinfo=timezone.utc)
        return x.astimezone(timezone.utc)
    if isinstance(x, str):
        try:
            dt = pd.to_datetime(x, utc=True)
        except Exception as e:
            raise ValueError(f"Could not parse datetime string: {x!r}") from e
        return dt.to_pydatetime()
    raise TypeError(f"Expected datetime or str, got {type(x)}")


def expr_interval_started_between(
    *,
    start_after: DateLike | None = None,
    start_before: DateLike | None = None,
    start_col: str = "start",
) -> pl.Expr:
    start_after_dt = _to_utc_datetime(start_after)
    start_before_dt = _to_utc_datetime(start_before)
    e = pl.lit(True)
    if start_after_dt is not None:
        e = e & (pl.col(start_col) >= pl.lit(start_after_dt))
    if start_before_dt is not None:
        e = e & (pl.col(start_col) < pl.lit(start_before_dt))
    return e


def expr_only_closed(
    only_closed: bool = True,
    *,
    is_open_ended_col: str = "is_open_ended",
) -> pl.Expr:
    return pl.col(is_open_ended_col) == (not only_closed)


def pr_ids_with_any_labels(
    df_prlabel: pl.DataFrame,
    df_label_defs: pl.DataFrame,
    labels: list[str],
    repository_id: int | None = None,
    *,
    prlabel_label_col: str = "label_def_id",
    prlabel_pr_col: str = "pull_request_id",
    labeldef_id_col: str = "id",
    labeldef_name_col: str = "name",
    labeldef_repo_col: str = "repository_id",
) -> pl.Series:
    label_defs = df_label_defs.filter(pl.col(labeldef_name_col).is_in(labels))
    if repository_id is not None:
        label_defs = label_defs.filter(pl.col(labeldef_repo_col) == repository_id)

    label_ids = label_defs.select(pl.col(labeldef_id_col).alias(prlabel_label_col))
    return (
        df_prlabel.join(label_ids, on=prlabel_label_col, how="inner")
        .select(prlabel_pr_col)
        .unique()
        .get_column(prlabel_pr_col)
    )


def pr_ids_with_all_labels(
    df_prlabel: pl.DataFrame,
    df_label_defs: pl.DataFrame,
    labels: list[str],
    repository_id: int | None = None,
    *,
    prlabel_label_col: str = "label_def_id",
    prlabel_pr_col: str = "pull_request_id",
    labeldef_id_col: str = "id",
    labeldef_name_col: str = "name",
    labeldef_repo_col: str = "repository_id",
) -> pl.Series:
    label_defs = df_label_defs.filter(pl.col(labeldef_name_col).is_in(labels))
    if repository_id is not None:
        label_defs = label_defs.filter(pl.col(labeldef_repo_col) == repository_id)

    label_ids = label_defs.select(
        pl.col(labeldef_id_col).alias(prlabel_label_col)
    ).unique()
    n = len(labels)
    return (
        df_prlabel.join(label_ids, on=prlabel_label_col, how="inner")
        .group_by(prlabel_pr_col)
        .agg(pl.col(prlabel_label_col).n_unique().alias("n"))
        .filter(pl.col("n") == n)
        .select(prlabel_pr_col)
        .get_column(prlabel_pr_col)
    )


def expr_pr_has_any_of(
    pr_ids: pl.Series, *, pr_id_col: str = "pull_request_id"
) -> pl.Expr:
    return pl.col(pr_id_col).is_in(pr_ids.implode())


def expr_pr_lacks_any_of(
    pr_ids: pl.Series, *, pr_id_col: str = "pull_request_id"
) -> pl.Expr:
    return ~pl.col(pr_id_col).is_in(pr_ids.implode())


def filter_rows(df: pl.DataFrame, *exprs: pl.Expr) -> pl.DataFrame:
    """Apply multiple Polars boolean expressions as filters."""
    if not exprs:
        return df
    e = pl.lit(True)
    for ex in exprs:
        e = e & ex
    return df.filter(e)


def filter_intervals(df: pl.DataFrame, *exprs: pl.Expr) -> pl.DataFrame:
    """Backwards-compatible alias for notebook code."""
    return filter_rows(df, *exprs)
