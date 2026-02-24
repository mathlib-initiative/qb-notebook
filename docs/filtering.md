# Filtering Guide

`qb_notebook.filters` provides reusable Polars-expression builders plus helpers for combining them.

## Core pattern

Use expression builders and combine them with `filter_rows`:

```python
from qb_notebook.filters import expr_title_regex, expr_repo_in, filter_rows

filtered = filter_rows(
    df,
    expr_repo_in([123, 456]),
    expr_title_regex(r"^feat"),
)
```

## Column overrides for schema variants

Most helpers accept optional `*_col` keyword arguments. This keeps semantics stable even when physical column names differ.

```python
from qb_notebook.filters import expr_repo_in, expr_title_regex, filter_rows

filtered = filter_rows(
    df,
    expr_repo_in([10], repo_col="repo_id"),
    expr_title_regex(r"^feat", title_col="pr_title"),
)
```

## Interval filtering

Use `expr_interval_started_between` for time bounds:

```python
from qb_notebook.filters import expr_interval_started_between, filter_rows

filtered = filter_rows(
    intervals,
    expr_interval_started_between(
        start_after="2025-01-01",
        start_before="2026-01-01",
        start_col="start",
    ),
)
```

## Labels

Use label-id helpers to derive PR ids, then convert to expressions:

```python
from qb_notebook.filters import pr_ids_with_any_labels, expr_pr_has_any_of, filter_rows

ids = pr_ids_with_any_labels(df_prlabel, df_label_defs, ["new-contributor"])
filtered = filter_rows(intervals, expr_pr_has_any_of(ids))
```

## API note

- `filter_rows(df, *exprs)` is the API for combining expression filters.
