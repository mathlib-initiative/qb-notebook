from datetime import datetime, timezone

import polars as pl

from qb_notebook.filters import (
    expr_closed_by_event_type,
    expr_commenters_include_any,
    expr_interval_started_between,
    expr_opened_by_event_type,
    expr_repo_in,
    expr_title_regex,
    filter_rows,
    pr_ids_with_any_labels,
)


def test_expr_repo_in_with_custom_column() -> None:
    df = pl.DataFrame({"repo_id": [1, 2, 3], "v": [10, 20, 30]})
    out = df.filter(expr_repo_in([2, 3], repo_col="repo_id"))
    assert out["v"].to_list() == [20, 30]


def test_expr_title_regex_with_custom_column() -> None:
    df = pl.DataFrame({"pr_title": ["feat: x", "fix: y", None]})
    out = df.filter(expr_title_regex(r"^feat", title_col="pr_title"))
    assert out.height == 1
    assert out["pr_title"][0] == "feat: x"


def test_filter_rows_combines_expressions() -> None:
    df = pl.DataFrame({"repo_id": [1, 1, 2], "pr_title": ["feat a", "fix b", "feat c"]})
    out = filter_rows(
        df,
        expr_repo_in([1], repo_col="repo_id"),
        expr_title_regex(r"^feat", title_col="pr_title"),
    )
    assert out.height == 1
    assert out["pr_title"][0] == "feat a"


def test_pr_ids_with_any_labels_with_custom_columns() -> None:
    df_label_defs = pl.DataFrame(
        {
            "label_id": [11, 12],
            "label_name": ["new-contributor", "bug"],
            "repo_id": [99, 99],
        }
    )
    df_prlabel = pl.DataFrame(
        {
            "label_ref": [11, 12, 11],
            "pr_id": [100, 100, 101],
            "created_at": [
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 2, tzinfo=timezone.utc),
            ],
        }
    )

    ids = pr_ids_with_any_labels(
        df_prlabel,
        df_label_defs,
        labels=["new-contributor"],
        repository_id=99,
        prlabel_label_col="label_ref",
        prlabel_pr_col="pr_id",
        labeldef_id_col="label_id",
        labeldef_name_col="label_name",
        labeldef_repo_col="repo_id",
    )
    assert sorted(ids.to_list()) == [100, 101]


def test_expr_interval_started_between_with_string_dates() -> None:
    df = pl.DataFrame(
        {
            "window_start": [
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 2, 1, tzinfo=timezone.utc),
            ]
        }
    )

    out = df.filter(
        expr_interval_started_between(
            start_after="2025-01-15",
            start_before="2025-03-01",
            start_col="window_start",
        )
    )
    assert out.height == 1
    assert out["window_start"][0] == datetime(2025, 2, 1, tzinfo=timezone.utc)


def test_expr_commenters_include_any_matches_login() -> None:
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "commenters": [
                '["alice", "bob"]',
                '["carol"]',
                None,
            ],
        }
    )
    out = df.filter(expr_commenters_include_any(["bob", "dave"]))
    assert out["id"].to_list() == [1]


def test_expr_commenters_include_any_null_treated_as_empty() -> None:
    df = pl.DataFrame({"id": [1], "commenters": [None]})
    out = df.filter(expr_commenters_include_any(["alice"]))
    assert out.is_empty()


def test_expr_commenters_include_any_empty_logins_matches_nothing() -> None:
    df = pl.DataFrame({"id": [1], "commenters": ['["alice"]']})
    out = df.filter(expr_commenters_include_any([]))
    assert out.is_empty()


def test_expr_opened_by_event_type_matches() -> None:
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "opened_by_event_type": [
                "CI_PASSED",
                "INITIAL_STATE",
                "FORBIDDEN_LABEL_REMOVED",
                None,
            ],
        }
    )
    out = df.filter(expr_opened_by_event_type(["CI_PASSED", "INITIAL_STATE"]))
    assert sorted(out["id"].to_list()) == [1, 2]


def test_expr_closed_by_event_type_matches() -> None:
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "closed_by_event_type": [
                "CI_FAILED",
                "FORBIDDEN_LABEL_ADDED",
                "HEAD_PUSHED",
                None,
            ],
        }
    )
    out = df.filter(expr_closed_by_event_type(["CI_FAILED"]))
    assert out["id"].to_list() == [1]


def test_expr_opened_by_event_type_custom_col() -> None:
    df = pl.DataFrame({"id": [1, 2], "etype": ["CI_PASSED", "PR_OPENED"]})
    out = df.filter(expr_opened_by_event_type(["PR_OPENED"], event_type_col="etype"))
    assert out["id"].to_list() == [2]
