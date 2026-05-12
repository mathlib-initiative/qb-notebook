from datetime import datetime, timezone

import polars as pl

from qb_notebook.filters import (
    expr_closed_by_event_type,
    expr_commenters_include_any,
    expr_interval_started_between,
    expr_is_draft,
    expr_merged_at_effective,
    expr_merged_to_master,
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


def _merge_fixture() -> pl.DataFrame:
    """Six representative PRs covering the bors / GitHub / collateral cases."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "base_ref_name": [
                "master",  # bors-merged to master
                "bump/v4.21.0",  # bors-merged to a release branch
                "master",  # GitHub-merged to master
                "master",  # bors closed it (branch deletion) but not a merge
                "master",  # still open
                "master",  # genuinely abandoned
            ],
            "state": ["closed", "closed", "merged", "closed", "open", "closed"],
            "title": [
                "[Merged by Bors] - feat: foo",
                "[Merged by Bors] - chore: bump",
                "feat: direct merge",
                "feat: collateral closure",
                "feat: still open",
                "feat: abandoned",
            ],
            "merged_at": [
                None,
                None,
                datetime(2025, 3, 1, tzinfo=timezone.utc),
                None,
                None,
                None,
            ],
            "closed_at": [
                datetime(2025, 4, 1, tzinfo=timezone.utc),
                datetime(2025, 4, 2, tzinfo=timezone.utc),
                datetime(2025, 3, 1, tzinfo=timezone.utc),
                datetime(2025, 4, 3, tzinfo=timezone.utc),
                None,
                datetime(2025, 4, 4, tzinfo=timezone.utc),
            ],
        }
    )


def test_expr_merged_to_master_matches_bors_and_github() -> None:
    df = _merge_fixture()
    out = df.filter(expr_merged_to_master()).sort("id")
    assert out["id"].to_list() == [1, 3]


def test_expr_merged_to_master_excludes_non_master_branches() -> None:
    df = _merge_fixture()
    out = df.filter(expr_merged_to_master())
    assert 2 not in out["id"].to_list()  # bors-merged but bump/v4.21.0


def test_expr_merged_to_master_excludes_collateral_closures() -> None:
    df = _merge_fixture()
    out = df.filter(expr_merged_to_master())
    # PR 4 was closed by bors as branch-deletion collateral; no title rewrite.
    assert 4 not in out["id"].to_list()


def test_expr_merged_to_master_excludes_open_and_abandoned() -> None:
    df = _merge_fixture()
    out = df.filter(expr_merged_to_master())
    assert 5 not in out["id"].to_list()
    assert 6 not in out["id"].to_list()


def test_expr_merged_to_master_respects_custom_base_branch() -> None:
    df = _merge_fixture()
    out = df.filter(expr_merged_to_master(base_branch="bump/v4.21.0"))
    assert out["id"].to_list() == [2]


def test_expr_is_draft_matches_postgres_string_default() -> None:
    df = pl.DataFrame({"id": [1, 2, 3], "is_draft": ["t", "f", "t"]})
    out = df.filter(expr_is_draft()).sort("id")
    assert out["id"].to_list() == [1, 3]


def test_expr_is_draft_negation_on_string_column() -> None:
    df = pl.DataFrame({"id": [1, 2, 3], "is_draft": ["t", "f", "t"]})
    out = df.filter(expr_is_draft(is_draft=False))
    assert out["id"].to_list() == [2]


def test_expr_is_draft_accepts_bool_override() -> None:
    df = pl.DataFrame({"id": [1, 2, 3], "is_draft": [True, False, True]})
    out = df.filter(expr_is_draft(draft_true=True)).sort("id")
    assert out["id"].to_list() == [1, 3]


def test_expr_merged_at_effective_prefers_merged_at() -> None:
    df = _merge_fixture().filter(expr_merged_to_master())
    out = df.with_columns(expr_merged_at_effective().alias("merge_ts")).sort("id")
    # PR 1 is bors-merged (merged_at null → fall back to closed_at);
    # PR 3 is GitHub-merged (merged_at populated).
    assert out["merge_ts"].to_list() == [
        datetime(2025, 4, 1, tzinfo=timezone.utc),
        datetime(2025, 3, 1, tzinfo=timezone.utc),
    ]
