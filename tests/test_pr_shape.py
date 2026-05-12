from datetime import datetime, timezone

import polars as pl

from qb_notebook.pr_shape import (
    DEFAULT_FILES_BREAKS,
    DEFAULT_LINES_BREAKS,
    DEFAULT_PR_TYPES,
    author_cohort,
    bucket_labels,
    pr_type,
    pr_type_order,
    size_buckets,
    started_as_draft,
)


def _dt(day: int, hour: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, tzinfo=timezone.utc)


def _prs(rows: list[dict]) -> pl.DataFrame:
    schema = {
        "id": pl.Int64,
        "author_id": pl.Float64,
        "gh_created_at": pl.Datetime("us", "UTC"),
        "additions": pl.Int64,
        "deletions": pl.Int64,
        "changed_files_count": pl.Int64,
        "is_draft": pl.String,
    }
    return pl.DataFrame(rows, schema=schema)


def _events(rows: list[dict]) -> pl.DataFrame:
    schema = {
        "pull_request_id": pl.Int64,
        "occurred_at": pl.Datetime("us", "UTC"),
        "type": pl.String,
    }
    return pl.DataFrame(rows, schema=schema)


def test_bucket_labels_canonical_form() -> None:
    assert bucket_labels((10, 50, 200, 1000)) == [
        "0-10",
        "11-50",
        "51-200",
        "201-1000",
        "1001+",
    ]
    # Single break still works.
    assert bucket_labels((5,)) == ["0-5", "6+"]


def test_size_buckets_assigns_expected_buckets() -> None:
    rows = [
        {
            "id": 1,
            "author_id": 1.0,
            "gh_created_at": _dt(1),
            "additions": 3,
            "deletions": 2,
            "changed_files_count": 1,
            "is_draft": "f",
        },
        {
            "id": 2,
            "author_id": 1.0,
            "gh_created_at": _dt(2),
            "additions": 40,
            "deletions": 10,
            "changed_files_count": 2,
            "is_draft": "f",
        },
        {
            "id": 3,
            "author_id": 2.0,
            "gh_created_at": _dt(3),
            "additions": 600,
            "deletions": 600,
            "changed_files_count": 50,
            "is_draft": "f",
        },
    ]
    out = size_buckets(_prs(rows))
    by_id = {r["id"]: r for r in out.iter_rows(named=True)}
    assert by_id[1]["lines_changed"] == 5
    assert by_id[1]["lines_bucket"] == "0-10"
    assert by_id[1]["files_bucket"] == "0-1"
    assert by_id[2]["lines_changed"] == 50
    assert by_id[2]["lines_bucket"] == "11-50"
    assert by_id[2]["files_bucket"] == "2-3"
    assert by_id[3]["lines_changed"] == 1200
    assert by_id[3]["lines_bucket"] == "1001+"
    assert by_id[3]["files_bucket"] == "31+"


def test_size_buckets_null_propagates() -> None:
    rows = [
        {
            "id": 1,
            "author_id": 1.0,
            "gh_created_at": _dt(1),
            "additions": None,
            "deletions": 10,
            "changed_files_count": 1,
            "is_draft": "f",
        },
        {
            "id": 2,
            "author_id": 1.0,
            "gh_created_at": _dt(2),
            "additions": 20,
            "deletions": 30,
            "changed_files_count": None,
            "is_draft": "f",
        },
    ]
    out = size_buckets(_prs(rows))
    by_id = {r["id"]: r for r in out.iter_rows(named=True)}
    assert by_id[1]["lines_bucket"] is None
    assert by_id[1]["files_bucket"] == "0-1"
    assert by_id[2]["lines_bucket"] == "11-50"
    assert by_id[2]["files_bucket"] is None


def test_size_buckets_breaks_constants_round_trip() -> None:
    # Sanity: the module's exported breaks match the canonical label form.
    assert bucket_labels(DEFAULT_LINES_BREAKS)[0] == "0-10"
    assert bucket_labels(DEFAULT_FILES_BREAKS)[-1] == "31+"


def test_author_cohort_sequence_and_first_pr_flag() -> None:
    rows = [
        # alice: PRs on day 1 and day 5 → seq 1, 2
        {
            "id": 10,
            "author_id": 1.0,
            "gh_created_at": _dt(5),
            "additions": 0,
            "deletions": 0,
            "changed_files_count": 0,
            "is_draft": "f",
        },
        {
            "id": 11,
            "author_id": 1.0,
            "gh_created_at": _dt(1),
            "additions": 0,
            "deletions": 0,
            "changed_files_count": 0,
            "is_draft": "f",
        },
        # bob: single PR → seq 1, is_first
        {
            "id": 12,
            "author_id": 2.0,
            "gh_created_at": _dt(3),
            "additions": 0,
            "deletions": 0,
            "changed_files_count": 0,
            "is_draft": "f",
        },
        # null author: should get null cohort cols
        {
            "id": 13,
            "author_id": None,
            "gh_created_at": _dt(2),
            "additions": 0,
            "deletions": 0,
            "changed_files_count": 0,
            "is_draft": "f",
        },
    ]
    out = author_cohort(_prs(rows))
    by_id = {r["id"]: r for r in out.iter_rows(named=True)}
    assert by_id[11]["author_pr_seq"] == 1
    assert by_id[11]["is_first_pr"] is True
    assert by_id[11]["author_first_pr_at"] == _dt(1)
    assert by_id[10]["author_pr_seq"] == 2
    assert by_id[10]["is_first_pr"] is False
    assert by_id[10]["author_first_pr_at"] == _dt(1)
    assert by_id[12]["author_pr_seq"] == 1
    assert by_id[12]["is_first_pr"] is True
    assert by_id[13]["author_pr_seq"] is None
    assert by_id[13]["is_first_pr"] is None
    assert by_id[13]["author_first_pr_at"] is None


def test_started_as_draft_first_event_is_rfr() -> None:
    prs = _prs(
        [
            {
                "id": 1,
                "author_id": 1.0,
                "gh_created_at": _dt(1),
                "additions": 0,
                "deletions": 0,
                "changed_files_count": 0,
                "is_draft": "f",
            },
        ]
    )
    events = _events(
        [
            {"pull_request_id": 1, "occurred_at": _dt(2), "type": "READY_FOR_REVIEW"},
            {"pull_request_id": 1, "occurred_at": _dt(3), "type": "CONVERT_TO_DRAFT"},
        ]
    )
    out = started_as_draft(prs, events)
    assert out.row(0, named=True)["started_as_draft"] is True


def test_started_as_draft_first_event_is_ctd() -> None:
    prs = _prs(
        [
            {
                "id": 1,
                "author_id": 1.0,
                "gh_created_at": _dt(1),
                "additions": 0,
                "deletions": 0,
                "changed_files_count": 0,
                "is_draft": "f",
            },
        ]
    )
    events = _events(
        [
            {"pull_request_id": 1, "occurred_at": _dt(2), "type": "CONVERT_TO_DRAFT"},
            {"pull_request_id": 1, "occurred_at": _dt(3), "type": "READY_FOR_REVIEW"},
        ]
    )
    out = started_as_draft(prs, events)
    assert out.row(0, named=True)["started_as_draft"] is False


def test_started_as_draft_no_events_falls_back_to_is_draft() -> None:
    prs = _prs(
        [
            # PR 1: currently draft, no events → started_as_draft = True
            {
                "id": 1,
                "author_id": 1.0,
                "gh_created_at": _dt(1),
                "additions": 0,
                "deletions": 0,
                "changed_files_count": 0,
                "is_draft": "t",
            },
            # PR 2: not draft, no events → False
            {
                "id": 2,
                "author_id": 1.0,
                "gh_created_at": _dt(2),
                "additions": 0,
                "deletions": 0,
                "changed_files_count": 0,
                "is_draft": "f",
            },
        ]
    )
    events = _events([])
    out = started_as_draft(prs, events)
    by_id = {r["id"]: r["started_as_draft"] for r in out.iter_rows(named=True)}
    assert by_id[1] is True
    assert by_id[2] is False


def _titled_prs(rows: list[tuple[int, str | None]]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"id": i, "title": t} for i, t in rows],
        schema={"id": pl.Int64, "title": pl.String},
    )


def test_pr_type_canonical_prefixes() -> None:
    out = pr_type(
        _titled_prs(
            [
                (1, "feat: add lemma"),
                (2, "chore: bump version"),
                (3, "fix: off-by-one in foo"),
                (4, "refactor: rename Bar to Baz"),
                (5, "doc: update README"),
                (6, "perf: faster simp"),
                (7, "ci: pin GH Actions"),
                (8, "style: tidy whitespace"),
                (9, "test: cover edge case"),
            ]
        )
    )
    by_id = {r["id"]: r["pr_type"] for r in out.iter_rows(named=True)}
    assert by_id == {
        1: "feat",
        2: "chore",
        3: "fix",
        4: "refactor",
        5: "doc",
        6: "perf",
        7: "ci",
        8: "style",
        9: "test",
    }


def test_pr_type_strips_bors_prefix_and_scope() -> None:
    out = pr_type(
        _titled_prs(
            [
                (1, "[Merged by Bors] - feat: scope-free"),
                (2, "[Merged by Bors] - feat(Algebra/Group): in scope"),
                (3, "feat(Topology): live"),
                # Tolerate odd whitespace around the dash and the colon.
                (4, "[Merged by Bors]  -  chore : deps"),
            ]
        )
    )
    by_id = {r["id"]: r["pr_type"] for r in out.iter_rows(named=True)}
    assert by_id == {1: "feat", 2: "feat", 3: "feat", 4: "chore"}


def test_pr_type_alias_remap() -> None:
    out = pr_type(
        _titled_prs(
            [
                (1, "feature: same as feat"),
                (2, "docs: same as doc"),
                (3, "Feat: case-insensitive"),
            ]
        )
    )
    by_id = {r["id"]: r["pr_type"] for r in out.iter_rows(named=True)}
    assert by_id == {1: "feat", 2: "doc", 3: "feat"}


def test_pr_type_other_and_unparsed_buckets() -> None:
    out = pr_type(
        _titled_prs(
            [
                # Parsed but not canonical -> "other".
                (1, "experiment: try a thing"),
                (2, "wip: not ready"),
                # No conventional prefix -> "unparsed".
                (3, "Add missing lemma to Mathlib.Foo"),
                (4, "Just a sentence with no colon prefix"),
                # Null title -> null pr_type.
                (5, None),
            ]
        )
    )
    by_id = {r["id"]: r["pr_type"] for r in out.iter_rows(named=True)}
    assert by_id[1] == "other"
    assert by_id[2] == "other"
    assert by_id[3] == "unparsed"
    assert by_id[4] == "unparsed"
    assert by_id[5] is None


def test_pr_type_order_helper_matches_canonical() -> None:
    order = pr_type_order()
    assert order[: len(DEFAULT_PR_TYPES)] == list(DEFAULT_PR_TYPES)
    assert order[-2:] == ["other", "unparsed"]
