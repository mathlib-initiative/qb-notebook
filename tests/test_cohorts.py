from datetime import date, datetime, timezone

import polars as pl
import pytest

from qb_notebook.cohorts import (
    AUTHOR_FIRST_TIME,
    AUTHOR_RETURNING,
    DEFAULT_STAGES,
    MAINTAINER_FIRST_SECONDS,
    STAGE_FIRST_TOUCH_TO_MM,
    STAGE_OPEN_TO_FIRST_TOUCH,
    TOPIC_NONE,
    CohortSpec,
    Stage,
    cohort_frames,
    filter_cohort,
    milestone_summary,
    stage_quantiles,
    stage_series,
    window_bounds,
    with_unique_names,
)


def _dt(day: int, hour: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, tzinfo=timezone.utc)


def _prs(rows: list[dict]) -> pl.DataFrame:
    """A wide per-PR frame in the shape `filter_cohort` expects."""
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "opened_at": pl.Datetime("us", "UTC"),
            "pr_type": pl.String,
            "lines_bucket": pl.String,
            "is_first_pr": pl.Boolean,
        },
    )


def _t_intervals(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"pull_request_id": pl.Int64, "label_name": pl.String},
    )


_FRAME = _prs(
    [
        {
            "pull_request_id": 1,
            "opened_at": _dt(1),
            "pr_type": "feat",
            "lines_bucket": "0-10",
            "is_first_pr": True,
        },
        {
            "pull_request_id": 2,
            "opened_at": _dt(2, 23),
            "pr_type": "chore",
            "lines_bucket": "11-50",
            "is_first_pr": False,
        },
        {
            "pull_request_id": 3,
            "opened_at": _dt(5),
            "pr_type": "feat",
            "lines_bucket": "11-50",
            "is_first_pr": False,
        },
        {
            "pull_request_id": 4,
            "opened_at": _dt(9),
            "pr_type": "doc",
            "lines_bucket": "0-10",
            "is_first_pr": None,
        },
    ]
)

_T_INTERVALS = _t_intervals(
    [
        {"pull_request_id": 1, "label_name": "t-algebra"},
        {"pull_request_id": 2, "label_name": "t-topology"},
        {"pull_request_id": 2, "label_name": "t-algebra"},
        # PRs 3 and 4 never carried a t-* label.
    ]
)


def _ids(df: pl.DataFrame) -> list[int]:
    return df.get_column("pull_request_id").to_list()


def test_window_bounds_end_is_inclusive_day() -> None:
    lo, hi = window_bounds(
        CohortSpec("c", start=date(2025, 1, 2), end=date(2025, 1, 2))
    )
    assert lo == _dt(2)
    assert hi == _dt(3)


def test_window_bounds_open_ended() -> None:
    assert window_bounds(CohortSpec("c")) == (None, None)
    lo, hi = window_bounds(CohortSpec("c", start=date(2025, 1, 4)))
    assert lo == _dt(4)
    assert hi is None


def test_empty_spec_selects_everything() -> None:
    assert _ids(filter_cohort(_FRAME, CohortSpec("all"))) == [1, 2, 3, 4]


def test_date_filter_includes_both_endpoint_days() -> None:
    # PR 2 opened at 23:00 on the end day: it must survive the half-open
    # `< end + 1 day` upper bound.
    spec = CohortSpec("c", start=date(2025, 1, 1), end=date(2025, 1, 2))
    assert _ids(filter_cohort(_FRAME, spec)) == [1, 2]


def test_topic_filter_matches_any_selected_label() -> None:
    spec = CohortSpec("c", topics=("t-algebra",))
    assert _ids(filter_cohort(_FRAME, spec, t_intervals=_T_INTERVALS)) == [1, 2]


def test_topic_none_bucket_selects_unlabeled_prs() -> None:
    spec = CohortSpec("c", topics=(TOPIC_NONE,))
    assert _ids(filter_cohort(_FRAME, spec, t_intervals=_T_INTERVALS)) == [3, 4]


def test_topic_none_unions_with_real_labels() -> None:
    spec = CohortSpec("c", topics=("t-topology", TOPIC_NONE))
    assert _ids(filter_cohort(_FRAME, spec, t_intervals=_T_INTERVALS)) == [2, 3, 4]


def test_topic_filter_without_intervals_raises() -> None:
    with pytest.raises(ValueError, match="t_intervals"):
        filter_cohort(_FRAME, CohortSpec("c", topics=("t-algebra",)))


def test_pr_type_and_size_filters_are_anded() -> None:
    spec = CohortSpec("c", pr_types=("feat",), lines_buckets=("11-50",))
    assert _ids(filter_cohort(_FRAME, spec)) == [3]


def test_author_cohort_filters_exclude_null_is_first_pr() -> None:
    first = CohortSpec("c", author_cohort=AUTHOR_FIRST_TIME)
    returning = CohortSpec("c", author_cohort=AUTHOR_RETURNING)
    assert _ids(filter_cohort(_FRAME, first)) == [1]
    # PR 4 has a null `is_first_pr` (no author on record) and is in neither.
    assert _ids(filter_cohort(_FRAME, returning)) == [2, 3]


def test_unknown_author_cohort_raises() -> None:
    with pytest.raises(ValueError, match="author_cohort"):
        filter_cohort(_FRAME, CohortSpec("c", author_cohort="nope"))


def test_with_unique_names_fills_blanks_and_dedupes() -> None:
    named = with_unique_names(
        [CohortSpec(""), CohortSpec("A"), CohortSpec("A"), CohortSpec("  A  ")]
    )
    assert [s.name for s in named] == ["cohort 1", "A", "A (2)", "A (3)"]


def test_with_unique_names_is_idempotent() -> None:
    once = with_unique_names([CohortSpec("A"), CohortSpec("A")])
    assert [s.name for s in with_unique_names(once)] == [s.name for s in once]


def test_with_unique_names_preserves_other_fields() -> None:
    spec = CohortSpec("A", pr_types=("feat",), color="#eb6834")
    (out,) = with_unique_names([spec])
    assert out is spec


def test_cohort_frames_keys_on_unique_names() -> None:
    frames = cohort_frames(
        _FRAME,
        [CohortSpec("dup", pr_types=("feat",)), CohortSpec("dup", pr_types=("doc",))],
    )
    assert list(frames) == ["dup", "dup (2)"]
    assert _ids(frames["dup"]) == [1, 3]
    assert _ids(frames["dup (2)"]) == [4]


def _pipeline(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pull_request_id": pl.Int64,
            "first_touch_at": pl.Datetime("us", "UTC"),
            "first_maintainer_merge_at": pl.Datetime("us", "UTC"),
            "first_ready_to_merge_at": pl.Datetime("us", "UTC"),
            "is_merged": pl.Boolean,
            "is_closed": pl.Boolean,
            "seconds_open_to_first_touch": pl.Float64,
            "seconds_first_touch_to_maintainer_merge": pl.Float64,
            "seconds_maintainer_merge_to_ready_to_merge": pl.Float64,
            "seconds_ready_to_merge_to_merged": pl.Float64,
            "seconds_open_to_merged": pl.Float64,
        },
    )


_PIPELINE = _pipeline(
    [
        # merged, traversed every milestone
        {
            "pull_request_id": 1,
            "first_touch_at": _dt(1),
            "first_maintainer_merge_at": _dt(2),
            "first_ready_to_merge_at": _dt(3),
            "is_merged": True,
            "is_closed": True,
            "seconds_open_to_first_touch": 3600.0,
            "seconds_first_touch_to_maintainer_merge": 10.0,
            "seconds_maintainer_merge_to_ready_to_merge": 86400.0,
            "seconds_ready_to_merge_to_merged": 3600.0,
            "seconds_open_to_merged": 86400.0,
        },
        # merged without ever getting the maintainer-merge label
        {
            "pull_request_id": 2,
            "first_touch_at": _dt(1),
            "first_maintainer_merge_at": None,
            "first_ready_to_merge_at": _dt(3),
            "is_merged": True,
            "is_closed": True,
            "seconds_open_to_first_touch": 7200.0,
            "seconds_first_touch_to_maintainer_merge": None,
            "seconds_maintainer_merge_to_ready_to_merge": None,
            "seconds_ready_to_merge_to_merged": 7200.0,
            "seconds_open_to_merged": 3 * 86400.0,
        },
        # closed unmerged, never touched
        {
            "pull_request_id": 3,
            "first_touch_at": None,
            "first_maintainer_merge_at": None,
            "first_ready_to_merge_at": None,
            "is_merged": False,
            "is_closed": True,
            "seconds_open_to_first_touch": None,
            "seconds_first_touch_to_maintainer_merge": None,
            "seconds_maintainer_merge_to_ready_to_merge": None,
            "seconds_ready_to_merge_to_merged": None,
            "seconds_open_to_merged": None,
        },
        # still open, touched
        {
            "pull_request_id": 4,
            "first_touch_at": _dt(2),
            "first_maintainer_merge_at": None,
            "first_ready_to_merge_at": None,
            "is_merged": False,
            "is_closed": False,
            "seconds_open_to_first_touch": 0.0,
            "seconds_first_touch_to_maintainer_merge": 600.0,
            "seconds_maintainer_merge_to_ready_to_merge": None,
            "seconds_ready_to_merge_to_merged": None,
            "seconds_open_to_merged": None,
        },
    ]
)


def test_milestone_summary_counts_and_shares() -> None:
    row = milestone_summary({"c": _PIPELINE}).row(0, named=True)
    assert row["cohort"] == "c"
    assert row["n_prs"] == 4
    assert (row["n_first_touch"], row["pct_first_touch"]) == (3, 0.75)
    assert (row["n_maintainer_merge"], row["pct_maintainer_merge"]) == (1, 0.25)
    assert (row["n_ready_to_merge"], row["pct_ready_to_merge"]) == (2, 0.5)
    assert (row["n_merged"], row["pct_merged"]) == (2, 0.5)
    assert (row["n_closed_unmerged"], row["pct_closed_unmerged"]) == (1, 0.25)
    assert (row["n_still_open"], row["pct_still_open"]) == (1, 0.25)


def test_milestone_summary_ttm_over_merged_only() -> None:
    row = milestone_summary({"c": _PIPELINE}).row(0, named=True)
    assert row["median_ttm_days"] == pytest.approx(2.0)
    assert row["p90_ttm_days"] == pytest.approx(2.8)


def test_milestone_summary_empty_cohort_keeps_schema() -> None:
    summary = milestone_summary({"empty": _PIPELINE.clear(), "full": _PIPELINE})
    assert summary.get_column("cohort").to_list() == ["empty", "full"]
    empty = summary.row(0, named=True)
    assert empty["n_prs"] == 0
    assert empty["n_merged"] == 0
    assert empty["pct_merged"] is None
    assert empty["median_ttm_days"] is None


def test_milestone_summary_row_order_follows_frames() -> None:
    summary = milestone_summary({"b": _PIPELINE, "a": _PIPELINE.clear()})
    assert summary.get_column("cohort").to_list() == ["b", "a"]


def test_stage_series_converts_to_days_and_drops_zero() -> None:
    days = stage_series(_PIPELINE, STAGE_OPEN_TO_FIRST_TOUCH)
    # PR 3 is null and PR 4 is exactly 0 (no place on a log axis).
    assert sorted(days.to_list()) == pytest.approx([1 / 24, 2 / 24])


def test_stage_series_applies_min_seconds_floor() -> None:
    stage = Stage(
        STAGE_FIRST_TOUCH_TO_MM.column,
        STAGE_FIRST_TOUCH_TO_MM.label,
        min_seconds=MAINTAINER_FIRST_SECONDS,
    )
    # PR 1's 10 s delta is the maintainer-first bot-latency floor; only PR 4's
    # 600 s survives.
    assert stage_series(_PIPELINE, stage).to_list() == pytest.approx([600 / 86400])


def test_stage_quantiles_shape_and_values() -> None:
    table = stage_quantiles(
        {"c": _PIPELINE},
        stages=(STAGE_OPEN_TO_FIRST_TOUCH,),
        quantiles=(0.5,),
    )
    assert table.columns == ["cohort", "stage", "n", "p50_days"]
    row = table.row(0, named=True)
    assert row["stage"] == STAGE_OPEN_TO_FIRST_TOUCH.label
    assert row["n"] == 2
    assert row["p50_days"] == pytest.approx(1.5 / 24)


def test_stage_quantiles_empty_cohort_is_null_not_missing() -> None:
    table = stage_quantiles(
        {"empty": _PIPELINE.clear()}, stages=(STAGE_OPEN_TO_FIRST_TOUCH,)
    )
    row = table.row(0, named=True)
    assert row["n"] == 0
    assert row["p50_days"] is None and row["p90_days"] is None


def test_stage_quantiles_covers_every_cohort_stage_pair() -> None:
    table = stage_quantiles({"a": _PIPELINE.clear(), "b": _PIPELINE.clear()})
    assert table.height == 2 * len(DEFAULT_STAGES)


def test_describe_lists_active_filters_only() -> None:
    spec = CohortSpec("c", start=date(2025, 1, 1), pr_types=("feat", "doc"))
    assert spec.describe() == "opened 2025-01-01 → …; types: feat, doc"
