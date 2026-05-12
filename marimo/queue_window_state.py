"""Theme 1 companion — queue-window view of the review state machine."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")

# Mirrors Theme 1 (`review_state_machine.py`) but reconstructs the
# "in reviewers' court" state from `analyzer_prqueuewindow` ruleset 3
# (`from_ts` / `to_ts`) instead of the retired `awaiting-review` label.
# Includes an overlap-cohort cell that compares the two state sources
# on PRs active during the label's lifetime (2022-11 → 2024-07).
#
# Run interactively:
#     uv run marimo edit marimo/queue_window_state.py
# Serve read-only:
#     uv run marimo run marimo/queue_window_state.py


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Queue-window state — companion to Theme 1

    Reconstructs per-PR "on queue" intervals from
    `analyzer_prqueuewindow` ruleset 3 (the analyzer's machine-defined
    successor to the retired `awaiting-review` label) and mirrors the
    Theme 1 plot deck: sojourn distribution, cycle count, stage-by-stage
    latency to merge, monthly rolling quantiles, and currently-stuck
    open windows.

    The last section overlays the two state sources on the cohort of
    PRs that were active while `awaiting-review` was in use
    (2022-11-01 → 2024-07-10), so the queue window's faithfulness as a
    successor can be inspected directly.
    """)
    return


@app.cell
def _():
    import sys
    from pathlib import Path

    _repo_root = Path(__file__).resolve().parents[1]
    if str(_repo_root) not in sys.path:
        sys.path.insert(0, str(_repo_root))

    from datetime import datetime, timezone

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    from qb_notebook.data_io import load_pr_interval_data, merged_prs_frame
    from qb_notebook.pr_shape import (
        DEFAULT_LINES_BREAKS,
        DEFAULT_PR_TYPES,
        bucket_labels,
        pr_type,
        size_buckets,
    )
    from qb_notebook.review_states import (
        label_intervals,
        label_overlap_seconds,
        queue_window_intervals,
        stage_timestamps,
    )

    return (
        DEFAULT_LINES_BREAKS,
        DEFAULT_PR_TYPES,
        Path,
        bucket_labels,
        datetime,
        label_intervals,
        label_overlap_seconds,
        load_pr_interval_data,
        merged_prs_frame,
        np,
        pl,
        plt,
        pr_type,
        queue_window_intervals,
        size_buckets,
        stage_timestamps,
        timezone,
    )


@app.cell
def _(Path, datetime, load_pr_interval_data, pl, timezone):
    _data_dir = Path(__file__).resolve().parents[1] / "data"
    data = load_pr_interval_data(_data_dir)
    prs = data["prs"]
    events = data["events"]
    queue_windows_all = data["queue_windows"]
    asof = datetime.now(tz=timezone.utc)
    # See `label_intervals` docstring: closes any `awaiting-review`
    # interval at the PR's close time, since GitHub does not auto-remove
    # labels on close.
    pr_close = prs.select(pl.col("id").alias("pull_request_id"), "closed_at")
    return asof, events, pr_close, prs, queue_windows_all


@app.cell
def _(mo, queue_windows_all):
    _rs_options = sorted(queue_windows_all["rule_set_id"].unique().to_list())
    rule_selector = mo.ui.dropdown(
        options=[str(r) for r in _rs_options],
        value="3",
        label="rule_set_id",
    )
    merged_only = mo.ui.checkbox(value=False, label="Merged to master only")
    mo.hstack([rule_selector, merged_only])
    return merged_only, rule_selector


@app.cell
def _(merged_prs_frame, pl, prs):
    merged_prs = merged_prs_frame(prs).select(
        [
            pl.col("id").alias("pull_request_id"),
            "gh_created_at",
            "closed_at",
            "merged_at",
            "merged_at_effective",
            "is_draft",
        ]
    )
    return (merged_prs,)


@app.cell
def _(
    asof,
    merged_only,
    merged_prs,
    queue_window_intervals,
    queue_windows_all,
    rule_selector,
):
    rule_id = int(rule_selector.value)
    intervals_all = queue_window_intervals(
        queue_windows_all, rule_set_id=rule_id, asof=asof
    )
    if merged_only.value:
        intervals = intervals_all.join(
            merged_prs.select("pull_request_id"),
            on="pull_request_id",
            how="inner",
        )
    else:
        intervals = intervals_all
    return intervals, rule_id


@app.cell
def _(intervals, mo, pl, rule_id):
    summary = intervals.select(
        [
            pl.lit(rule_id).alias("rule_set_id"),
            pl.len().alias("windows"),
            pl.col("pull_request_id").n_unique().alias("prs"),
            pl.col("is_open").sum().alias("currently_open"),
            pl.col("duration_days").median().alias("median_days"),
            pl.col("duration_days").quantile(0.75).alias("p75_days"),
            pl.col("duration_days").quantile(0.90).alias("p90_days"),
            pl.col("duration_days").quantile(0.99).alias("p99_days"),
        ]
    )
    mo.md("### Queue-window summary")
    summary
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1. Sojourn distribution per queue window

    Closed windows only (open ones distort the tail). Truncated at 60
    days for readability; the >60d tail is summarized below.
    """)
    return


@app.cell
def _(intervals, pl, plt):
    closed = intervals.filter(~pl.col("is_open"))
    _vals = closed.filter(pl.col("duration_days") <= 60)["duration_days"].to_numpy()
    _fig, _ax = plt.subplots(figsize=(9, 4))
    _ax.hist(_vals, bins=list(range(0, 61, 1)), color="#6aa3d8", alpha=0.85)
    _ax.set_xlabel("Queue window duration (days, closed, ≤60d)")
    _ax.set_ylabel("Number of windows")
    _ax.set_title(f"Queue-window sojourn (n={len(_vals)} of {closed.height} closed)")
    _fig.tight_layout()
    _fig
    return (closed,)


@app.cell
def _(closed, mo, pl):
    long_tail = closed.filter(pl.col("duration_days") > 60).select(
        [
            pl.len().alias("n_above_60d"),
            pl.col("duration_days").max().alias("max_days"),
            pl.col("duration_days").mean().alias("mean_days"),
        ]
    )
    mo.md("### Long-tail windows (>60 days)")
    long_tail
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. Cycle count per PR

    `cycle_index` increments each time a PR re-enters the queue (the
    analyzer's analogue of the `awaiting-review → awaiting-author →
    awaiting-review` ping-pong). Plot is the distribution of the
    maximum cycle index seen per PR.
    """)
    return


@app.cell
def _(intervals, pl, plt):
    per_pr_cycles = intervals.group_by("pull_request_id").agg(
        pl.col("cycle_index").max().alias("max_cycle")
    )
    _counts = (
        per_pr_cycles.group_by("max_cycle").agg(pl.len().alias("prs")).sort("max_cycle")
    )
    _fig, _ax = plt.subplots(figsize=(8, 3.5))
    _ax.bar(_counts["max_cycle"].to_numpy(), _counts["prs"].to_numpy(), color="#6aa3d8")
    _ax.set_xlabel("Max cycle_index per PR")
    _ax.set_ylabel("PR count")
    _ax.set_title("How many times does a PR re-enter the queue?")
    _fig.tight_layout()
    _fig
    return (per_pr_cycles,)


@app.cell
def _(merged_only, merged_prs, mo, per_pr_cycles, pl, plt):
    if merged_only.value:
        _pv = (
            per_pr_cycles.join(
                merged_prs.select(
                    ["pull_request_id", "gh_created_at", "merged_at_effective"]
                ),
                on="pull_request_id",
                how="inner",
            )
            .with_columns(
                (
                    (
                        pl.col("merged_at_effective") - pl.col("gh_created_at")
                    ).dt.total_seconds()
                    / 86400.0
                ).alias("time_to_merge_days")
            )
            .filter(pl.col("time_to_merge_days") > 0)
        )
        _fig, _ax = plt.subplots(figsize=(8, 4))
        _ax.scatter(
            _pv["max_cycle"].to_numpy(),
            _pv["time_to_merge_days"].to_numpy(),
            s=6,
            alpha=0.25,
        )
        _ax.set_yscale("log")
        _ax.set_xlabel("Max cycle_index")
        _ax.set_ylabel("Time to merge (days, log scale)")
        _ax.set_title("Queue cycles vs. time-to-merge (merged-to-master PRs)")
        _fig.tight_layout()
        out = _fig
    else:
        out = mo.callout(
            mo.md(
                "Enable **Merged to master only** above to see cycle count "
                "vs. time-to-merge."
            ),
            kind="info",
        )
    out
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Stage-by-stage cumulative latency (merged-to-master PRs)

    For each PR that landed on master: time from `gh_created_at` to
    `first_on_queue_ts` (first queue entry), then to first
    `maintainer-merge`, then to effective merge.
    """)
    return


@app.cell
def _(events, intervals, merged_prs, pl, stage_timestamps):
    first_queue = intervals.group_by("pull_request_id").agg(
        pl.col("first_on_queue_ts").min().alias("first_on_queue_ts")
    )
    _stages_labels = stage_timestamps(events, label_order=("maintainer-merge",))
    stages_merged = (
        merged_prs.select(
            [
                "pull_request_id",
                "gh_created_at",
                pl.col("merged_at_effective").alias("merged_at"),
            ]
        )
        .join(first_queue, on="pull_request_id", how="left")
        .join(_stages_labels, on="pull_request_id", how="left")
        .with_columns(
            [
                (
                    (
                        pl.col("first_on_queue_ts") - pl.col("gh_created_at")
                    ).dt.total_seconds()
                    / 86400.0
                ).alias("open_to_queue_days"),
                (
                    (
                        pl.col("first_maintainer_merge") - pl.col("first_on_queue_ts")
                    ).dt.total_seconds()
                    / 86400.0
                ).alias("queue_to_signoff_days"),
                (
                    (
                        pl.col("merged_at") - pl.col("first_maintainer_merge")
                    ).dt.total_seconds()
                    / 86400.0
                ).alias("signoff_to_merge_days"),
                (
                    (pl.col("merged_at") - pl.col("gh_created_at")).dt.total_seconds()
                    / 86400.0
                ).alias("total_days"),
            ]
        )
    )
    return (stages_merged,)


@app.cell
def _(np, plt, stages_merged):
    _stage_cols = [
        "open_to_queue_days",
        "queue_to_signoff_days",
        "signoff_to_merge_days",
    ]
    _stage_disp = ["open → queue", "queue → sign-off", "sign-off → merge"]
    _fig, _ax = plt.subplots(figsize=(8, 4))
    _arrays = []
    for _col in _stage_cols:
        _vals = stages_merged[_col].drop_nulls().to_numpy()
        _vals = _vals[(_vals >= 0) & np.isfinite(_vals)]
        _arrays.append(_vals)
    _bp = _ax.boxplot(
        _arrays, tick_labels=_stage_disp, showfliers=False, patch_artist=True
    )
    for _patch in _bp["boxes"]:
        _patch.set_facecolor("#6aa3d8")
    _ax.set_ylabel("Days")
    _ax.set_title("Per-stage latency for merged PRs (boxplot, outliers hidden)")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo, pl, stages_merged):
    stage_medians = stages_merged.select(
        [
            pl.col("open_to_queue_days").median().alias("open→queue (median d)"),
            pl.col("queue_to_signoff_days").median().alias("queue→sign-off (median d)"),
            pl.col("signoff_to_merge_days").median().alias("sign-off→merge (median d)"),
            pl.col("total_days").median().alias("total (median d)"),
        ]
    )
    mo.md("### Stage medians")
    stage_medians
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4. Monthly rolling sojourn quantiles

    Each closed window is attributed to the month its `to_ts` falls in;
    p50/p75/p90 of `duration_days` are taken per month.
    """)
    return


@app.cell
def _(intervals, mo, pl, plt):
    _rolling_input = intervals.filter(~pl.col("is_open"))
    if _rolling_input.height == 0:
        _out = mo.md("_No closed windows to plot._")
    else:
        _rolling = (
            _rolling_input.with_columns(pl.col("end").dt.truncate("1mo").alias("month"))
            .group_by("month")
            .agg(
                [
                    pl.col("duration_days").quantile(0.5).alias("p50"),
                    pl.col("duration_days").quantile(0.75).alias("p75"),
                    pl.col("duration_days").quantile(0.9).alias("p90"),
                    pl.len().alias("n"),
                ]
            )
            .filter(pl.col("n") >= 20)
            .sort("month")
        )
        _fig, _ax = plt.subplots(figsize=(10, 4))
        _months = _rolling["month"].to_numpy()
        for _q, _color in [("p50", "#3a6"), ("p75", "#d80"), ("p90", "#c33")]:
            _ax.plot(_months, _rolling[_q].to_numpy(), label=_q, color=_color)
        _ax.set_title("Queue-window sojourn — monthly quantiles")
        _ax.set_ylabel("days")
        _ax.legend(loc="upper right", fontsize=9)
        _fig.tight_layout()
        _out = _fig
    _out
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. Currently-stuck open windows

    Open windows (still on queue at `asof`) whose `duration_days`
    exceeds the p90 of historical closed sojourns.
    """)
    return


@app.cell
def _(intervals, pl):
    _p90 = (
        intervals.filter(~pl.col("is_open"))
        .select(pl.col("duration_days").quantile(0.9).alias("p90_days"))
        .item()
    )
    stuck = (
        intervals.filter(pl.col("is_open"))
        .filter(pl.col("duration_days") > _p90)
        .select(
            [
                "pull_request_id",
                "cycle_index",
                "start",
                "duration_days",
                pl.lit(_p90).alias("p90_days"),
                "opened_by_event_type",
            ]
        )
        .sort("duration_days", descending=True)
    )
    stuck
    return


@app.cell
def _(mo):
    mo.md("""
    ## 6. Overlap with `awaiting-review` (2022-11-01 → 2024-07-10)

    On the cohort of PRs that saw both an `awaiting-review` label
    interval and a queue window within the label's lifetime, compare
    per-PR seconds attributed to each. The queue window is a faithful
    successor to the extent the two agree.

    Jaccard = intersection-seconds / union-seconds. Values near 1.0 mean
    the two intervals cover almost the same calendar time.
    """)
    return


@app.cell
def _(
    asof,
    datetime,
    events,
    intervals_all,
    label_intervals,
    pl,
    pr_close,
    timezone,
):
    """Clamp both source streams to [cohort_start, cohort_end] symmetrically.

    Bug fixes (relative to the original notebook):
    1. Previously only `end_effective` was clamped (to cohort_end).
       Intervals that started before cohort_start contributed their full
       pre-cohort time, inflating per-PR totals and pulling Jaccard down.
       ~93 `awaiting-review` intervals had this shape; we now clamp
       `start` to cohort_start symmetrically.
    2. `label_intervals` now takes `df_pr_close` so open `awaiting-review`
       intervals on closed PRs end at the PR's close, not at `asof`.
       GitHub does not auto-remove labels when a PR closes, so without
       this clamp ~531 phantom open intervals run to `asof`.
    """
    cohort_start = datetime(2022, 11, 1, tzinfo=timezone.utc)
    cohort_end = datetime(2024, 7, 10, 23, 59, 59, tzinfo=timezone.utc)

    def _clamp_to_cohort(df):
        return (
            df.filter(pl.col("start") <= cohort_end)
            .filter(pl.col("end_effective") >= cohort_start)
            .with_columns(
                [
                    pl.max_horizontal(pl.col("start"), pl.lit(cohort_start)).alias(
                        "start"
                    ),
                    pl.min_horizontal(
                        pl.col("end_effective"), pl.lit(cohort_end)
                    ).alias("end_effective"),
                ]
            )
            .filter(pl.col("end_effective") > pl.col("start"))
        )

    label_ints = _clamp_to_cohort(
        label_intervals(events, "awaiting-review", asof=asof, df_pr_close=pr_close)
    )
    queue_ints = _clamp_to_cohort(intervals_all)
    return cohort_end, cohort_start, label_ints, queue_ints


@app.cell
def _(label_ints, pl, queue_ints):
    label_secs = (
        label_ints.with_columns(
            (pl.col("end_effective") - pl.col("start"))
            .dt.total_seconds()
            .cast(pl.Float64)
            .alias("secs")
        )
        .group_by("pull_request_id")
        .agg(pl.col("secs").sum().alias("label_seconds"))
    )
    queue_secs = (
        queue_ints.with_columns(
            (pl.col("end_effective") - pl.col("start"))
            .dt.total_seconds()
            .cast(pl.Float64)
            .alias("secs")
        )
        .group_by("pull_request_id")
        .agg(pl.col("secs").sum().alias("queue_seconds"))
    )

    cohort_prs = label_secs.join(queue_secs, on="pull_request_id", how="inner")
    return cohort_prs, label_secs, queue_secs


@app.cell
def _(cohort_prs, label_ints, label_overlap_seconds, pl, queue_ints):
    _cohort_keys = cohort_prs.select("pull_request_id")
    queue_windows_per_pr = queue_ints.join(
        _cohort_keys, on="pull_request_id", how="inner"
    ).select(
        [
            "pull_request_id",
            pl.col("start").alias("window_start"),
            pl.col("end_effective").alias("window_end"),
        ]
    )

    overlap_per_window = label_overlap_seconds(
        label_ints.join(_cohort_keys, on="pull_request_id", how="inner"),
        queue_windows_per_pr,
    )
    intersection_secs = overlap_per_window.group_by("pull_request_id").agg(
        pl.col("overlap_seconds").sum().alias("intersection_seconds")
    )

    cohort = (
        cohort_prs.join(intersection_secs, on="pull_request_id", how="left")
        .with_columns(pl.col("intersection_seconds").fill_null(0.0))
        .with_columns(
            [
                (
                    pl.col("label_seconds")
                    + pl.col("queue_seconds")
                    - pl.col("intersection_seconds")
                ).alias("union_seconds"),
                (pl.col("label_seconds") / 86400.0).alias("label_days"),
                (pl.col("queue_seconds") / 86400.0).alias("queue_days"),
            ]
        )
        .with_columns(
            pl.when(pl.col("union_seconds") > 0)
            .then(pl.col("intersection_seconds") / pl.col("union_seconds"))
            .otherwise(None)
            .alias("jaccard")
        )
    )
    return (cohort,)


@app.cell
def _(cohort, mo, pl):
    cohort_summary = cohort.select(
        [
            pl.len().alias("prs"),
            pl.col("jaccard").median().alias("median_jaccard"),
            pl.col("jaccard").quantile(0.25).alias("p25_jaccard"),
            pl.col("jaccard").quantile(0.75).alias("p75_jaccard"),
            (pl.col("jaccard") > 0.5).sum().alias("prs_jaccard_gt_0p5"),
            (pl.col("jaccard") > 0.8).sum().alias("prs_jaccard_gt_0p8"),
        ]
    )
    mo.md("### Cohort-level overlap summary")
    cohort_summary
    return


@app.cell
def _(cohort, np, plt):
    _x = cohort["label_days"].to_numpy()
    _y = cohort["queue_days"].to_numpy()
    _fig, _ax = plt.subplots(figsize=(6.5, 6.5))
    _ax.scatter(_x, _y, s=8, alpha=0.25, color="#6aa3d8")
    _lim = float(np.nanmax([_x.max() if _x.size else 1, _y.max() if _y.size else 1]))
    _lim = max(_lim, 1.0)
    _ax.plot([0, _lim], [0, _lim], color="#888", lw=1, ls="--", label="y = x")
    _ax.set_xlim(0, _lim)
    _ax.set_ylim(0, _lim)
    _ax.set_xlabel("`awaiting-review` total days per PR")
    _ax.set_ylabel("queue-open total days per PR")
    _ax.set_title("Per-PR coverage: label vs. queue window")
    _ax.legend(loc="lower right", fontsize=9)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(cohort, plt):
    _vals = cohort["jaccard"].drop_nulls().to_numpy()
    _fig, _ax = plt.subplots(figsize=(8, 3.5))
    _ax.hist(_vals, bins=20, color="#6aa3d8", alpha=0.85, range=(0, 1))
    _ax.set_xlabel("Jaccard (intersection / union of seconds)")
    _ax.set_ylabel("PR count")
    _ax.set_title(
        f"Per-PR Jaccard between `awaiting-review` and queue window (n={len(_vals)})"
    )
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 7. Sojourn × PR shape (Session 8)

    Queue-window duration faceted by PR size (`lines_bucket`) and
    conventional-commit type (`pr_type`). Shape attributes are joined
    onto the filtered `intervals` frame — they inherit the
    `rule_set_id` / `merged_only` toggles above.
    """)
    return


@app.cell
def _(intervals, pl, pr_type, prs, size_buckets):
    """Join PR-shape attributes onto the queue-window intervals."""
    _shape = pr_type(size_buckets(prs)).select(
        pl.col("id").alias("pull_request_id"),
        "lines_bucket",
        "pr_type",
    )
    intervals_shape = intervals.join(_shape, on="pull_request_id", how="left")
    return (intervals_shape,)


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, intervals_shape, np, pl, plt):
    """Queue-window sojourn boxplot by `lines_bucket`. Closed windows only;
    y-clipped at 30d to match the corresponding view in
    `review_state_machine.py`."""
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    _closed = intervals_shape.filter(~pl.col("is_open"))
    _data = [
        _closed.filter(pl.col("lines_bucket") == _b)["duration_days"].to_numpy()
        for _b in _bucket_order
    ]
    _fig, _ax = plt.subplots(figsize=(9, 4))
    _ax.boxplot(
        [_d[_d <= 30] for _d in _data],
        tick_labels=_bucket_order,
        showfliers=False,
        widths=0.6,
    )
    _ax.set_ylabel("Queue-window duration (days, ≤30d)")
    _ax.set_xlabel("Lines changed bucket")
    _ax.set_title("Sojourn by PR size")
    _ax.grid(axis="y", alpha=0.3)
    for _i, _d in enumerate(_data):
        _ax.text(
            _i + 1, _ax.get_ylim()[1] * 0.92, f"n={len(_d)}", ha="center", fontsize=8
        )
    _ = np  # silence unused-import lint
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, intervals_shape, mo, pl):
    """Per-bucket sojourn summary table."""
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    sojourn_by_lines = (
        intervals_shape.filter(
            ~pl.col("is_open") & pl.col("lines_bucket").is_not_null()
        )
        .group_by("lines_bucket")
        .agg(
            [
                pl.len().alias("windows"),
                pl.col("duration_days").median().alias("median_d"),
                pl.col("duration_days").quantile(0.75).alias("p75_d"),
                pl.col("duration_days").quantile(0.90).alias("p90_d"),
            ]
        )
        .with_columns(pl.col("lines_bucket").cast(pl.Enum(_bucket_order)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    mo.md("### Queue-window sojourn quantiles by lines_bucket")
    sojourn_by_lines
    return


@app.cell
def _(DEFAULT_PR_TYPES, intervals_shape, pl, plt):
    """Queue-window sojourn boxplot by `pr_type` (canonical types only)."""
    _types = list(DEFAULT_PR_TYPES)
    _closed = intervals_shape.filter(~pl.col("is_open"))
    _data = [
        _closed.filter(pl.col("pr_type") == _t)["duration_days"].to_numpy()
        for _t in _types
    ]
    _fig, _ax = plt.subplots(figsize=(10, 4))
    _ax.boxplot(
        [_d[_d <= 30] for _d in _data],
        tick_labels=_types,
        showfliers=False,
        widths=0.6,
    )
    _ax.set_ylabel("Queue-window duration (days, ≤30d)")
    _ax.set_xlabel("PR type")
    _ax.set_title("Sojourn by PR type")
    _ax.grid(axis="y", alpha=0.3)
    for _i, _d in enumerate(_data):
        _ax.text(
            _i + 1, _ax.get_ylim()[1] * 0.92, f"n={len(_d)}", ha="center", fontsize=8
        )
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 8. Cycle count × PR shape (Session 8)

    Maximum `cycle_index` per PR (one bigger than the Section 2 metric;
    `0` = single visit) split by shape. Unlike the label-based ping-pong
    view in `review_state_machine.py`, queue-window coverage runs the
    full project history (2021-05 → present) so the cohort is much
    larger.
    """)
    return


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, intervals_shape, mo, pl):
    """Per-PR `max_cycle` aggregated, joined to shape attributes for tables
    below. Shape columns flow through from `intervals_shape` and are
    constant per PR, so a `group_by(pull_request_id, ...)` works."""
    cycles_shape = intervals_shape.group_by(
        ["pull_request_id", "lines_bucket", "pr_type"]
    ).agg(pl.col("cycle_index").max().alias("max_cycle"))
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    cycles_by_lines = (
        cycles_shape.filter(pl.col("lines_bucket").is_not_null())
        .group_by("lines_bucket")
        .agg(
            [
                pl.len().alias("prs"),
                pl.col("max_cycle").median().alias("median_cycles"),
                pl.col("max_cycle").quantile(0.75).alias("p75_cycles"),
                pl.col("max_cycle").max().alias("max_cycles"),
                (pl.col("max_cycle") > 0).mean().alias("share_multi_cycle"),
            ]
        )
        .with_columns(pl.col("lines_bucket").cast(pl.Enum(_bucket_order)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    mo.md("### Queue cycle count by lines_bucket")
    cycles_by_lines
    return (cycles_shape,)


@app.cell
def _(DEFAULT_PR_TYPES, cycles_shape, mo, pl):
    """Queue cycle count summary per `pr_type`."""
    _types = list(DEFAULT_PR_TYPES) + ["other", "unparsed"]
    cycles_by_type = (
        cycles_shape.filter(pl.col("pr_type").is_not_null())
        .group_by("pr_type")
        .agg(
            [
                pl.len().alias("prs"),
                pl.col("max_cycle").median().alias("median_cycles"),
                pl.col("max_cycle").quantile(0.75).alias("p75_cycles"),
                pl.col("max_cycle").max().alias("max_cycles"),
                (pl.col("max_cycle") > 0).mean().alias("share_multi_cycle"),
            ]
        )
        .with_columns(pl.col("pr_type").cast(pl.Enum(_types)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    mo.md("### Queue cycle count by pr_type")
    cycles_by_type
    return


if __name__ == "__main__":
    app.run()
