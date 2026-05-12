"""Theme 1 — Review state machine: sojourn times & ping-pong.

Run interactively:
    uv run marimo edit marimo/review_state_machine.py

Serve read-only:
    uv run marimo run marimo/review_state_machine.py
"""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Review state machine — sojourn times & ping-pong

    Reconstructs `awaiting-review`, `awaiting-author`, `WIP`, and
    `maintainer-merge` intervals from `LABELED`/`UNLABELED` events and
    shows how long PRs spend in each state, how often they bounce, and
    the stage-by-stage merge timeline.

    **Note on `awaiting-review`.** This label was retired around
    2024-07-10 and removed from `syncer_labeldef`; the
    "in reviewers' court" state on current PRs is implicit (PR open +
    not `awaiting-author` / `WIP`), with `analyzer_prqueuewindow`
    ruleset 3 as the closest modern proxy. The plots below still
    reconstruct historical `awaiting-review` intervals (2021-08 →
    2024-07) — useful for retrospectives, but PRs created after the
    cutover contribute zero `awaiting-review` time. See the
    cross-cutting TODO in `docs/review-analysis-plan.md` for the
    unified-intervals helper that would close this gap.
    """)
    return


@app.cell
def _():
    import sys
    from pathlib import Path

    # Repo isn't installed as a package; make `qb_notebook` importable
    # regardless of where marimo was launched from.
    _repo_root = Path(__file__).resolve().parents[1]
    if str(_repo_root) not in sys.path:
        sys.path.insert(0, str(_repo_root))

    from datetime import datetime, timezone

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    from qb_notebook.data_io import load_pr_interval_data
    from qb_notebook.filters import expr_merged_at_effective, expr_merged_to_master
    from qb_notebook.review_states import label_intervals, stage_timestamps

    return (
        Path,
        datetime,
        expr_merged_at_effective,
        expr_merged_to_master,
        label_intervals,
        load_pr_interval_data,
        np,
        pl,
        plt,
        stage_timestamps,
        timezone,
    )


@app.cell
def _(Path, datetime, load_pr_interval_data, timezone):
    _data_dir = Path(__file__).resolve().parents[1] / "data"
    data = load_pr_interval_data(_data_dir)
    prs = data["prs"]
    events = data["events"]
    asof = datetime.now(tz=timezone.utc)
    return asof, events, prs


@app.cell
def _(mo):
    state_labels = ("awaiting-review", "awaiting-author", "WIP", "maintainer-merge")
    label_selector = mo.ui.multiselect(
        options=list(state_labels),
        value=list(state_labels),
        label="Labels to include",
    )
    merged_only = mo.ui.checkbox(value=False, label="Merged to master only")
    exclude_drafts = mo.ui.checkbox(value=False, label="Exclude draft PRs")
    mo.hstack([label_selector, merged_only, exclude_drafts])
    return exclude_drafts, label_selector, merged_only, state_labels


@app.cell
def _(expr_merged_at_effective, expr_merged_to_master, pl, prs):
    """Bors-aware merged-to-master view of the PR table.

    `prs.merged_at` is null for the vast majority of mathlib merges (bors
    closes PRs after pushing to master rather than using GitHub's merge
    flow), so we build a small derived frame that exposes the right
    boolean + timestamp once and reuse it everywhere downstream.
    """
    merged_prs = (
        prs.filter(expr_merged_to_master())
        .with_columns(expr_merged_at_effective().alias("merged_at_effective"))
        .select(
            [
                pl.col("id").alias("pull_request_id"),
                "gh_created_at",
                "closed_at",
                "merged_at",
                "merged_at_effective",
                "is_draft",
            ]
        )
    )
    return (merged_prs,)


@app.cell
def _(asof, events, label_intervals, label_selector, state_labels):
    labels_chosen = label_selector.value or list(state_labels)
    intervals_all = label_intervals(events, labels_chosen, asof=asof)
    return intervals_all, labels_chosen


@app.cell
def _(exclude_drafts, intervals_all, merged_only, merged_prs, pl, prs):
    _pr_meta = prs.select(
        [
            pl.col("id").alias("pull_request_id"),
            "is_draft",
        ]
    )
    _df = intervals_all.join(_pr_meta, on="pull_request_id", how="left")
    if merged_only.value:
        _df = _df.join(
            merged_prs.select("pull_request_id"),
            on="pull_request_id",
            how="inner",
        )
    if exclude_drafts.value:
        _df = _df.filter(~pl.col("is_draft").fill_null(False))
    intervals = _df
    return (intervals,)


@app.cell
def _(intervals, mo, pl):
    summary = (
        intervals.group_by("label_name")
        .agg(
            [
                pl.len().alias("intervals"),
                pl.col("pull_request_id").n_unique().alias("prs"),
                pl.col("is_open").sum().alias("currently_open"),
                pl.col("duration_days").median().alias("median_days"),
                pl.col("duration_days").quantile(0.75).alias("p75_days"),
                pl.col("duration_days").quantile(0.90).alias("p90_days"),
                pl.col("duration_days").quantile(0.99).alias("p99_days"),
            ]
        )
        .sort("intervals", descending=True)
    )
    mo.md("### Per-label interval summary")
    summary
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1. Sojourn-time distributions

    Closed intervals only (open ones distort the tail). Truncated at 60 days
    for readability — the long tail past 60d is shown in a side table.
    """)
    return


@app.cell
def _(intervals, labels_chosen, pl, plt):
    closed = intervals.filter(~pl.col("is_open"))
    _fig, _ax = plt.subplots(figsize=(9, 4.5))
    _bins = list(range(0, 61, 1))
    for _label in labels_chosen:
        _vals = closed.filter(
            (pl.col("label_name") == _label) & (pl.col("duration_days") <= 60)
        )["duration_days"].to_numpy()
        if len(_vals):
            _ax.hist(_vals, bins=_bins, alpha=0.45, label=f"{_label} (n={len(_vals)})")
    _ax.set_xlabel("Sojourn (days, closed intervals, ≤60d)")
    _ax.set_ylabel("Number of intervals")
    _ax.legend()
    _ax.set_title("Time spent per state per interval")
    _fig.tight_layout()
    _fig
    return (closed,)


@app.cell
def _(closed, mo, pl):
    long_tail = (
        closed.filter(pl.col("duration_days") > 60)
        .group_by("label_name")
        .agg(
            [
                pl.len().alias("n_above_60d"),
                pl.col("duration_days").max().alias("max_days"),
            ]
        )
        .sort("n_above_60d", descending=True)
    )
    mo.md("### Long-tail intervals (>60 days)")
    long_tail
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. Ping-pong: how many awaiting-review cycles per PR?
    """)
    return


@app.cell
def _(intervals, pl, plt):
    ping = (
        intervals.filter(pl.col("label_name") == "awaiting-review")
        .group_by("pull_request_id")
        .agg(pl.len().alias("review_cycles"))
    )
    _counts = (
        ping.group_by("review_cycles").agg(pl.len().alias("prs")).sort("review_cycles")
    )
    _fig, _ax = plt.subplots(figsize=(8, 3.5))
    _ax.bar(_counts["review_cycles"].to_numpy(), _counts["prs"].to_numpy())
    _ax.set_xlabel("awaiting-review cycles per PR")
    _ax.set_ylabel("PR count")
    _ax.set_title("How often does a PR get re-reviewed?")
    _fig.tight_layout()
    _fig
    return (ping,)


@app.cell
def _(merged_only, merged_prs, mo, ping, pl, plt):
    if merged_only.value:
        _pv = ping.join(
            merged_prs.select(
                ["pull_request_id", "gh_created_at", "merged_at_effective"]
            ),
            on="pull_request_id",
            how="inner",
        )
        _pv = _pv.with_columns(
            (
                (
                    pl.col("merged_at_effective") - pl.col("gh_created_at")
                ).dt.total_seconds()
                / 86400.0
            ).alias("time_to_merge_days")
        ).filter(pl.col("time_to_merge_days") > 0)
        _fig, _ax = plt.subplots(figsize=(8, 4))
        _ax.scatter(
            _pv["review_cycles"].to_numpy(),
            _pv["time_to_merge_days"].to_numpy(),
            s=6,
            alpha=0.25,
        )
        _ax.set_yscale("log")
        _ax.set_xlabel("awaiting-review cycles")
        _ax.set_ylabel("Time to merge (days, log scale)")
        _ax.set_title("Ping-pong vs. time-to-merge (merged-to-master PRs)")
        _fig.tight_layout()
        out = _fig
    else:
        out = mo.callout(
            mo.md(
                "Enable **Merged to master only** above to see ping-pong vs. "
                "time-to-merge."
            ),
            kind="info",
        )
    out
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Stage-by-stage cumulative latency (merged-to-master PRs)

    For each PR that landed on master (bors-merged via `[Merged by Bors]`
    title rewrite, or GitHub-merged directly): time from `gh_created_at`
    to first `awaiting-review`, then to first `maintainer-merge`, then to
    effective merge (`merged_at` if set, else `closed_at`).
    """)
    return


@app.cell
def _(events, merged_prs, pl, stage_timestamps):
    stages = stage_timestamps(events)
    _base = merged_prs.select(
        [
            "pull_request_id",
            "gh_created_at",
            pl.col("merged_at_effective").alias("merged_at"),
        ]
    )
    stages_merged = _base.join(stages, on="pull_request_id", how="left").with_columns(
        [
            (
                (
                    pl.col("first_awaiting_review") - pl.col("gh_created_at")
                ).dt.total_seconds()
                / 86400.0
            ).alias("open_to_review_days"),
            (
                (
                    pl.col("first_maintainer_merge") - pl.col("first_awaiting_review")
                ).dt.total_seconds()
                / 86400.0
            ).alias("review_to_signoff_days"),
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
    return (stages_merged,)


@app.cell
def _(np, plt, stages_merged):
    _stage_cols = [
        "open_to_review_days",
        "review_to_signoff_days",
        "signoff_to_merge_days",
    ]
    _stage_disp = ["open → review", "review → sign-off", "sign-off → merge"]
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
            pl.col("open_to_review_days").median().alias("open→review (median d)"),
            pl.col("review_to_signoff_days")
            .median()
            .alias("review→sign-off (median d)"),
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

    For each closed interval, attribute it to the month its end falls in,
    then take p50/p75/p90 of `duration_days` per month per label.
    """)
    return


@app.cell
def _(intervals, mo, pl, plt):
    _rolling_input = intervals.filter(~pl.col("is_open"))
    if _rolling_input.height == 0:
        _out = mo.md("_No closed intervals to plot._")
    else:
        _rolling = (
            _rolling_input.with_columns(pl.col("end").dt.truncate("1mo").alias("month"))
            .group_by(["month", "label_name"])
            .agg(
                [
                    pl.col("duration_days").quantile(0.5).alias("p50"),
                    pl.col("duration_days").quantile(0.75).alias("p75"),
                    pl.col("duration_days").quantile(0.9).alias("p90"),
                    pl.len().alias("n"),
                ]
            )
            .filter(pl.col("n") >= 20)
            .sort(["label_name", "month"])
        )
        _n_labels = _rolling["label_name"].n_unique()
        _fig, _axes = plt.subplots(
            nrows=max(_n_labels, 1),
            sharex=True,
            figsize=(10, 2.6 * max(_n_labels, 1)),
            squeeze=False,
        )
        for _ax, _label in zip(
            _axes.flat, sorted(_rolling["label_name"].unique().to_list())
        ):
            _sub = _rolling.filter(pl.col("label_name") == _label).sort("month")
            _months = _sub["month"].to_numpy()
            for _q, _color in [("p50", "#3a6"), ("p75", "#d80"), ("p90", "#c33")]:
                _ax.plot(_months, _sub[_q].to_numpy(), label=_q, color=_color)
            _ax.set_title(f"{_label} — monthly sojourn quantiles")
            _ax.set_ylabel("days")
            _ax.legend(loc="upper right", fontsize=8)
        _fig.tight_layout()
        _out = _fig
    _out
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. "Stuck" open intervals

    Open intervals (still applied at `asof`) whose `duration_days` exceeds
    the p90 of historical closed sojourns for that label.
    """)
    return


@app.cell
def _(intervals, pl):
    _closed_p90 = (
        intervals.filter(~pl.col("is_open"))
        .group_by("label_name")
        .agg(pl.col("duration_days").quantile(0.9).alias("p90_days"))
    )
    stuck = (
        intervals.filter(pl.col("is_open"))
        .join(_closed_p90, on="label_name", how="left")
        .filter(pl.col("duration_days") > pl.col("p90_days"))
        .select(
            [
                "pull_request_id",
                "label_name",
                "start",
                "duration_days",
                "p90_days",
                "applied_by",
            ]
        )
        .sort("duration_days", descending=True)
    )
    stuck
    return


if __name__ == "__main__":
    app.run()
