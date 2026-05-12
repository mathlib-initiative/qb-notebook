"""Theme 3 — Bottleneck localization."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")

# Run interactively:  uv run marimo edit marimo/bottleneck_localization.py
# Serve read-only:    uv run marimo run  marimo/bottleneck_localization.py
#
# Where does the "approved but not merged" tail come from? Once a PR
# gets `maintainer-merge`, the remaining latency is split between bors
# queue cycles, CI failures, merge conflicts, and reviewers pushing it
# back to the author. This notebook quantifies that breakdown for
# mathlib4 PRs that landed on master.


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Bottleneck localization — approved-to-merge latency

    Per-PR window: first `maintainer-merge` → effective merge. Within
    that window we flag overlap with `merge-conflict`, `awaiting-CI`, and
    `awaiting-author` intervals, and join in the per-PR bors queue
    cycle count from `analyzer_prqueuewindow`. The goal is to see which
    of those signals correlate with the long tail.
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
        labels_active_at,
    )

    return (
        DEFAULT_LINES_BREAKS,
        DEFAULT_PR_TYPES,
        Path,
        bucket_labels,
        datetime,
        label_intervals,
        label_overlap_seconds,
        labels_active_at,
        load_pr_interval_data,
        merged_prs_frame,
        np,
        pl,
        plt,
        pr_type,
        size_buckets,
        timezone,
    )


@app.cell
def _(Path, datetime, load_pr_interval_data, pl, timezone):
    _data_dir = Path(__file__).resolve().parents[1] / "data"
    data = load_pr_interval_data(_data_dir)
    prs = data["prs"]
    events = data["events"]
    queue_windows = data["queue_windows"]
    label_defs = data["label_defs"]
    asof = datetime.now(tz=timezone.utc)
    # Threaded into every `label_intervals` call below. GitHub does not
    # auto-remove labels when a PR is closed (bors-merged or otherwise);
    # without this clamp, e.g. `maintainer-merge` reports ~2964 phantom
    # open intervals on this artifact, only 32 of which are actually on
    # PRs that are still open.
    pr_close = prs.select(pl.col("id").alias("pull_request_id"), "closed_at")
    return asof, events, label_defs, pr_close, prs, queue_windows


@app.cell
def _(merged_prs_frame, pl, prs):
    """Bors-aware merged-to-master view: id, gh_created_at, merged_at_effective."""
    merged_prs = merged_prs_frame(prs).select(
        [
            pl.col("id").alias("pull_request_id"),
            "gh_created_at",
            "merged_at_effective",
        ]
    )
    return (merged_prs,)


@app.cell
def _(asof, events, label_intervals, merged_prs, pl, pr_close):
    # Approval intervals = `maintainer-merge` LABELED intervals. We take
    # the first application per PR as the start of the approved window;
    # the end is the effective merge (set later by joining merged_prs).
    mm = label_intervals(events, "maintainer-merge", asof=asof, df_pr_close=pr_close)
    first_mm = (
        mm.group_by("pull_request_id")
        .agg(pl.col("start").min().alias("first_mm_at"))
        .join(merged_prs, on="pull_request_id", how="inner")
        .with_columns(
            (
                (
                    pl.col("merged_at_effective") - pl.col("first_mm_at")
                ).dt.total_seconds()
                / 86400.0
            ).alias("mm_to_merge_days")
        )
        .filter(pl.col("mm_to_merge_days") >= 0)
    )
    return first_mm, mm


@app.cell
def _(first_mm, mo, pl):
    _summary = first_mm.select(
        [
            pl.len().alias("merged PRs with maintainer-merge"),
            pl.col("mm_to_merge_days").median().alias("median mm→merge (d)"),
            pl.col("mm_to_merge_days").quantile(0.75).alias("p75 (d)"),
            pl.col("mm_to_merge_days").quantile(0.9).alias("p90 (d)"),
            pl.col("mm_to_merge_days").quantile(0.99).alias("p99 (d)"),
        ]
    )
    mo.md("### Approved-to-merge cohort summary")
    _summary
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1. Overlap flags during the approved window

    For each PR, ask: while sitting in the approved-but-not-yet-merged
    window, did the PR overlap with `merge-conflict`, `awaiting-CI`, or
    `awaiting-author` label intervals? `ready-to-merge` is included as
    the "made it into the bors queue at least once" signal.
    """)
    return


@app.cell
def _(asof, events, first_mm, label_intervals, label_overlap_seconds, pl, pr_close):
    _windows = first_mm.select(
        [
            "pull_request_id",
            pl.col("first_mm_at").alias("window_start"),
            pl.col("merged_at_effective").alias("window_end"),
        ]
    )
    overlap = _windows
    for _label in (
        "ready-to-merge",
        "merge-conflict",
        "awaiting-CI",
        "awaiting-author",
    ):
        _ivals = label_intervals(events, _label, asof=asof, df_pr_close=pr_close)
        overlap = label_overlap_seconds(
            _ivals,
            overlap,
            overlap_col=f"sec_{_label.replace('-', '_')}",
            had_overlap_col=f"had_{_label.replace('-', '_')}",
        )
    flags = first_mm.join(
        overlap.select(
            [
                "pull_request_id",
                "had_ready_to_merge",
                "had_merge_conflict",
                "had_awaiting_CI",
                "had_awaiting_author",
                "sec_awaiting_CI",
                "sec_merge_conflict",
                "sec_awaiting_author",
            ]
        ),
        on="pull_request_id",
        how="left",
    )
    return (flags,)


@app.cell
def _(flags, mo, pl):
    flag_counts = (
        flags.select(
            [
                pl.len().alias("PRs"),
                pl.col("had_ready_to_merge").sum().alias("ever in bors queue"),
                pl.col("had_merge_conflict").sum().alias("hit merge-conflict"),
                pl.col("had_awaiting_CI").sum().alias("hit awaiting-CI"),
                pl.col("had_awaiting_author").sum().alias("bounced to author"),
            ]
        )
        .unpivot(variable_name="signal", value_name="count")
        .with_columns(
            (pl.col("count") / pl.col("count").first() * 100).alias("share_pct")
        )
    )
    mo.md("### Signal prevalence (out of approved-and-merged PRs)")
    flag_counts
    return


@app.cell
def _(flags, mo, pl):
    by_signal = (
        flags.unpivot(
            on=[
                "had_ready_to_merge",
                "had_merge_conflict",
                "had_awaiting_CI",
                "had_awaiting_author",
            ],
            index=["pull_request_id", "mm_to_merge_days"],
            variable_name="signal",
            value_name="had_signal",
        )
        .group_by(["signal", "had_signal"])
        .agg(
            [
                pl.len().alias("n"),
                pl.col("mm_to_merge_days").median().alias("median_days"),
                pl.col("mm_to_merge_days").quantile(0.75).alias("p75_days"),
                pl.col("mm_to_merge_days").quantile(0.9).alias("p90_days"),
            ]
        )
        .sort(["signal", "had_signal"])
    )
    mo.md(
        "### Conditional mm→merge latency by signal\n"
        "Compare the `had_signal = true` rows to their `false` counterparts "
        "to read off how much each stall adds to the tail."
    )
    by_signal
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. Approved-to-merge latency distribution

    Histogram of `first_maintainer_merge → merged_at_effective`. Capped
    at 14 days for readability; longer tails summarized below.
    """)
    return


@app.cell
def _(first_mm, np, pl, plt):
    _vals = first_mm.filter(pl.col("mm_to_merge_days") <= 14)[
        "mm_to_merge_days"
    ].to_numpy()
    _fig, _ax = plt.subplots(figsize=(9, 4))
    _ax.hist(_vals, bins=np.linspace(0, 14, 57), color="#6aa3d8", edgecolor="white")
    _ax.set_xlabel("Days from first maintainer-merge to effective merge")
    _ax.set_ylabel("PRs")
    _ax.set_title(
        f"Approved → merged latency  (≤14d, n={len(_vals)} of {first_mm.height})"
    )
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(first_mm, mo, pl):
    long_tail = first_mm.filter(pl.col("mm_to_merge_days") > 14).select(
        [
            pl.len().alias("PRs > 14d"),
            pl.col("mm_to_merge_days").median().alias("median (d)"),
            pl.col("mm_to_merge_days").max().alias("max (d)"),
        ]
    )
    mo.md("### Long-tail approved-to-merge (>14d)")
    long_tail
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Bors queue bounce rate

    For each approved-and-merged PR, how many times did it enter and
    exit the bors queue? `cycle_index` on `analyzer_prqueuewindow` is
    0-indexed and per PR; total cycles = `max(cycle_index) + 1`.
    Restricted to ruleset 3 (the mathlib4 main queue).
    """)
    return


@app.cell
def _(first_mm, pl, plt, queue_windows):
    bounces = (
        queue_windows.filter(pl.col("rule_set_id") == 3)
        .group_by("pull_request_id")
        .agg((pl.col("cycle_index").max() + 1).alias("queue_cycles"))
    )
    cohort_bounces = first_mm.join(
        bounces, on="pull_request_id", how="left"
    ).with_columns(pl.col("queue_cycles").fill_null(0))
    _counts = (
        cohort_bounces.group_by("queue_cycles")
        .agg(pl.len().alias("prs"))
        .sort("queue_cycles")
    )
    _fig, _ax = plt.subplots(figsize=(8, 3.5))
    _x = _counts["queue_cycles"].to_numpy()
    _ax.bar(_x, _counts["prs"].to_numpy(), color="#888")
    _ax.set_xlabel("Queue cycles per PR (0 = never queued)")
    _ax.set_ylabel("PRs")
    _ax.set_title("Bors queue bounce rate for approved-and-merged PRs")
    _ax.set_xticks(_x[_x <= 12])
    _fig.tight_layout()
    _fig
    return (cohort_bounces,)


@app.cell
def _(cohort_bounces, mo, pl):
    bounce_summary = (
        cohort_bounces.group_by("queue_cycles")
        .agg(
            [
                pl.len().alias("prs"),
                pl.col("mm_to_merge_days").median().alias("median (d)"),
                pl.col("mm_to_merge_days").quantile(0.9).alias("p90 (d)"),
            ]
        )
        .sort("queue_cycles")
    )
    mo.md("### Latency conditional on queue cycles")
    bounce_summary
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4. Queue close reasons over time

    `closed_by_event_type` on `analyzer_prqueuewindow` records what
    kicked a PR off the queue. Tracking the monthly mix tells us
    whether failures shifted (e.g. more CI-induced bounces, fewer
    forbidden-label removals).
    """)
    return


@app.cell
def _(np, pl, plt, queue_windows):
    close_reasons = (
        queue_windows.filter(pl.col("rule_set_id") == 3)
        .filter(pl.col("closed_by_event_type").is_not_null())
        .with_columns(pl.col("to_ts").dt.truncate("1mo").alias("month"))
        .drop_nulls(["month"])
        .group_by(["month", "closed_by_event_type"])
        .agg(pl.len().alias("n"))
        .sort(["month", "closed_by_event_type"])
    )
    _wide = (
        close_reasons.pivot(
            on="closed_by_event_type",
            index="month",
            values="n",
            aggregate_function="sum",
        )
        .fill_null(0)
        .sort("month")
    )
    _reason_cols = [c for c in _wide.columns if c != "month"]
    _totals = _wide.select([pl.col(c).sum().alias(c) for c in _reason_cols]).row(0)
    _ordered = [c for _, c in sorted(zip(_totals, _reason_cols), reverse=True)]
    _months = _wide["month"].to_numpy()
    _row_totals = np.zeros(len(_months), dtype=float)
    for _c in _ordered:
        _row_totals = _row_totals + _wide[_c].to_numpy()
    _row_totals[_row_totals == 0] = 1.0
    _fig, _ax = plt.subplots(figsize=(10, 4))
    _bottom = np.zeros(len(_months))
    _colors = plt.colormaps.get_cmap("tab10")
    for _i, _c in enumerate(_ordered):
        _share = _wide[_c].to_numpy() / _row_totals
        _ax.fill_between(
            _months,
            _bottom,
            _bottom + _share,
            label=_c,
            color=_colors(_i),
            alpha=0.85,
        )
        _bottom = _bottom + _share
    _ax.set_ylim(0, 1)
    _ax.set_ylabel("Share of queue closures")
    _ax.set_title("Queue close reasons over time (ruleset 3, monthly share)")
    _ax.legend(loc="upper left", fontsize=8, ncol=2)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. Currently-stuck PRs

    Open PRs that have an active `maintainer-merge` interval at `asof`:
    they've been approved but haven't merged yet. We list them with
    age since the approval and any stall labels currently applied.
    """)
    return


@app.cell
def _(asof, events, label_intervals, mm, pl, pr_close, prs):
    _open_prs = prs.filter(pl.col("state") == "open").select(
        [pl.col("id").alias("pull_request_id"), "number", "title", "head_ci_state"]
    )
    open_mm = (
        mm.filter(pl.col("is_open"))
        .group_by("pull_request_id")
        .agg(pl.col("start").min().alias("approved_at"))
        .join(_open_prs, on="pull_request_id", how="inner")
        .with_columns(
            ((pl.lit(asof) - pl.col("approved_at")).dt.total_seconds() / 86400.0).alias(
                "days_since_approval"
            )
        )
    )

    # Which stall labels are currently active on each of those PRs?
    _stalls = ("merge-conflict", "awaiting-CI", "awaiting-author", "ready-to-merge")
    active_stalls = (
        label_intervals(events, list(_stalls), asof=asof, df_pr_close=pr_close)
        .filter(pl.col("is_open"))
        .group_by("pull_request_id")
        .agg(pl.col("label_name").unique().sort().alias("active_labels"))
    )
    stuck = (
        open_mm.join(active_stalls, on="pull_request_id", how="left")
        .with_columns(
            pl.col("active_labels").list.join(", ").fill_null("").alias("active_labels")
        )
        .select(
            [
                "number",
                "title",
                "days_since_approval",
                "head_ci_state",
                "active_labels",
            ]
        )
        .sort("days_since_approval", descending=True)
    )
    stuck
    return (stuck,)


@app.cell
def _(mo, pl, stuck):
    _summary = stuck.select(
        [
            pl.len().alias("open + approved PRs"),
            pl.col("days_since_approval").median().alias("median age (d)"),
            pl.col("days_since_approval").quantile(0.9).alias("p90 age (d)"),
            pl.col("days_since_approval").max().alias("max age (d)"),
        ]
    )
    mo.md("### Stuck-PR summary")
    _summary
    return


@app.cell
def _(mo):
    mo.md("""
    ## 6. CI stalls

    `awaiting-CI` sojourn distribution across all closed intervals,
    plus a snapshot of `head_ci_state` for currently-open PRs.
    """)
    return


@app.cell
def _(asof, events, label_intervals, pl, plt, pr_close):
    _ci = label_intervals(
        events, "awaiting-CI", asof=asof, df_pr_close=pr_close
    ).filter(~pl.col("is_open"))
    _vals = _ci.filter(pl.col("duration_hours") <= 72)["duration_hours"].to_numpy()
    _fig, _ax = plt.subplots(figsize=(9, 3.8))
    _ax.hist(_vals, bins=36, color="#c69", edgecolor="white")
    _ax.set_xlabel("awaiting-CI sojourn (hours, closed intervals, ≤72h)")
    _ax.set_ylabel("Intervals")
    _ax.set_title(f"awaiting-CI sojourn distribution  (n={len(_vals)} of {_ci.height})")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo, pl, prs):
    ci_snapshot = (
        prs.filter(pl.col("state") == "open")
        .group_by("head_ci_state")
        .agg(pl.len().alias("open_prs"))
        .sort("open_prs", descending=True)
    )
    mo.md("### head_ci_state snapshot (open PRs)")
    ci_snapshot
    return


@app.cell
def _(mo):
    mo.md("""
    ## 7. Stall signals × PR shape (Session 8)

    The Section 1 stall flags (`had_merge_conflict`, `had_awaiting_CI`,
    `had_awaiting_author`, `had_ready_to_merge`) split by `lines_bucket`
    and `pr_type`. A 1000+-line `feat:` likely has very different
    stall-prevalence than a 1-line `chore:`; this surfaces that.
    """)
    return


@app.cell
def _(flags, pl, pr_type, prs, size_buckets):
    """Decorate the `flags` frame with PR-shape attributes once. Joined
    cohort = approved-and-merged PRs from Section 1, plus per-PR
    `lines_bucket` / `pr_type` from the shape helpers."""
    _shape = pr_type(size_buckets(prs)).select(
        pl.col("id").alias("pull_request_id"),
        "lines_bucket",
        "pr_type",
    )
    flags_shape = flags.join(_shape, on="pull_request_id", how="left")
    return (flags_shape,)


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, flags_shape, mo, pl):
    """Stall-signal prevalence per `lines_bucket`. Each cell is the share
    of PRs in the bucket that hit the signal at any point inside the
    approved window."""
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    signal_by_lines = (
        flags_shape.filter(pl.col("lines_bucket").is_not_null())
        .group_by("lines_bucket")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("had_ready_to_merge").mean().alias("share_ever_queued"),
                pl.col("had_merge_conflict").mean().alias("share_merge_conflict"),
                pl.col("had_awaiting_CI").mean().alias("share_awaiting_CI"),
                pl.col("had_awaiting_author").mean().alias("share_awaiting_author"),
                pl.col("mm_to_merge_days").median().alias("median_mm_merge_d"),
                pl.col("mm_to_merge_days").quantile(0.9).alias("p90_mm_merge_d"),
            ]
        )
        .with_columns(pl.col("lines_bucket").cast(pl.Enum(_bucket_order)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    mo.md("### Stall signals & latency by lines_bucket")
    signal_by_lines
    return


@app.cell
def _(DEFAULT_PR_TYPES, flags_shape, mo, pl):
    """Same table by `pr_type`. Includes `other` / `unparsed` so callers
    can see whether non-canonical titles correlate with more stalling."""
    _types = list(DEFAULT_PR_TYPES) + ["other", "unparsed"]
    signal_by_type = (
        flags_shape.filter(pl.col("pr_type").is_not_null())
        .group_by("pr_type")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("had_ready_to_merge").mean().alias("share_ever_queued"),
                pl.col("had_merge_conflict").mean().alias("share_merge_conflict"),
                pl.col("had_awaiting_CI").mean().alias("share_awaiting_CI"),
                pl.col("had_awaiting_author").mean().alias("share_awaiting_author"),
                pl.col("mm_to_merge_days").median().alias("median_mm_merge_d"),
                pl.col("mm_to_merge_days").quantile(0.9).alias("p90_mm_merge_d"),
            ]
        )
        .with_columns(pl.col("pr_type").cast(pl.Enum(_types)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    mo.md("### Stall signals & latency by pr_type")
    signal_by_type
    return


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, flags_shape, np, pl, plt):
    """Approved-to-merge latency boxplot by `lines_bucket`. Y-clipped at
    14d to mirror the Section 2 histogram."""
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    _data = [
        flags_shape.filter(pl.col("lines_bucket") == _b)["mm_to_merge_days"].to_numpy()
        for _b in _bucket_order
    ]
    _fig, _ax = plt.subplots(figsize=(9, 4))
    _ax.boxplot(
        [_d[_d <= 14] for _d in _data],
        tick_labels=_bucket_order,
        showfliers=False,
        widths=0.6,
    )
    _ax.set_ylabel("mm → merge (days, ≤14d shown)")
    _ax.set_xlabel("Lines changed bucket")
    _ax.set_title("Approved-to-merge latency by PR size")
    _ax.grid(axis="y", alpha=0.3)
    for _i, _d in enumerate(_data):
        _ax.text(
            _i + 1, _ax.get_ylim()[1] * 0.92, f"n={len(_d)}", ha="center", fontsize=8
        )
    _ = np
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 8. Stall signals × topic area (Session 8)

    For each approved-and-merged PR, attribute it to whatever `t-*`
    area(s) were active at `first_mm_at` via
    `labels_active_at(t_intervals, first_mm_pts)`. PRs with multiple
    active `t-*` labels contribute to each — same count-each semantics
    as Theme 4. Areas with `n < 20` are dropped from the headline cuts
    to avoid small-N artifacts.
    """)
    return


@app.cell
def _(asof, events, label_defs, label_intervals, pl, pr_close):
    """Topic-area label intervals. `t-*` labels are reconstructed the
    same way as in `area_health.py` so the cohort definition stays
    consistent across themes."""
    _t_names = (
        label_defs.filter(pl.col("name").str.starts_with("t-"))["name"].sort().to_list()
    )
    t_intervals = label_intervals(events, _t_names, asof=asof, df_pr_close=pr_close)
    return (t_intervals,)


@app.cell
def _(first_mm, labels_active_at, pl, t_intervals):
    """Attribute the approved-window start to active topic areas. One row
    per (PR, area) — PRs without a `t-*` label active at `first_mm_at`
    drop out."""
    first_mm_pts = first_mm.select(
        "pull_request_id",
        pl.col("first_mm_at").alias("at"),
    )
    mm_by_area = labels_active_at(t_intervals, first_mm_pts).rename(
        {"label_name": "area"}
    )
    return (mm_by_area,)


@app.cell
def _(flags_shape, mm_by_area, pl):
    """Per-(PR, area) row with the stall flags + latency carried through."""
    flags_area = mm_by_area.join(
        flags_shape.select(
            "pull_request_id",
            "mm_to_merge_days",
            "had_ready_to_merge",
            "had_merge_conflict",
            "had_awaiting_CI",
            "had_awaiting_author",
        ),
        on="pull_request_id",
        how="inner",
    )
    return (flags_area,)


@app.cell
def _(flags_area, mo, pl):
    """Prevalence + latency by area. Sorted by approved-PR volume so the
    busiest areas are at the top. Restricted to areas with n >= 20."""
    signal_by_area = (
        flags_area.group_by("area")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("had_ready_to_merge").mean().alias("share_ever_queued"),
                pl.col("had_merge_conflict").mean().alias("share_merge_conflict"),
                pl.col("had_awaiting_CI").mean().alias("share_awaiting_CI"),
                pl.col("had_awaiting_author").mean().alias("share_awaiting_author"),
                pl.col("mm_to_merge_days").median().alias("median_mm_merge_d"),
                pl.col("mm_to_merge_days").quantile(0.9).alias("p90_mm_merge_d"),
            ]
        )
        .filter(pl.col("n") >= 20)
        .sort("n", descending=True)
    )
    mo.md("### Stall signals & latency by topic area (n ≥ 20)")
    signal_by_area
    return


if __name__ == "__main__":
    app.run()
