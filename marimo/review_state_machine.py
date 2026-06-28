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
async def _(mo):
    # WASM/Pyodide bootstrap. In the browser (`sys.platform == "emscripten"`)
    # qb_notebook is not on the path and the raw `data/` dir doesn't exist, so
    # we micropip-install the packaged wheel + slimmed-data loader deps here,
    # before any `qb_notebook` import runs. Threading `is_wasm` into the
    # downstream import/data cells enforces that ordering via marimo's DAG.
    # No-op under a normal local kernel (`uv run marimo edit ...`). Locals are
    # `_`-prefixed so only `is_wasm` enters the cross-cell namespace.
    import sys as _sys

    is_wasm = _sys.platform == "emscripten"
    if is_wasm:
        import micropip as _micropip

        # Patch stdlib urllib onto the browser fetch API so wasm_io can pull
        # the parquet files over HTTP.
        _ = await _micropip.install("pyodide-http")
        import pyodide_http as _pyodide_http

        _ = _pyodide_http.patch_all()

        # qb_notebook's eager __init__ transitively imports these. Install
        # them explicitly so the wheel installs with deps=False (its metadata
        # still lists the full dev set, incl. kaleido, which has no Pyodide
        # build). pyarrow is needed because marimo patches pl.read_parquet to
        # route through it in WASM (qb_notebook.wasm_io reads the slimmed
        # parquet). pyyaml backs qb_notebook.teams.
        _ = await _micropip.install(
            ["polars", "pandas", "numpy", "pyarrow", "matplotlib", "scipy", "pyyaml"]
        )
        _wheel = (
            mo.notebook_location() / "public" / "qb_notebook-0.1.0-py3-none-any.whl"
        )
        _ = await _micropip.install(str(_wheel), deps=False)

        # Patch library API gaps vs. Pyodide's older builds (e.g. matplotlib
        # boxplot tick_labels). No-op on new-enough libraries.
        from qb_notebook.wasm_io import apply_wasm_compat_shims as _apply_shims

        _apply_shims()
    return (is_wasm,)


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
def _(is_wasm):
    import sys
    from pathlib import Path

    if not is_wasm:
        # `__file__` is undefined in the WASM runtime; only needed to find the
        # repo root for the local kernel (where qb_notebook lives on disk).
        _repo_root = Path(__file__).resolve().parents[1]
        if str(_repo_root) not in sys.path:
            sys.path.insert(0, str(_repo_root))

    from datetime import datetime, timezone

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    from qb_notebook.data_io import load_pr_interval_data, merged_prs_frame
    from qb_notebook.filters import expr_is_draft
    from qb_notebook.pr_shape import (
        DEFAULT_LINES_BREAKS,
        DEFAULT_PR_TYPES,
        bucket_labels,
        pr_type,
        size_buckets,
    )
    from qb_notebook.review_states import (
        MATHLIB_LABEL_RETIRED_AT,
        attribute_label_events,
        label_intervals,
        stage_timestamps,
    )
    from qb_notebook.teams import load as load_teams
    from qb_notebook.wasm_io import load_slimmed_data, load_teams_snapshot

    return (
        DEFAULT_LINES_BREAKS,
        DEFAULT_PR_TYPES,
        MATHLIB_LABEL_RETIRED_AT,
        Path,
        attribute_label_events,
        bucket_labels,
        datetime,
        expr_is_draft,
        label_intervals,
        load_pr_interval_data,
        load_slimmed_data,
        load_teams,
        load_teams_snapshot,
        merged_prs_frame,
        np,
        pl,
        plt,
        pr_type,
        size_buckets,
        stage_timestamps,
        timezone,
    )


@app.cell
def _(
    Path,
    datetime,
    is_wasm,
    load_pr_interval_data,
    load_slimmed_data,
    mo,
    pl,
    timezone,
):
    # core_user (mapped author_id -> github_login in a later cell) ships as a
    # slimmed table under public/ in WASM (see scripts/export_wasm_data.py);
    # locally it's read directly off disk (not a load_pr_interval_data key).
    if is_wasm:
        data = load_slimmed_data(str(mo.notebook_location() / "public"))
        core_user_raw = data["core_user"]
    else:
        _data_dir = Path(__file__).resolve().parents[1] / "data"
        data = load_pr_interval_data(_data_dir)
        core_user_raw = pl.read_parquet(_data_dir / "core_user.parquet")
    prs = data["prs"]
    events = data["events"]
    asof = datetime.now(tz=timezone.utc)
    return asof, core_user_raw, events, prs


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
def _(merged_prs_frame, pl, prs):
    """Bors-aware merged-to-master view of the PR table.

    `prs.merged_at` is null for the vast majority of mathlib merges (bors
    closes PRs after pushing to master rather than using GitHub's merge
    flow), so we build a small derived frame that exposes the right
    boolean + timestamp once and reuse it everywhere downstream.
    """
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
    MATHLIB_LABEL_RETIRED_AT,
    asof,
    events,
    label_intervals,
    label_selector,
    pl,
    prs,
    state_labels,
):
    """Reconstruct label intervals with two end-of-interval clamps:

    - `df_pr_close` closes any open interval at the PR's `closed_at` —
      GitHub does **not** auto-remove labels on PR close, so labels
      that were never explicitly removed before bors merged the PR
      would otherwise show up as phantom open intervals running to
      `asof`. This affects every label, not just retired ones.
    - `label_asof_overrides` is the retirement-date safety net for
      labels that were deleted from the repo (currently
      `awaiting-review`, retired 2024-07-10). Combined with the close
      clamp, the override only matters for the handful of PRs that
      are still open and still carry the retired label.
    """
    labels_chosen = label_selector.value or list(state_labels)
    intervals_all = label_intervals(
        events,
        labels_chosen,
        asof=asof,
        label_asof_overrides=MATHLIB_LABEL_RETIRED_AT,
        df_pr_close=prs.select(pl.col("id").alias("pull_request_id"), "closed_at"),
    )
    return intervals_all, labels_chosen


@app.cell
def _(exclude_drafts, expr_is_draft, intervals_all, merged_only, merged_prs, pl, prs):
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
        # `is_draft` arrives as a Postgres "t"/"f" string; `expr_is_draft`
        # default targets that schema. Treat null (PR not in join) as
        # non-draft so the row is kept.
        _df = _df.filter(expr_is_draft(is_draft=False) | pl.col("is_draft").is_null())
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


@app.cell
def _(mo):
    mo.md("""
    ## 6. Sojourn × PR shape (Session 8)

    Per-state sojourn distributions split by PR size and conventional-
    commit type. Honors the same UI toggles (label selector, merged-only,
    drafts) as the sections above. Shape attributes come from
    `qb_notebook.pr_shape` (the same helpers Theme 5 uses) and are joined
    onto the filtered `intervals` frame so the two views stay in lockstep.
    """)
    return


@app.cell
def _(intervals, pl, pr_type, prs, size_buckets):
    """Decorate the filtered `intervals` frame with per-PR shape attributes.

    `lines_bucket` and `pr_type` are PR-level — they don't vary across
    intervals on the same PR — so a left-join on `pull_request_id`
    suffices. Unmatched PRs (e.g. older PRs missing additions/deletions)
    get null bucket / type.
    """
    _shape = pr_type(size_buckets(prs)).select(
        pl.col("id").alias("pull_request_id"),
        "lines_bucket",
        "pr_type",
    )
    intervals_shape = intervals.join(_shape, on="pull_request_id", how="left")
    return (intervals_shape,)


@app.cell
def _(
    DEFAULT_LINES_BREAKS,
    bucket_labels,
    intervals_shape,
    labels_chosen,
    pl,
    plt,
):
    """Sojourn boxplot by `lines_bucket`, one panel per state label.
    Closed intervals only, y-axis clipped at 30d for legibility."""
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    _closed = intervals_shape.filter(~pl.col("is_open"))
    _n = max(len(labels_chosen), 1)
    _fig, _axes = plt.subplots(
        nrows=_n, sharex=True, figsize=(9, 2.6 * _n), squeeze=False
    )
    for _ax, _lbl in zip(_axes.flat, labels_chosen):
        _data = [
            _closed.filter(
                (pl.col("label_name") == _lbl) & (pl.col("lines_bucket") == _b)
            )["duration_days"].to_numpy()
            for _b in _bucket_order
        ]
        _ax.boxplot(
            [_d[_d <= 30] for _d in _data],
            tick_labels=_bucket_order,
            showfliers=False,
            widths=0.6,
        )
        _ax.set_title(f"{_lbl}")
        _ax.set_ylabel("days (≤30d)")
        _ax.grid(axis="y", alpha=0.3)
        for _i, _d in enumerate(_data):
            _ax.text(
                _i + 1,
                _ax.get_ylim()[1] * 0.92,
                f"n={len(_d)}",
                ha="center",
                fontsize=7,
            )
    _axes.flat[-1].set_xlabel("Lines changed bucket")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, intervals_shape, mo, pl):
    """Per-(label, lines_bucket) sojourn summary table — median / p75 / p90
    plus interval count, ordered by bucket then label."""
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    sojourn_by_lines = (
        intervals_shape.filter(
            ~pl.col("is_open") & pl.col("lines_bucket").is_not_null()
        )
        .group_by(["label_name", "lines_bucket"])
        .agg(
            [
                pl.len().alias("n"),
                pl.col("duration_days").median().alias("median_d"),
                pl.col("duration_days").quantile(0.75).alias("p75_d"),
                pl.col("duration_days").quantile(0.90).alias("p90_d"),
            ]
        )
        .with_columns(pl.col("lines_bucket").cast(pl.Enum(_bucket_order)).alias("_o"))
        .sort(["label_name", "_o"])
        .drop("_o")
    )
    mo.md("### Sojourn quantiles per (state, lines_bucket)")
    sojourn_by_lines
    return


@app.cell
def _(DEFAULT_PR_TYPES, intervals_shape, labels_chosen, pl, plt):
    """Sojourn boxplot by `pr_type`, one panel per state label.

    Restricted to the 9 canonical conventional-commit types — `other` and
    `unparsed` are dropped to keep the axis legible. Same y-clip as the
    size view."""
    _types = list(DEFAULT_PR_TYPES)
    _closed = intervals_shape.filter(~pl.col("is_open"))
    _n = max(len(labels_chosen), 1)
    _fig, _axes = plt.subplots(
        nrows=_n, sharex=True, figsize=(10, 2.6 * _n), squeeze=False
    )
    for _ax, _lbl in zip(_axes.flat, labels_chosen):
        _data = [
            _closed.filter((pl.col("label_name") == _lbl) & (pl.col("pr_type") == _t))[
                "duration_days"
            ].to_numpy()
            for _t in _types
        ]
        _ax.boxplot(
            [_d[_d <= 30] for _d in _data],
            tick_labels=_types,
            showfliers=False,
            widths=0.6,
        )
        _ax.set_title(f"{_lbl}")
        _ax.set_ylabel("days (≤30d)")
        _ax.grid(axis="y", alpha=0.3)
        for _i, _d in enumerate(_data):
            _ax.text(
                _i + 1,
                _ax.get_ylim()[1] * 0.92,
                f"n={len(_d)}",
                ha="center",
                fontsize=7,
            )
    _axes.flat[-1].set_xlabel("PR type (conventional-commit prefix)")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 7. Ping-pong × PR shape (Session 8)

    The Section 2 ping-pong count (`awaiting-review` cycles per PR) split
    by `lines_bucket` and `pr_type`. Coverage is bounded by the
    `awaiting-review` label lifetime (retired 2024-07-10), so this
    section is informative for the 2021-08 → 2024-07 cohort only.
    """)
    return


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, intervals_shape, mo, pl):
    """Per-PR `awaiting-review` cycle count, joined back to shape attrs."""
    review_cycles_shape = (
        intervals_shape.filter(pl.col("label_name") == "awaiting-review")
        .group_by(["pull_request_id", "lines_bucket", "pr_type"])
        .agg(pl.len().alias("review_cycles"))
    )
    _bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    ping_by_lines = (
        review_cycles_shape.filter(pl.col("lines_bucket").is_not_null())
        .group_by("lines_bucket")
        .agg(
            [
                pl.len().alias("prs"),
                pl.col("review_cycles").median().alias("median_cycles"),
                pl.col("review_cycles").quantile(0.75).alias("p75_cycles"),
                pl.col("review_cycles").max().alias("max_cycles"),
                (pl.col("review_cycles") > 1).mean().alias("share_multi_cycle"),
            ]
        )
        .with_columns(pl.col("lines_bucket").cast(pl.Enum(_bucket_order)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    mo.md("### Ping-pong cycles by lines_bucket")
    ping_by_lines
    return (review_cycles_shape,)


@app.cell
def _(DEFAULT_PR_TYPES, mo, pl, review_cycles_shape):
    """Ping-pong cycle summary per canonical `pr_type`."""
    _types = list(DEFAULT_PR_TYPES) + ["other", "unparsed"]
    ping_by_type = (
        review_cycles_shape.filter(pl.col("pr_type").is_not_null())
        .group_by("pr_type")
        .agg(
            [
                pl.len().alias("prs"),
                pl.col("review_cycles").median().alias("median_cycles"),
                pl.col("review_cycles").quantile(0.75).alias("p75_cycles"),
                pl.col("review_cycles").max().alias("max_cycles"),
                (pl.col("review_cycles") > 1).mean().alias("share_multi_cycle"),
            ]
        )
        .with_columns(pl.col("pr_type").cast(pl.Enum(_types)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    mo.md("### Ping-pong cycles by pr_type")
    ping_by_type
    return


@app.cell
def _(mo):
    mo.md("""
    ## 8. Delegated-merge path (Session 13)

    `delegated` is the alternative sign-off label: a maintainer grants the
    PR's author permission to push the bors merge themselves. Pre-2024
    (`maintainer-merge` was introduced 2024-02-15) it was the *only*
    workflow label for "ready to merge"; post-cutover it runs as a
    parallel pathway alongside `maintainer-merge`. ~99 % of `delegated`
    LABELED events are applied by bots in response to a `bors
    delegate=author` command, so we use `attribute_label_events` to
    credit the maintainer who actually issued the delegation.
    """)
    return


@app.cell
def _(Path, is_wasm, load_teams, load_teams_snapshot, mo):
    """Teams overlay so the top-delegators table can be annotated. In WASM it
    loads the teams.json snapshot shipped under public/ (see
    scripts/build_wasm_site.write_teams_snapshot); locally it reads the sibling
    leanprover-community.github.io checkout, falling through gracefully if
    that checkout is missing."""
    if is_wasm:
        teams = load_teams_snapshot(str(mo.notebook_location() / "public"))
        teams_status = mo.md("_Loaded team snapshot._")
    else:
        _candidate = (
            Path(__file__).resolve().parents[2] / "leanprover-community.github.io"
        )
        if _candidate.exists():
            teams = load_teams(_candidate, warn_on_unmatched=False)
            teams_status = mo.md(f"_Loaded teams from `{_candidate}`._")
        else:
            teams = None
            teams_status = mo.callout(
                mo.md(
                    f"Sibling checkout `{_candidate}` not found — "
                    "team-membership annotations disabled."
                ),
                kind="warn",
            )
    teams_status
    return (teams,)


@app.cell
def _(datetime, events, pl, timezone):
    """First-LABELED timestamps for delegated and maintainer-merge, per PR."""
    _deleg_first = (
        events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == "delegated")
        )
        .group_by("pull_request_id")
        .agg(pl.col("occurred_at").min().alias("first_deleg_at"))
    )
    _mm_first = (
        events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == "maintainer-merge")
        )
        .group_by("pull_request_id")
        .agg(pl.col("occurred_at").min().alias("first_mm_at"))
    )
    signoff_first = _deleg_first.join(
        _mm_first, on="pull_request_id", how="full", coalesce=True
    )
    # 2024-02-15: `maintainer-merge` label introduced. Pre-cutover the only
    # sign-off label available was `delegated`.
    mm_cutover = datetime(2024, 2, 15, tzinfo=timezone.utc)
    return mm_cutover, signoff_first


@app.cell
def _(mm_cutover, mo, pl, signoff_first):
    """Cohort × era table: which sign-off path(s) did each PR take?"""
    _classed = signoff_first.with_columns(
        pl.min_horizontal("first_deleg_at", "first_mm_at").alias("first_signoff_at"),
        pl.when(
            pl.col("first_deleg_at").is_not_null() & pl.col("first_mm_at").is_not_null()
        )
        .then(pl.lit("both"))
        .when(pl.col("first_deleg_at").is_not_null())
        .then(pl.lit("delegated_only"))
        .otherwise(pl.lit("mm_only"))
        .alias("path"),
    ).with_columns(
        pl.when(pl.col("first_signoff_at") < mm_cutover)
        .then(pl.lit("pre_mm"))
        .otherwise(pl.lit("post_mm"))
        .alias("era"),
    )
    cohort_table = (
        _classed.group_by(["era", "path"])
        .len()
        .pivot(on="path", index="era", values="len", aggregate_function="first")
        .fill_null(0)
        .sort("era")
    )
    mo.md("### Cohort by era × path")
    cohort_table
    return


@app.cell
def _(events, mm_cutover, pl, plt):
    """Monthly trend of `delegated` vs `maintainer-merge` LABELED events.

    Marker line at the `maintainer-merge` cutover so the two-track
    workflow is visible at a glance.
    """
    _monthly = (
        events.filter(
            (pl.col("type") == "LABELED")
            & (pl.col("label_name").is_in(["delegated", "maintainer-merge"]))
        )
        .with_columns(pl.col("occurred_at").dt.truncate("1mo").alias("month"))
        .group_by(["month", "label_name"])
        .len()
        .pivot(on="label_name", index="month", values="len", aggregate_function="first")
        .fill_null(0)
        .sort("month")
    )
    _fig, _ax = plt.subplots(figsize=(11, 4))
    _ax.plot(
        _monthly["month"].to_numpy(),
        _monthly["delegated"].to_numpy(),
        color="#6aa3d8",
        label="delegated",
    )
    _ax.plot(
        _monthly["month"].to_numpy(),
        _monthly["maintainer-merge"].to_numpy(),
        color="#c63",
        label="maintainer-merge",
    )
    _ax.axvline(mm_cutover, color="#888", linestyle="--", linewidth=1)
    _ax.text(
        mm_cutover,
        _ax.get_ylim()[1] * 0.95,
        " mm-label introduced",
        fontsize=8,
        color="#666",
        verticalalignment="top",
    )
    _ax.set_xlabel("Month")
    _ax.set_ylabel("LABELED events")
    _ax.set_title("Monthly LABELED volume — delegated vs maintainer-merge")
    _ax.legend()
    _ax.grid(True, alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(attribute_label_events, events, mo, np, pl):
    """Attribution coverage for the `delegated` label. The default bot list
    now includes the bors-family accounts (mathlib-bors / bors /
    leanprover-radar), so the inferred-actor column points at the
    maintainer who issued `bors delegate=...`."""
    deleg_attr = attribute_label_events(events, "delegated")
    _n = deleg_attr.height
    _na = deleg_attr.filter(pl.col("attributed")).height
    _gaps = deleg_attr.filter(pl.col("attributed"))["gap_seconds"].to_numpy()
    deleg_coverage = pl.DataFrame(
        [
            {
                "events": _n,
                "attributed": _na,
                "pct_attributed": round(100 * _na / max(_n, 1), 1),
                "median_gap_s": int(np.median(_gaps)) if _gaps.size else 0,
                "p90_gap_s": int(np.percentile(_gaps, 90)) if _gaps.size else 0,
            }
        ]
    )
    mo.md("### Attribution coverage — `delegated`")
    deleg_coverage
    return (deleg_attr,)


@app.cell
def _(deleg_attr, pl, teams):
    """Top delegators by attributed `delegated` LABELED events."""
    _per = (
        deleg_attr.filter(pl.col("attributed"))
        .group_by("inferred_actor")
        .agg(
            pl.len().alias("triggers"),
            pl.col("pull_request_id").n_unique().alias("distinct_prs"),
        )
        .sort("triggers", descending=True)
        .rename({"inferred_actor": "actor"})
    )
    if teams is not None:
        _maint = teams.maintainers
        _rev = teams.reviewers
        _per = _per.with_columns(
            _per["actor"]
            .map_elements(
                lambda a: "maintainer"
                if a.lower() in _maint
                else ("reviewer" if a.lower() in _rev else "other"),
                return_dtype=str,
            )
            .alias("team")
        )
    top_delegators = _per.head(15)
    return (top_delegators,)


@app.cell
def _(mo, top_delegators):
    mo.md("### Top 15 delegators")
    top_delegators
    return


@app.cell
def _(merged_prs, mm_cutover, pl, signoff_first):
    """Per-PR sign-off → merge latency for each path. Merged-to-master only
    (so bors's effective merge timestamp is in scope), and we restrict to
    post-cutover sign-offs for the head-to-head comparison so both paths
    have the same denominator era."""
    _merged_slim = merged_prs.select("pull_request_id", "merged_at_effective")
    _joined = signoff_first.join(_merged_slim, on="pull_request_id", how="inner")
    deleg_lat = _joined.filter(
        pl.col("first_deleg_at").is_not_null()
        & pl.col("merged_at_effective").is_not_null()
        & (pl.col("merged_at_effective") >= pl.col("first_deleg_at"))
    ).with_columns(
        (
            (
                pl.col("merged_at_effective") - pl.col("first_deleg_at")
            ).dt.total_seconds()
            / 86400.0
        ).alias("days")
    )
    mm_lat = _joined.filter(
        pl.col("first_mm_at").is_not_null()
        & pl.col("merged_at_effective").is_not_null()
        & (pl.col("merged_at_effective") >= pl.col("first_mm_at"))
    ).with_columns(
        (
            (pl.col("merged_at_effective") - pl.col("first_mm_at")).dt.total_seconds()
            / 86400.0
        ).alias("days")
    )
    deleg_lat_post = deleg_lat.filter(pl.col("first_deleg_at") >= mm_cutover)
    mm_lat_post = mm_lat.filter(pl.col("first_mm_at") >= mm_cutover)
    return deleg_lat, deleg_lat_post, mm_lat_post


@app.cell
def _(deleg_lat_post, mm_lat_post, mo, np, pl):
    """Headline latency comparison (post-cutover, days)."""

    def _pcts(arr: np.ndarray) -> dict:
        if arr.size == 0:
            return {"n": 0, "median_d": None, "p75_d": None, "p90_d": None}
        return {
            "n": int(arr.size),
            "median_d": round(float(np.median(arr)), 3),
            "p75_d": round(float(np.percentile(arr, 75)), 3),
            "p90_d": round(float(np.percentile(arr, 90)), 3),
        }

    _deleg_vals = deleg_lat_post["days"].to_numpy()
    _mm_vals = mm_lat_post["days"].to_numpy()
    signoff_latency = pl.DataFrame(
        [
            {"path": "delegated", **_pcts(_deleg_vals)},
            {"path": "maintainer-merge", **_pcts(_mm_vals)},
        ]
    )
    mo.md("### Sign-off → merge latency (post-cutover, days)")
    signoff_latency
    return


@app.cell
def _(deleg_lat_post, mm_lat_post, plt):
    """Side-by-side distribution of sign-off → merge latency (≤7d zoom)."""
    _fig, _ax = plt.subplots(figsize=(10, 4))
    _bins = [i * 0.25 for i in range(29)]  # 0..7 days, 0.25d bins
    _ax.hist(
        deleg_lat_post.filter(deleg_lat_post["days"] <= 7)["days"].to_numpy(),
        bins=_bins,
        color="#6aa3d8",
        alpha=0.6,
        label="delegated",
    )
    _ax.hist(
        mm_lat_post.filter(mm_lat_post["days"] <= 7)["days"].to_numpy(),
        bins=_bins,
        color="#c63",
        alpha=0.6,
        label="maintainer-merge",
    )
    _ax.set_xlabel("Days from sign-off → merge")
    _ax.set_ylabel("PRs")
    _ax.set_title("Sign-off → merge latency (post-cutover, ≤7d zoom)")
    _ax.legend()
    _ax.grid(True, alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(core_user_raw, pl, prs):
    """Author-login table for the self-merge attribution cell. `core_user` isn't
    loaded by `load_pr_interval_data` (it's loaded in the data cell — off disk
    locally, from the slimmed bundle in WASM), so we join it to `prs.author_id`
    here to map each PR to its author's GitHub login."""
    _users = core_user_raw.select(
        pl.col("id").alias("author_id"),
        pl.col("github_login").alias("author_login"),
    )
    pr_author = (
        prs.select("id", "author_id")
        .join(_users, on="author_id", how="left")
        .rename({"id": "pull_request_id"})
        .select("pull_request_id", "author_login")
    )
    return (pr_author,)


@app.cell
def _(attribute_label_events, deleg_lat, events, mo, pl, pr_author):
    """Self-merge rate: of delegated PRs that subsequently get a
    `ready-to-merge` attribution, how often is the bors-trigger the PR
    *author* themselves? That's the literal purpose of delegation."""
    _r2m = (
        attribute_label_events(events, "ready-to-merge")
        .filter(pl.col("attributed"))
        .select(
            "pull_request_id",
            pl.col("label_at").alias("r2m_at"),
            pl.col("inferred_actor").alias("r2m_actor"),
        )
    )
    _deleg_au = deleg_lat.select("pull_request_id", "first_deleg_at").join(
        pr_author, on="pull_request_id", how="left"
    )
    _pairs = (
        _deleg_au.join(_r2m, on="pull_request_id", how="left")
        .filter(pl.col("r2m_at") >= pl.col("first_deleg_at"))
        .sort(["pull_request_id", "r2m_at"])
        .unique(subset=["pull_request_id"], keep="first")
        .with_columns(
            (
                pl.col("r2m_actor").str.to_lowercase()
                == pl.col("author_login").str.to_lowercase()
            ).alias("author_self_merge")
        )
    )
    _n = _pairs.height
    _self = int(_pairs["author_self_merge"].sum())
    self_merge_table = pl.DataFrame(
        [
            {
                "delegated_PRs_with_subsequent_r2m": _n,
                "author_self_merge": _self,
                "share": round(_self / max(_n, 1), 3),
                "other_actor_merge": _n - _self,
            }
        ]
    )
    mo.md(
        "### Author self-merge rate after delegation\n"
        "For each delegated PR that later received a `ready-to-merge` "
        "attribution, was the bors-trigger actor the PR author?"
    )
    self_merge_table
    return


if __name__ == "__main__":
    app.run()
