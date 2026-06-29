"""Theme 4 — Topic-area health."""

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
        # tzdata: Pyodide ships no system zoneinfo database, so materializing
        # tz-aware datetimes (e.g. `.to_dicts()` on a UTC column) raises
        # ZoneInfoNotFoundError until this is installed.
        _ = await _micropip.install(
            [
                "polars",
                "pandas",
                "numpy",
                "pyarrow",
                "matplotlib",
                "scipy",
                "pyyaml",
                "tzdata",
            ]
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
    # Topic-area health (`t-*` labels)

    Per-area open backlog, throughput, latency, and active-reviewer
    coverage. Area membership is reconstructed from `LABELED`/`UNLABELED`
    events on the `t-*` topic labels; a PR with multiple `t-*` labels
    contributes to each area independently. Time windows are anchored
    to `asof` (artifact snapshot time).
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

    from datetime import timedelta, timezone

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    from qb_notebook.data_io import load_pr_interval_data, merged_prs_frame
    from qb_notebook.review_states import (
        attribute_label_events,
        label_intervals,
        label_overlap_seconds,
        labels_active_at,
        queue_window_intervals,
        reviewers_court_intervals,
    )
    from qb_notebook.teams import load as load_teams
    from qb_notebook.wasm_io import load_slimmed_data, load_teams_snapshot

    return (
        Path,
        attribute_label_events,
        label_intervals,
        label_overlap_seconds,
        labels_active_at,
        load_pr_interval_data,
        load_slimmed_data,
        load_teams,
        load_teams_snapshot,
        merged_prs_frame,
        np,
        pl,
        plt,
        queue_window_intervals,
        reviewers_court_intervals,
        timedelta,
        timezone,
    )


@app.cell
def _(
    Path,
    is_wasm,
    load_pr_interval_data,
    load_slimmed_data,
    mo,
):
    if is_wasm:
        # Slimmed per-notebook parquet shipped under the site's public/ folder
        # (see scripts/export_wasm_data.py); same dict shape as the full loader.
        data = load_slimmed_data(str(mo.notebook_location() / "public"))
    else:
        _data_dir = Path(__file__).resolve().parents[1] / "data"
        data = load_pr_interval_data(_data_dir)
    prs = data["prs"]
    events = data["events"]
    label_defs = data["label_defs"]
    prlabel = data["prlabel"]
    queue_windows = data["queue_windows"]
    # `asof` is the artifact snapshot time — the latest moment any table
    # observed state — NOT wall-clock now(). These notebooks (especially the
    # frozen WASM export) read a static snapshot, so a live clock drifts past
    # the last data point and empties every "last N days" window: §5's 30d/60d
    # coverage pivot then loses a bucket column and raises StopIteration. Per
    # AGENTS.md, anchor relative windows to max(date). `events.occurred_at` is
    # the latest column that survives slimming and bounds every other ts here.
    asof = events["occurred_at"].max()
    return asof, events, label_defs, prlabel, prs, queue_windows


@app.cell
def _(Path, is_wasm, load_teams, load_teams_snapshot, mo):
    """Team-membership overlay for the (disabled) §4 reviewer × area views.
    Kept enabled but silent so §4 re-enables cleanly — the status callout that
    named team sizes is intentionally not rendered. In WASM it loads the
    `teams.json` snapshot shipped under public/ (see
    scripts/build_wasm_site.write_teams_snapshot); locally it reads the sibling
    `leanprover-community.github.io` checkout, or None if that checkout is
    missing."""
    if is_wasm:
        teams = load_teams_snapshot(str(mo.notebook_location() / "public"))
    else:
        _candidate = (
            Path(__file__).resolve().parents[2] / "leanprover-community.github.io"
        )
        if _candidate.exists():
            teams = load_teams(_candidate, warn_on_unmatched=False)
        else:
            teams = None
    return (teams,)


@app.cell
def _(label_defs, pl):
    """Topic-area label inventory: every label whose name starts with `t-`."""
    t_label_defs = (
        label_defs.filter(pl.col("name").str.starts_with("t-"))
        .select("id", "name")
        .sort("name")
    )
    t_label_names = t_label_defs["name"].to_list()
    return t_label_defs, t_label_names


@app.cell
def _(asof, events, label_intervals, pl, t_label_names):
    """Per-PR per-area intervals over the full timeline. Open intervals
    (label still applied at `asof`) get `end = null`, `end_effective = asof`.

    `first_applied_per_area` records when each `t-*` label first appeared
    in the LABELED stream. Note that `label_defs.created_at` is **not**
    the introduction date — it's just when the syncer first inserted the
    row, which is the artifact build time. The first-LABELED event is
    the closest signal we have to "the label started being used."
    """
    t_intervals = label_intervals(events, t_label_names, asof=asof)
    first_applied_per_area = (
        t_intervals.group_by("label_name")
        .agg(pl.col("start").min().alias("first_applied"))
        .rename({"label_name": "area"})
        .sort("first_applied")
    )
    return first_applied_per_area, t_intervals


@app.cell
def _(mo):
    mo.md("""
    ## 1. Current open backlog by area

    Counts come from the `prlabel` current-state table joined to open PRs;
    age is `asof - gh_created_at`. The bottom rows are areas with very
    small backlogs — useful sanity check but mind the small-N when
    reading the median.
    """)
    return


@app.cell
def _(asof, pl, prlabel, prs, t_label_defs):
    _open_prs = prs.filter(pl.col("state") == "open").select(
        pl.col("id").alias("pull_request_id"), "gh_created_at"
    )
    open_backlog = (
        prlabel.join(t_label_defs, left_on="label_def_id", right_on="id", how="inner")
        .join(_open_prs, on="pull_request_id", how="inner")
        .group_by("name")
        .agg(
            [
                pl.len().alias("open_prs"),
                (
                    (pl.lit(asof) - pl.col("gh_created_at")).dt.total_seconds().median()
                    / 86400.0
                ).alias("median_age_d"),
                (
                    (pl.lit(asof) - pl.col("gh_created_at"))
                    .dt.total_seconds()
                    .quantile(0.9)
                    / 86400.0
                ).alias("p90_age_d"),
            ]
        )
        .sort("open_prs", descending=True)
    )
    open_backlog
    return (open_backlog,)


@app.cell
def _(np, open_backlog, plt):
    _names = open_backlog["name"].to_numpy()
    _counts = open_backlog["open_prs"].to_numpy()
    _ages = open_backlog["median_age_d"].to_numpy()
    _y = np.arange(len(_names))
    _fig, (_ax1, _ax2) = plt.subplots(
        1, 2, figsize=(12, max(4, 0.28 * len(_names))), sharey=True
    )
    _ax1.barh(_y, _counts, color="#6aa3d8")
    _ax1.set_yticks(_y, _names)
    _ax1.invert_yaxis()
    _ax1.set_xlabel("Open PRs")
    _ax1.set_title("Open PRs by area")
    _ax2.barh(_y, _ages, color="#c69")
    _ax2.set_xlabel("Median age (days)")
    _ax2.set_title("Median open-PR age by area")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ### Currently on the queue by area

    Same shape as above but counts only PRs with an **open queue window**
    in ruleset 3 (i.e. currently eligible for reviewer attention). Age is
    the **total time on queue across cycles** — the sum of every queue
    window's duration per PR, so a PR that bounced between queue and
    author-court accumulates only its on-queue time. Currently-open
    windows contribute up to `asof`.
    """)
    return


@app.cell
def _(asof, pl, prlabel, queue_window_intervals, queue_windows, t_label_defs):
    _windows = queue_window_intervals(queue_windows, asof=asof)
    _on_queue_prs = (
        _windows.filter(pl.col("is_open")).select("pull_request_id").unique()
    )
    _total_on_queue = (
        _windows.join(_on_queue_prs, on="pull_request_id", how="inner")
        .group_by("pull_request_id")
        .agg(pl.col("duration_days").sum().alias("total_on_queue_d"))
    )
    queue_backlog = (
        prlabel.join(t_label_defs, left_on="label_def_id", right_on="id", how="inner")
        .join(_total_on_queue, on="pull_request_id", how="inner")
        .group_by("name")
        .agg(
            [
                pl.len().alias("on_queue_prs"),
                pl.col("total_on_queue_d").median().alias("median_on_queue_d"),
                pl.col("total_on_queue_d").quantile(0.9).alias("p90_on_queue_d"),
            ]
        )
        .sort("on_queue_prs", descending=True)
    )
    queue_backlog
    return (queue_backlog,)


@app.cell
def _(np, plt, queue_backlog):
    _names = queue_backlog["name"].to_numpy()
    _counts = queue_backlog["on_queue_prs"].to_numpy()
    _ages = queue_backlog["median_on_queue_d"].to_numpy()
    _y = np.arange(len(_names))
    _fig, (_ax1, _ax2) = plt.subplots(
        1, 2, figsize=(12, max(4, 0.28 * len(_names))), sharey=True
    )
    _ax1.barh(_y, _counts, color="#6aa3d8")
    _ax1.set_yticks(_y, _names)
    _ax1.invert_yaxis()
    _ax1.set_xlabel("PRs on queue")
    _ax1.set_title("PRs currently on queue by area")
    _ax2.barh(_y, _ages, color="#c69")
    _ax2.set_xlabel("Median total time on queue (days)")
    _ax2.set_title("Median total queue time by area")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. Throughput

    Merges (to master, bors-aware) attributed to the `t-*` label active
    at the effective merge time. A multi-area PR contributes to every
    matching area. The heatmap captures month × area; the right-hand bar
    is the total merges per area in the last 365 days.
    """)
    return


@app.cell
def _(labels_active_at, merged_prs_frame, pl, prs, t_intervals):
    """Per (area, merge_month) row for every merge-to-master event whose PR
    carried at least one `t-*` label at merge time."""
    merged = merged_prs_frame(prs, effective_col="merged_at").select(
        pl.col("id").alias("pull_request_id"), "gh_created_at", "merged_at"
    )
    merge_pts = merged.select(
        "pull_request_id", "gh_created_at", pl.col("merged_at").alias("at")
    )
    merges_by_area = (
        labels_active_at(t_intervals, merge_pts)
        .rename({"label_name": "area", "at": "merged_at"})
        .with_columns(
            [
                pl.col("merged_at").dt.truncate("1mo").alias("month"),
                (
                    (pl.col("merged_at") - pl.col("gh_created_at")).dt.total_seconds()
                    / 86400.0
                ).alias("ttm_days"),
            ]
        )
    )
    return (merges_by_area,)


@app.cell
def _(asof, first_applied_per_area, merges_by_area, np, pl, plt, timedelta):
    """area × month → merges heatmap (months on x, areas on y, color = log1p
    of merge count). White circles mark the first month a `t-*` label was
    applied, so a long dark stretch followed by a sudden onset reads as
    "label was introduced here," not "area was inactive."""
    cutoff = asof - timedelta(days=365 * 4)  # cap to ~recent 4 years
    monthly = (
        merges_by_area.filter(pl.col("month") >= cutoff)
        .group_by(["area", "month"])
        .agg(pl.len().alias("n"))
    )
    if monthly.height == 0:
        _fig = plt.figure(figsize=(8, 3))
        _ax = _fig.add_subplot()
        _ax.text(0.5, 0.5, "no data in cutoff window", ha="center")
        _fig
    else:
        _areas = (
            monthly.group_by("area")
            .agg(pl.col("n").sum().alias("total"))
            .sort("total", descending=True)["area"]
            .to_list()
        )
        _months = sorted(monthly["month"].unique().to_list())
        _idx = {a: i for i, a in enumerate(_areas)}
        _midx = {m: i for i, m in enumerate(_months)}
        _grid = np.zeros((len(_areas), len(_months)))
        for _row in monthly.iter_rows(named=True):
            _grid[_idx[_row["area"]], _midx[_row["month"]]] = _row["n"]
        _fig, _ax = plt.subplots(figsize=(12, max(4, 0.32 * len(_areas))))
        _im = _ax.imshow(
            np.log1p(_grid),
            aspect="auto",
            cmap="viridis",
            interpolation="nearest",
        )
        _ax.set_yticks(np.arange(len(_areas)), _areas)
        _xtick_idx = np.linspace(0, len(_months) - 1, min(10, len(_months))).astype(int)
        _ax.set_xticks(_xtick_idx, [_months[i].strftime("%Y-%m") for i in _xtick_idx])
        # Mark each area's first-applied month. Areas whose first apply
        # predates the visible cutoff get no marker (they're already "old"
        # by the time the heatmap starts).
        _intro = first_applied_per_area.with_columns(
            pl.col("first_applied").dt.truncate("1mo").alias("first_month")
        )
        _xs, _ys = [], []
        for _row in _intro.iter_rows(named=True):
            _a, _m = _row["area"], _row["first_month"]
            if _a in _idx and _m in _midx:
                _xs.append(_midx[_m])
                _ys.append(_idx[_a])
        if _xs:
            _ax.scatter(
                _xs,
                _ys,
                facecolors="none",
                edgecolors="white",
                s=42,
                linewidths=1.5,
                label="first applied",
            )
            _ax.legend(loc="upper left", fontsize=8, framealpha=0.7)
        _ax.set_title("Merges per area per month (color = log1p(count))")
        _fig.colorbar(_im, ax=_ax, label="log1p(merges)")
        _fig.tight_layout()
    _fig
    return


@app.cell
def _(first_applied_per_area, mo):
    mo.md(
        "### `t-*` label introduction dates\n"
        "First-LABELED timestamp per area in the timeline. mathlib4 "
        "rolled out the topic taxonomy in batches: an initial group in "
        "July 2023, `t-data` in 2024-08, then `t-ring-theory` / "
        "`t-group-theory` (carved out of `t-algebra`) in 2025-08. The "
        "dark left edge of those rows in the heatmap is taxonomy "
        "evolution, not silence."
    )
    first_applied_per_area
    return


@app.cell
def _(asof, merges_by_area, mo, pl, timedelta):
    """Per-area summary table: merges in last 365d, median time-to-merge."""
    cutoff_365 = asof - timedelta(days=365)
    throughput_365d = (
        merges_by_area.filter(pl.col("merged_at") >= cutoff_365)
        .group_by("area")
        .agg(
            [
                pl.len().alias("merges_365d"),
                pl.col("ttm_days").median().alias("median_ttm_d"),
                pl.col("ttm_days").quantile(0.9).alias("p90_ttm_d"),
            ]
        )
        .sort("merges_365d", descending=True)
    )
    mo.md("### Per-area throughput (last 365 days)")
    throughput_365d
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Reviewer-court latency by area

    For each area-application interval (one row per `t-*` LABELED span
    per PR), we intersect it with the PR's reviewer-court intervals
    (queue-window primary, `awaiting-review` label fallback) via
    `label_overlap_seconds`. Sum per area gives total seconds spent in
    the reviewer's court while carrying that area label; we then turn
    that into a per-application distribution.
    """)
    return


@app.cell
def _(
    asof,
    events,
    label_overlap_seconds,
    pl,
    queue_windows,
    reviewers_court_intervals,
    t_intervals,
    timedelta,
    timezone,
):
    """Per-area-application court-time. Filter to area applications that
    overlap the last 2 years so this stays representative of modern
    review tempo (and avoids skew from very long-lived ancient PRs)."""
    from datetime import datetime as _dt_cls

    _court_clamp = _dt_cls(2024, 7, 10, tzinfo=timezone.utc)
    court = reviewers_court_intervals(
        events, queue_windows, asof=asof, label_asof=_court_clamp
    )
    area_windows = t_intervals.filter(
        pl.col("end_effective") >= asof - timedelta(days=365 * 2)
    ).select(
        [
            "pull_request_id",
            pl.col("label_name").alias("area"),
            pl.col("start").alias("window_start"),
            pl.col("end_effective").alias("window_end"),
        ]
    )
    area_court = label_overlap_seconds(court, area_windows)
    court_by_area = (
        area_court.with_columns(
            (pl.col("overlap_seconds") / 86400.0).alias("court_days")
        )
        .group_by("area")
        .agg(
            [
                pl.len().alias("area_applications"),
                pl.col("court_days").median().alias("median_court_d"),
                pl.col("court_days").quantile(0.9).alias("p90_court_d"),
                pl.col("had_overlap").mean().alias("share_with_court_time"),
            ]
        )
        .sort("median_court_d", descending=True)
    )
    court_by_area
    return


# Section 4 is disabled to avoid surfacing individual reviewer / maintainer
# logins (the reviewer × area heatmap lists them on its y-axis). The compute
# cell below is kept enabled because §5 (declining coverage) reuses
# `reviewer_area` / `cutoff_30` / `cutoff_60`. Re-enable the section by
# dropping `disabled=True` from these four cells.
@app.cell(disabled=True)
def _(mo):
    mo.md("""
    ## 4. Active reviewer coverage by area

    For each `LABELED(maintainer-merge)` event we infer the human trigger
    via `attribute_label_events` (default 10 min window) and look up
    which `t-*` labels were active on the PR at the trigger time. Active
    reviewer count is the number of distinct trigger actors in the last
    30 days per area.
    """)
    return


@app.cell
def _(
    asof,
    attribute_label_events,
    events,
    labels_active_at,
    pl,
    t_intervals,
    timedelta,
):
    """Attributed maintainer-merge triggers × area at trigger time."""
    mm_attr = attribute_label_events(events, "maintainer-merge").filter(
        pl.col("attributed")
    )
    trigger_pts = mm_attr.select(
        "pull_request_id",
        pl.col("inferred_actor").alias("reviewer"),
        pl.col("trigger_at").alias("at"),
    )
    reviewer_area = labels_active_at(t_intervals, trigger_pts).rename(
        {"label_name": "area"}
    )

    cutoff_30 = asof - timedelta(days=30)
    cutoff_60 = asof - timedelta(days=60)
    active_30d = (
        reviewer_area.filter(pl.col("at") >= cutoff_30)
        .group_by("area")
        .agg(
            [
                pl.col("reviewer").n_unique().alias("active_reviewers_30d"),
                pl.len().alias("triggers_30d"),
            ]
        )
        .sort("active_reviewers_30d", descending=True)
    )
    return active_30d, cutoff_30, cutoff_60, reviewer_area


@app.cell(disabled=True)
def _(active_30d, mo):
    mo.md("### Active reviewers per area (last 30 days)")
    active_30d
    return


@app.cell(disabled=True)
def _(np, pl, plt, reviewer_area, teams):
    """Top-15 areas × top-20 reviewers (count of attributed `maintainer-merge`
    triggers all-time). PRs with no `t-*` label at trigger time are excluded
    by construction (no row out of `labels_active_at`). When the team
    snapshot is available, y-tick labels are colored by team membership
    (maintainer / reviewer / other), mirroring the bar coloring in
    `reviewer_load.py`."""
    _top_areas = (
        reviewer_area.group_by("area")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)["area"]
        .head(15)
        .to_list()
    )
    _top_reviewers = (
        reviewer_area.group_by("reviewer")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)["reviewer"]
        .head(20)
        .to_list()
    )
    _mat = (
        reviewer_area.filter(pl.col("area").is_in(_top_areas))
        .filter(pl.col("reviewer").is_in(_top_reviewers))
        .group_by(["reviewer", "area"])
        .agg(pl.len().alias("n"))
    )
    _grid = np.zeros((len(_top_reviewers), len(_top_areas)))
    _ri = {r: i for i, r in enumerate(_top_reviewers)}
    _ai = {a: i for i, a in enumerate(_top_areas)}
    for _row in _mat.iter_rows(named=True):
        _grid[_ri[_row["reviewer"]], _ai[_row["area"]]] = _row["n"]
    _fig, _ax = plt.subplots(figsize=(12, 0.4 * len(_top_reviewers) + 2))
    _im = _ax.imshow(
        np.log1p(_grid), aspect="auto", cmap="magma", interpolation="nearest"
    )
    _ax.set_yticks(np.arange(len(_top_reviewers)), _top_reviewers)
    _ax.set_xticks(np.arange(len(_top_areas)), _top_areas, rotation=60, ha="right")
    _ax.set_title("Top reviewers × top areas (color = log1p(triggers all-time))")
    _fig.colorbar(_im, ax=_ax, label="log1p(triggers)")
    if teams is not None:
        _team_colors = {"maintainer": "#3a6", "reviewer": "#6aa3d8", "other": "#888"}
        _maint = teams.maintainers
        _rev = teams.reviewers
        for _tick, _name in zip(_ax.get_yticklabels(), _top_reviewers):
            _login = _name.lower()
            _team = (
                "maintainer"
                if _login in _maint
                else ("reviewer" if _login in _rev else "other")
            )
            _tick.set_color(_team_colors[_team])
        _handles = [
            plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=_c, label=_t)
            for _t, _c in _team_colors.items()
        ]
        _ax.legend(handles=_handles, loc="upper right", fontsize=8, title="Team")
    _fig.tight_layout()
    _fig
    return


@app.cell(disabled=True)
def _(mo, pl, reviewer_area, teams):
    """Per-area team-coverage breakdown: distinct attributed
    `maintainer-merge` trigger actors per area, split into
    maintainer-team / reviewer-team / other contributors. Skipped when
    the team snapshot isn't available."""
    if teams is None:
        area_team_coverage = mo.md(
            "_Team snapshot unavailable — skipping per-area team coverage table._"
        )
    else:
        _maint = teams.maintainers
        _rev = teams.reviewers
        _classified = reviewer_area.with_columns(
            pl.col("reviewer")
            .str.to_lowercase()
            .map_elements(
                lambda a: "maintainer"
                if a in _maint
                else ("reviewer" if a in _rev else "other"),
                return_dtype=str,
            )
            .alias("team")
        )
        _pivot = (
            _classified.group_by(["area", "team"])
            .agg(pl.col("reviewer").n_unique().alias("reviewers"))
            .pivot(on="team", index="area", values="reviewers")
            .fill_null(0)
        )
        # Ensure all three team columns exist even if a team is absent
        # from the data (e.g. no "other" contributors in any area).
        _missing = [
            t for t in ("maintainer", "reviewer", "other") if t not in _pivot.columns
        ]
        if _missing:
            _pivot = _pivot.with_columns([pl.lit(0).alias(t) for t in _missing])
        area_team_coverage = _pivot.select(
            ["area", "maintainer", "reviewer", "other"]
        ).sort("maintainer", descending=True)
    area_team_coverage
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. Declining coverage (30d vs preceding 30d)

    For each area, attributed `maintainer-merge` triggers in the last
    30 days vs. the 30 days before that. Areas with a meaningful drop
    (≥30 % and ≥5 triggers in the prior window — small areas are noisy)
    are flagged. Negative `pct_change` means activity is falling.
    """)
    return


@app.cell
def _(cutoff_30, cutoff_60, mo, pl, reviewer_area):
    bucketed = reviewer_area.with_columns(
        pl.when(pl.col("at") >= cutoff_30)
        .then(pl.lit("last_30d"))
        .when(pl.col("at") >= cutoff_60)
        .then(pl.lit("prior_30d"))
        .otherwise(pl.lit(None))
        .alias("bucket")
    ).filter(pl.col("bucket").is_not_null())
    # Conditional aggregation rather than `.pivot(...)`: a pivot omits the
    # column for a bucket with zero rows (e.g. a quiet last-30d window), so a
    # name/position-based lookup of the pivoted columns raises StopIteration.
    # Per-area filtered counts always yield all four columns (0 where a bucket
    # is empty) and don't depend on polars' cross-version pivot naming.
    _is_last = pl.col("bucket") == "last_30d"
    _is_prior = pl.col("bucket") == "prior_30d"
    coverage = (
        bucketed.group_by("area")
        .agg(
            pl.col("bucket").filter(_is_last).count().alias("triggers_last_30d"),
            pl.col("bucket").filter(_is_prior).count().alias("triggers_prior_30d"),
            pl.col("reviewer").filter(_is_last).n_unique().alias("reviewers_last_30d"),
            pl.col("reviewer")
            .filter(_is_prior)
            .n_unique()
            .alias("reviewers_prior_30d"),
        )
        .with_columns(
            pl.when(pl.col("triggers_prior_30d") > 0)
            .then(
                (pl.col("triggers_last_30d") - pl.col("triggers_prior_30d")).cast(
                    pl.Float64
                )
                / pl.col("triggers_prior_30d")
                * 100
            )
            .otherwise(None)
            .alias("pct_change")
        )
        .with_columns(
            (
                (pl.col("pct_change") <= -30.0) & (pl.col("triggers_prior_30d") >= 5)
            ).alias("declining")
        )
        .sort("pct_change")
    )
    mo.md("### 30d vs prior 30d trigger coverage")
    coverage
    return (coverage,)


@app.cell
def _(coverage, mo, pl):
    flagged = coverage.filter(pl.col("declining"))
    if flagged.height == 0:
        mo.md(
            "**No areas are flagged as declining at the current threshold** (≥30 % drop and ≥5 prior-window triggers)."
        )
    else:
        mo.md(f"### Flagged: {flagged.height} area(s) with declining coverage")
    flagged
    return


@app.cell
def _(mo):
    mo.md("""
    ## Notes

    - Area membership uses `LABELED` history, so a re-labeling event
      mid-life moves the PR into the new area only after that moment.
      Throughput / latency are attributed by which `t-*` was active at
      `merged_at`; a PR mislabeled at merge will mis-attribute.
    - The `t-*` taxonomy was rolled out incrementally on mathlib4: an
      initial batch in 2023-07, `t-data` in 2024-08, `t-ring-theory`
      and `t-group-theory` (likely carved out of `t-algebra`) in
      2025-08. The heatmap marks each area's first-applied month with
      a white circle so the dark left edge isn't misread as inactivity.
      `label_defs.created_at` is **not** the introduction date — it's
      just when the syncer inserted the row, which is artifact build
      time.
    - ~17 % of recent merged PRs carry **no** `t-*` label at merge
      time — they're invisible to per-area throughput. The plot above
      is therefore a lower bound on total area work.
    - "Reviewer-court latency by area" filters to area applications
      whose interval overlaps the last 2 years to keep numbers
      representative of the current process.
    - Geometric-group-theory and condensed-mathematics have very small
      sample sizes; tables sort by activity so they fall to the
      bottom, but mind the small-N when reading their medians.
    """)
    return


if __name__ == "__main__":
    app.run()
