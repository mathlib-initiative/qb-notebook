"""Theme 5 - PR-shape effects (size, contributor type, draft start)."""

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
        # parquet).
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
    # PR-shape effects

    Do bigger PRs take disproportionately longer to merge? Do first-time
    contributors wait longer or churn more? Are PRs that open as drafts
    different? Does `feat:` move at a different tempo than `chore:` or
    `refactor:`? This notebook slices the merge / court-exit metrics
    from Themes 1-4 along five axes of the PR itself:

    - **size**: `additions + deletions` and `changed_files_count` bucketed,
    - **author cohort**: first-PR (this author's earliest in the dataset)
      vs returning, plus author's PR-sequence number,
    - **draft history**: did the PR open as draft, or non-draft?
    - **WIP-at-open**: did the mathlib4 `WIP` label fire within 10 min
      of PR creation (parallel to draft, but project-specific)?
    - **type**: conventional-commit prefix on the title
      (`feat:`, `chore:`, `fix:`, `refactor:`, `doc:`, `perf:`, `ci:`,
      `style:`, `test:`, plus `other` / `unparsed` buckets).

    All cuts share the same per-PR row built up in the *cohort* cell, so
    the sections are directly comparable.
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

    from datetime import timezone

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    from qb_notebook.data_io import load_pr_interval_data, merged_prs_frame
    from qb_notebook.filters import expr_merged_to_master
    from qb_notebook.pr_shape import (
        DEFAULT_FILES_BREAKS,
        DEFAULT_LINES_BREAKS,
        DEFAULT_PR_TYPES,
        author_cohort,
        bucket_labels,
        had_wip_label_at_open,
        pr_type,
        size_buckets,
        started_as_draft,
    )
    from qb_notebook.review_states import (
        attribute_label_events,
        reviewers_court_intervals,
    )
    from qb_notebook.wasm_io import load_slimmed_data

    return (
        DEFAULT_FILES_BREAKS,
        DEFAULT_LINES_BREAKS,
        DEFAULT_PR_TYPES,
        Path,
        attribute_label_events,
        author_cohort,
        bucket_labels,
        expr_merged_to_master,
        had_wip_label_at_open,
        load_pr_interval_data,
        load_slimmed_data,
        merged_prs_frame,
        np,
        pl,
        plt,
        pr_type,
        reviewers_court_intervals,
        size_buckets,
        started_as_draft,
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
    prs_raw = data["prs"]
    events = data["events"]
    queue_windows = data["queue_windows"]
    # Snapshot time, not wall-clock now(): these notebooks read a frozen data
    # snapshot (especially the WASM export), so anchoring relative windows to a
    # live clock drifts past the last observed event and empties every "last N
    # days" window. `events.occurred_at` is the latest column surviving slimming
    # and bounds the other timestamps. Per AGENTS.md, anchor windows to max(date).
    asof = events["occurred_at"].max()
    return asof, events, prs_raw, queue_windows


@app.cell
def _(
    author_cohort,
    events,
    had_wip_label_at_open,
    pr_type,
    prs_raw,
    size_buckets,
    started_as_draft,
):
    """Per-PR shape attributes: size buckets, author cohort, draft history,
    WIP-at-open, type.

    `prs` is the prs frame plus columns:
      - `lines_changed`, `lines_bucket`, `files_bucket`
      - `author_first_pr_at`, `author_pr_seq`, `is_first_pr`
      - `started_as_draft`
      - `had_wip_label_at_open` (mathlib4 `WIP` label applied within
        10 min of PR creation — workflow-signal companion to
        `started_as_draft`)
      - `pr_type` (conventional-commit prefix bucketed into 9 canonical
        types + `other` + `unparsed`)

    "First-time" is relative to the dataset as a whole (full prs table,
    not filtered to merged). Reasonable since the snapshot covers the
    project lifetime; new authors who later return show up as
    `is_first_pr=False` on subsequent PRs.
    """
    prs = pr_type(
        had_wip_label_at_open(
            started_as_draft(author_cohort(size_buckets(prs_raw)), events),
            events,
        )
    )
    return (prs,)


@app.cell
def _(merged_prs_frame, pl, prs):
    """Merge cohort: PRs merged to master with bors-aware merged_at.

    `merged_prs` is the merged subset with:
      - `merged_at` (effective: bors fallback to closed_at)
      - `ttm_days` (gh_created_at -> merged_at)
      - all cohort attrs from `prs`.
    """
    merged_prs = (
        merged_prs_frame(prs, effective_col="merged_at")
        .with_columns(
            (
                (pl.col("merged_at") - pl.col("gh_created_at")).dt.total_seconds()
                / 86400.0
            ).alias("ttm_days")
        )
        .filter(pl.col("ttm_days").is_not_null() & (pl.col("ttm_days") >= 0))
    )
    return (merged_prs,)


@app.cell
def _(asof, events, pl, queue_windows, reviewers_court_intervals, timezone):
    """First reviewer-court exit per PR.

    Uses `reviewers_court_intervals` (queue-window primary, retired
    `awaiting-review` label fallback for ~219 PRs). Picks the earliest
    closed interval per PR; PRs still in their first court interval at
    `asof` are excluded from this metric.

    `time_to_first_court_exit_days` = first `end` - first `start`.
    """
    from datetime import datetime as _dt_cls

    _court_clamp = _dt_cls(2024, 7, 10, tzinfo=timezone.utc)
    court = reviewers_court_intervals(
        events, queue_windows, asof=asof, label_asof=_court_clamp
    )
    first_court_exit = (
        court.filter(~pl.col("is_open"))
        .sort(["pull_request_id", "start"])
        .group_by("pull_request_id", maintain_order=True)
        .agg(
            [
                pl.col("start").first().alias("first_court_start"),
                pl.col("end").first().alias("first_court_end"),
            ]
        )
        .with_columns(
            (
                (
                    pl.col("first_court_end") - pl.col("first_court_start")
                ).dt.total_seconds()
                / 86400.0
            ).alias("time_to_first_court_exit_days")
        )
    )
    return (first_court_exit,)


@app.cell
def _(mo):
    mo.md("""
    ## 1. Size effects

    Bigger PRs almost certainly take longer to merge, but the
    question is whether the relationship is roughly linear or
    super-linear, and whether time-to-first-court-exit (how long
    until *someone* responds to the PR) scales the same way as
    time-to-merge.

    Buckets are inclusive upper bounds; `1001+` is the long tail.
    Plots use merged-to-master PRs only so the merge-time axis is
    well defined.
    """)
    return


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, merged_prs, pl):
    """Per-bucket TTM summary, sorted by canonical bucket order."""
    _order = bucket_labels(DEFAULT_LINES_BREAKS)
    ttm_by_lines = (
        merged_prs.group_by("lines_bucket")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("ttm_days").median().alias("median_d"),
                pl.col("ttm_days").quantile(0.75).alias("p75_d"),
                pl.col("ttm_days").quantile(0.9).alias("p90_d"),
            ]
        )
        .with_columns(pl.col("lines_bucket").cast(pl.Enum(_order)).alias("_order"))
        .sort("_order")
        .drop("_order")
    )
    ttm_by_lines
    return


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, merged_prs, np, pl, plt):
    """Time-to-merge box plot by lines-changed bucket. Y-axis clipped at
    60 days; the long tail past that is dominated by stalled-but-not-yet-
    abandoned PRs which the bucket-median already captures."""
    _order = bucket_labels(DEFAULT_LINES_BREAKS)
    _data = [
        merged_prs.filter(pl.col("lines_bucket") == _b)["ttm_days"].to_numpy()
        for _b in _order
    ]
    _fig, _ax = plt.subplots(figsize=(9, 4))
    _ax.boxplot(
        [_d[_d <= 60] for _d in _data],
        tick_labels=_order,
        showfliers=False,
        widths=0.6,
    )
    _ax.set_ylabel("Time to merge (days, ≤60d shown)")
    _ax.set_xlabel("Lines changed (additions + deletions)")
    _ax.set_title("TTM by PR size (lines changed)")
    _ax.grid(axis="y", alpha=0.3)
    _ns = [len(_d) for _d in _data]
    for _i, _n in enumerate(_ns):
        _ax.text(_i + 1, _ax.get_ylim()[1] * 0.95, f"n={_n}", ha="center", fontsize=8)
    _ = np  # silence unused-import lint on np
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(DEFAULT_FILES_BREAKS, bucket_labels, merged_prs, pl):
    """Per-files-bucket TTM summary."""
    _order = bucket_labels(DEFAULT_FILES_BREAKS)
    ttm_by_files = (
        merged_prs.group_by("files_bucket")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("ttm_days").median().alias("median_d"),
                pl.col("ttm_days").quantile(0.75).alias("p75_d"),
                pl.col("ttm_days").quantile(0.9).alias("p90_d"),
            ]
        )
        .with_columns(pl.col("files_bucket").cast(pl.Enum(_order)).alias("_order"))
        .sort("_order")
        .drop("_order")
    )
    ttm_by_files
    return


@app.cell
def _(
    DEFAULT_LINES_BREAKS,
    bucket_labels,
    first_court_exit,
    merged_prs,
    pl,
    plt,
):
    """Court-exit time by lines bucket. Restricts to merged PRs that
    have a closed first court interval so the metric is well defined
    and comparable across cohorts."""
    _order = bucket_labels(DEFAULT_LINES_BREAKS)
    court_cohort = merged_prs.select("id", "lines_bucket").join(
        first_court_exit.select("pull_request_id", "time_to_first_court_exit_days"),
        left_on="id",
        right_on="pull_request_id",
        how="inner",
    )
    _data = [
        court_cohort.filter(pl.col("lines_bucket") == _b)[
            "time_to_first_court_exit_days"
        ].to_numpy()
        for _b in _order
    ]
    _fig, _ax = plt.subplots(figsize=(9, 4))
    _ax.boxplot(
        [_d[_d <= 30] for _d in _data],
        tick_labels=_order,
        showfliers=False,
        widths=0.6,
    )
    _ax.set_ylabel("Time to first court exit (days, ≤30d shown)")
    _ax.set_xlabel("Lines changed (additions + deletions)")
    _ax.set_title("First-court-exit time by PR size")
    _ax.grid(axis="y", alpha=0.3)
    _ns = [len(_d) for _d in _data]
    for _i, _n in enumerate(_ns):
        _ax.text(_i + 1, _ax.get_ylim()[1] * 0.95, f"n={_n}", ha="center", fontsize=8)
    _fig.tight_layout()
    _fig
    return (court_cohort,)


@app.cell
def _(DEFAULT_LINES_BREAKS, bucket_labels, court_cohort, pl):
    """Per-bucket court-exit summary alongside the TTM table above."""
    _order = bucket_labels(DEFAULT_LINES_BREAKS)
    court_by_lines = (
        court_cohort.group_by("lines_bucket")
        .agg(
            [
                pl.len().alias("n_with_court_exit"),
                pl.col("time_to_first_court_exit_days").median().alias("median_d"),
                pl.col("time_to_first_court_exit_days").quantile(0.9).alias("p90_d"),
            ]
        )
        .with_columns(pl.col("lines_bucket").cast(pl.Enum(_order)).alias("_order"))
        .sort("_order")
        .drop("_order")
    )
    court_by_lines
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. First-time vs returning contributors

    "First-time" means this author's earliest PR in the dataset
    (`is_first_pr`). The funnel below counts how many PRs in each
    cohort merged, were abandoned (closed without merge), are still
    open, and were "reviewed" (received the `maintainer-merge`
    label at any point — bot-applied, so this is the reviewer
    sign-off proxy from Theme 2).

    The cohort sizes are heavily skewed (~38k returning vs ~850
    first-time) so rates rather than counts are the right read.
    """)
    return


@app.cell
def _(events, expr_merged_to_master, pl, prs):
    """Per-cohort funnel: merged / abandoned / open / reviewed.

    "Reviewed" = received at least one `LABELED(maintainer-merge)` event.
    Cohorts: first-time (`is_first_pr=True`), returning (False).
    Null-author PRs (24 of them) drop out of this cut by design.
    """
    _mm_prs = (
        events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == "maintainer-merge")
        )
        .select("pull_request_id")
        .unique()
    )
    funnel_base = prs.filter(pl.col("is_first_pr").is_not_null()).with_columns(
        [
            expr_merged_to_master().alias("_merged"),
            ((pl.col("state") == "closed") & ~expr_merged_to_master()).alias(
                "_abandoned"
            ),
            (pl.col("state") == "open").alias("_open"),
            pl.col("id").is_in(_mm_prs["pull_request_id"]).alias("_reviewed"),
        ]
    )
    cohort_funnel = (
        funnel_base.group_by("is_first_pr")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("_merged").sum().alias("merged"),
                pl.col("_abandoned").sum().alias("abandoned"),
                pl.col("_open").sum().alias("open"),
                pl.col("_reviewed").sum().alias("reviewed"),
            ]
        )
        .with_columns(
            [
                (pl.col("merged") / pl.col("n")).alias("merged_rate"),
                (pl.col("abandoned") / pl.col("n")).alias("abandoned_rate"),
                (pl.col("reviewed") / pl.col("n")).alias("reviewed_rate"),
            ]
        )
        .sort("is_first_pr", descending=True)
    )
    cohort_funnel
    return (cohort_funnel,)


@app.cell
def _(cohort_funnel, np, plt):
    """Bar chart: merged-rate / reviewed-rate / abandoned-rate per cohort."""
    _rows = list(cohort_funnel.iter_rows(named=True))
    _labels = ["first-time" if r["is_first_pr"] else "returning" for r in _rows]
    _metrics = ["merged_rate", "reviewed_rate", "abandoned_rate"]
    _colors = ["#4c9", "#39c", "#c63"]
    _x = np.arange(len(_labels))
    _w = 0.25
    _fig, _ax = plt.subplots(figsize=(7, 4))
    for _i, (_m, _c) in enumerate(zip(_metrics, _colors)):
        _vals = [r[_m] for r in _rows]
        _ax.bar(_x + (_i - 1) * _w, _vals, _w, label=_m.replace("_rate", ""), color=_c)
    _ax.set_xticks(_x, _labels)
    _ax.set_ylim(0, 1)
    _ax.set_ylabel("Share of cohort")
    _ax.set_title("Outcome rates by contributor cohort")
    _ax.legend(loc="upper right")
    _ax.grid(axis="y", alpha=0.3)
    for _i, _r in enumerate(_rows):
        _ax.text(
            _i,
            0.95,
            f"n={_r['n']:,}",
            ha="center",
            fontsize=9,
            transform=_ax.get_xaxis_transform(),
        )
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(merged_prs, np, pl, plt):
    """Time-to-merge CDF for merged-to-master PRs, by cohort.

    First-time-author merges are a fraction of the dataset; the CDF
    sample size is small enough that the curve is jittery, but the
    shape comparison vs returning authors is still informative.
    """
    _fig, _ax = plt.subplots(figsize=(8, 4))
    for _label, _is_first in [("first-time", True), ("returning", False)]:
        _vals = (
            merged_prs.filter(pl.col("is_first_pr") == _is_first)["ttm_days"]
            .drop_nulls()
            .to_numpy()
        )
        _vals = np.sort(_vals)
        if len(_vals) == 0:
            continue
        _y = np.arange(1, len(_vals) + 1) / len(_vals)
        _ax.plot(_vals, _y, label=f"{_label} (n={len(_vals)})")
    _ax.set_xscale("log")
    _ax.set_xlabel("Time to merge (days, log scale)")
    _ax.set_ylabel("CDF")
    _ax.set_title("TTM CDF by contributor cohort (merged-to-master only)")
    _ax.legend()
    _ax.grid(alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(merged_prs, pl):
    """Per-author-sequence TTM bins: does the gap shrink with experience?

    Group by PR-sequence buckets (1, 2-5, 6-20, 21+). Useful sanity
    check that "first-time penalty" - if any - mostly applies to the
    first one or two PRs, not author #5.
    """
    seq_buckets = (
        merged_prs.with_columns(
            pl.when(pl.col("author_pr_seq") == 1)
            .then(pl.lit("1"))
            .when(pl.col("author_pr_seq") <= 5)
            .then(pl.lit("2-5"))
            .when(pl.col("author_pr_seq") <= 20)
            .then(pl.lit("6-20"))
            .otherwise(pl.lit("21+"))
            .alias("seq_bucket")
        )
        .group_by("seq_bucket")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("ttm_days").median().alias("median_d"),
                pl.col("ttm_days").quantile(0.9).alias("p90_d"),
            ]
        )
        .with_columns(
            pl.col("seq_bucket")
            .cast(pl.Enum(["1", "2-5", "6-20", "21+"]))
            .alias("_order")
        )
        .sort("_order")
        .drop("_order")
    )
    seq_buckets
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Author -> reviewer concentration

    Do first-time-author PRs get reviewed by a narrow subset of
    reviewers? For each merged-to-master PR, we attribute the
    bot-applied `maintainer-merge` label to the human who triggered
    it via `attribute_label_events` (Theme 2 heuristic). Then we
    compare the Lorenz curve / Gini of reviewer trigger counts
    across first-time vs returning author cohorts.

    A higher Gini means a smaller subset of reviewers does the bulk
    of the work for that cohort. A *much* higher first-time Gini
    than returning would mean the project has a "newcomer reviewer
    bench" doing onboarding triage. A comparable Gini means the
    same broad pool handles both.
    """)
    return


@app.cell
def _(attribute_label_events, events, merged_prs, pl):
    """Attribute maintainer-merge triggers, join to merged_prs cohort cols.

    `mm_attr` carries the attributed reviewer per LABELED event; multiple
    applications to the same PR (e.g. force-push reapply) each count as
    one trigger, consistent with Theme 2 / `reviewer_load.py`.
    """
    mm_attr_raw = attribute_label_events(events, "maintainer-merge").filter(
        pl.col("attributed")
    )
    reviewer_cohort = mm_attr_raw.join(
        merged_prs.select("id", "is_first_pr", "author_id"),
        left_on="pull_request_id",
        right_on="id",
        how="inner",
    )
    return (reviewer_cohort,)


@app.cell
def _(np, pl, plt, reviewer_cohort):
    """Lorenz curves: cumulative share of triggers vs cumulative share of
    reviewers, for first-time-author vs returning-author PRs."""

    def _lorenz_and_gini(counts: np.ndarray):
        _sorted = np.sort(counts)
        if _sorted.sum() == 0:
            return np.array([0, 1]), np.array([0, 1]), 0.0
        _cum = np.cumsum(_sorted) / _sorted.sum()
        _x = np.arange(1, len(_sorted) + 1) / len(_sorted)
        _gini = 1.0 - 2.0 * np.trapz(_cum, _x)
        return np.concatenate([[0.0], _x]), np.concatenate([[0.0], _cum]), float(_gini)

    _fig, _ax = plt.subplots(figsize=(6, 6))
    _ax.plot([0, 1], [0, 1], "k--", alpha=0.4, label="equality")
    _summary_rows = []
    for _label, _is_first, _color in [
        ("first-time", True, "#c63"),
        ("returning", False, "#39c"),
    ]:
        _counts = (
            reviewer_cohort.filter(pl.col("is_first_pr") == _is_first)
            .group_by("inferred_actor")
            .agg(pl.len().alias("n"))["n"]
            .to_numpy()
        )
        if len(_counts) == 0:
            continue
        _x, _y, _gini = _lorenz_and_gini(_counts)
        _ax.plot(
            _x,
            _y,
            label=f"{_label} (n_reviewers={len(_counts)}, Gini={_gini:.2f})",
            color=_color,
        )
        _summary_rows.append(
            {
                "cohort": _label,
                "n_triggers": int(_counts.sum()),
                "n_reviewers": len(_counts),
                "gini": _gini,
            }
        )
    _ax.set_xlabel("Cumulative share of reviewers (sorted by activity)")
    _ax.set_ylabel("Cumulative share of attributed triggers")
    _ax.set_title("Reviewer concentration by contributor cohort")
    _ax.legend(loc="upper left")
    _ax.set_aspect("equal")
    _ax.grid(alpha=0.3)
    _fig.tight_layout()
    concentration_summary = pl.DataFrame(_summary_rows)
    _fig
    return (concentration_summary,)


@app.cell
def _(concentration_summary, mo):
    mo.md("### Concentration summary")
    concentration_summary
    return


@app.cell
def _(pl, reviewer_cohort):
    """For each first-time-author PR, who reviewed it? Top-N table.

    Most active reviewers for newcomers; useful for spotting if a small
    set of people effectively own onboarding review.
    """
    first_time_reviewers = (
        reviewer_cohort.filter(pl.col("is_first_pr"))
        .group_by("inferred_actor")
        .agg(pl.len().alias("first_time_triggers"))
        .sort("first_time_triggers", descending=True)
        .head(15)
    )
    first_time_reviewers
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4. Draft start

    `started_as_draft` is the *initial* state of the PR, reconstructed
    from the timeline: if the first `READY_FOR_REVIEW` / `CONVERT_TO_DRAFT`
    event is `READY_FOR_REVIEW`, the PR opened as draft. PRs with no
    such events fall back to the current `prs.is_draft` snapshot
    (catches drafts that never marked ready, mostly currently-open).

    Bucket sizes on the current artifact: ~2.2k started-as-draft vs
    ~36.4k opened non-draft.
    """)
    return


@app.cell
def _(events, expr_merged_to_master, pl, prs):
    """Outcome funnel by draft start (same shape as the contributor funnel)."""
    _mm_prs = (
        events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == "maintainer-merge")
        )
        .select("pull_request_id")
        .unique()
    )
    draft_funnel = (
        prs.with_columns(
            [
                expr_merged_to_master().alias("_merged"),
                ((pl.col("state") == "closed") & ~expr_merged_to_master()).alias(
                    "_abandoned"
                ),
                (pl.col("state") == "open").alias("_open"),
                pl.col("id").is_in(_mm_prs["pull_request_id"]).alias("_reviewed"),
            ]
        )
        .group_by("started_as_draft")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("_merged").sum().alias("merged"),
                pl.col("_abandoned").sum().alias("abandoned"),
                pl.col("_open").sum().alias("open"),
                pl.col("_reviewed").sum().alias("reviewed"),
            ]
        )
        .with_columns(
            [
                (pl.col("merged") / pl.col("n")).alias("merged_rate"),
                (pl.col("abandoned") / pl.col("n")).alias("abandoned_rate"),
                (pl.col("reviewed") / pl.col("n")).alias("reviewed_rate"),
            ]
        )
        .sort("started_as_draft", descending=True)
    )
    draft_funnel
    return


@app.cell
def _(merged_prs, np, pl, plt):
    """TTM CDF by draft start, merged-to-master only."""
    _fig, _ax = plt.subplots(figsize=(8, 4))
    for _label, _val in [("opened as draft", True), ("opened non-draft", False)]:
        _vals = (
            merged_prs.filter(pl.col("started_as_draft") == _val)["ttm_days"]
            .drop_nulls()
            .to_numpy()
        )
        _vals = np.sort(_vals)
        if len(_vals) == 0:
            continue
        _y = np.arange(1, len(_vals) + 1) / len(_vals)
        _ax.plot(_vals, _y, label=f"{_label} (n={len(_vals)})")
    _ax.set_xscale("log")
    _ax.set_xlabel("Time to merge (days, log scale)")
    _ax.set_ylabel("CDF")
    _ax.set_title("TTM CDF by draft start (merged-to-master)")
    _ax.legend()
    _ax.grid(alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(merged_prs, pl):
    """TTM percentiles by draft start, with cohort sizes."""
    ttm_by_draft = (
        merged_prs.group_by("started_as_draft")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("ttm_days").median().alias("median_d"),
                pl.col("ttm_days").quantile(0.75).alias("p75_d"),
                pl.col("ttm_days").quantile(0.9).alias("p90_d"),
            ]
        )
        .sort("started_as_draft", descending=True)
    )
    ttm_by_draft
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4b. WIP-label at open

    `had_wip_label_at_open` flags PRs whose first `LABELED(WIP)` event
    fired within 10 minutes of `gh_created_at` — the mathlib4
    label-driven analog of `started_as_draft`. The two cuts are
    parallel but not redundant: `started_as_draft` captures
    GitHub-native draft state, `had_wip_label_at_open` captures the
    project-specific "not yet ready for review" convention. The
    `WIP`-LABELED gap distribution is bimodal — most applications
    fire within seconds (~70 % under 1 min), with a long tail of PRs
    converted to WIP later. The 10-minute cutoff captures the front
    mode cleanly.
    """)
    return


@app.cell
def _(events, expr_merged_to_master, pl, prs):
    """Outcome funnel by WIP-at-open (same shape as the draft funnel)."""
    _mm_prs = (
        events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == "maintainer-merge")
        )
        .select("pull_request_id")
        .unique()
    )
    wip_funnel = (
        prs.with_columns(
            [
                expr_merged_to_master().alias("_merged"),
                ((pl.col("state") == "closed") & ~expr_merged_to_master()).alias(
                    "_abandoned"
                ),
                (pl.col("state") == "open").alias("_open"),
                pl.col("id").is_in(_mm_prs["pull_request_id"]).alias("_reviewed"),
            ]
        )
        .group_by("had_wip_label_at_open")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("_merged").sum().alias("merged"),
                pl.col("_abandoned").sum().alias("abandoned"),
                pl.col("_open").sum().alias("open"),
                pl.col("_reviewed").sum().alias("reviewed"),
            ]
        )
        .with_columns(
            [
                (pl.col("merged") / pl.col("n")).alias("merged_rate"),
                (pl.col("abandoned") / pl.col("n")).alias("abandoned_rate"),
                (pl.col("reviewed") / pl.col("n")).alias("reviewed_rate"),
            ]
        )
        .sort("had_wip_label_at_open", descending=True)
    )
    wip_funnel
    return


@app.cell
def _(merged_prs, pl):
    """TTM percentiles by WIP-at-open, with cohort sizes."""
    ttm_by_wip = (
        merged_prs.group_by("had_wip_label_at_open")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("ttm_days").median().alias("median_d"),
                pl.col("ttm_days").quantile(0.75).alias("p75_d"),
                pl.col("ttm_days").quantile(0.9).alias("p90_d"),
            ]
        )
        .sort("had_wip_label_at_open", descending=True)
    )
    ttm_by_wip
    return


@app.cell
def _(merged_prs, pl):
    """Overlap matrix: how much do `started_as_draft` and
    `had_wip_label_at_open` agree on merged PRs? A 2x2 helps read the
    "is this just a relabel of draft?" question. Empirically on the
    current artifact: only ~5 % of WIP-at-open merged PRs also started
    as draft, and only ~13 % of started-as-draft merged PRs also had
    WIP-at-open. The two cuts capture largely distinct populations."""
    wip_vs_draft = (
        merged_prs.group_by(["started_as_draft", "had_wip_label_at_open"])
        .agg(pl.len().alias("n"))
        .sort(["started_as_draft", "had_wip_label_at_open"], descending=[True, True])
    )
    wip_vs_draft
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. PR type (title prefix)

    Conventional-commit-style prefix on the PR title, after stripping
    the `[Merged by Bors] - ` that bors adds at merge. Empirical
    distribution (current artifact): `feat` ~52 %, `chore` ~31 %, then
    a long tail of `refactor` / `fix` / `doc` / `perf` / `ci` / `style` /
    `test` plus `other` (parsed but non-canonical like `experiment`
    or `wip`) and `unparsed` (free-form titles, ~3.5 %).

    The natural questions: do `chore:` PRs (often deps bumps, file
    moves, port chores) merge faster than `feat:` PRs of similar size?
    Does `fix:` get reviewed at a higher rate than `feat:`? Are the
    `unparsed` PRs systematically slower (i.e. is following the
    convention a useful signal of intentful authorship)?
    """)
    return


@app.cell
def _(DEFAULT_PR_TYPES):
    """Canonical display order used by every cell in this section so
    tables and plots line up the same way."""
    pr_type_order_list = list(DEFAULT_PR_TYPES) + ["other", "unparsed"]
    return (pr_type_order_list,)


@app.cell
def _(pl, pr_type_order_list, prs):
    """Per-type cohort sizes by state across the full prs frame."""
    type_funnel = (
        prs.group_by("pr_type")
        .agg(
            [
                pl.len().alias("n"),
                (pl.col("state") == "open").sum().alias("open"),
                (pl.col("state") == "closed").sum().alias("closed"),
                (pl.col("state") == "merged").sum().alias("merged_gh_ui"),
            ]
        )
        .with_columns(pl.col("pr_type").cast(pl.Enum(pr_type_order_list)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    type_funnel
    return


@app.cell
def _(events, expr_merged_to_master, pl, pr_type_order_list, prs):
    """Merge-rate and reviewed-rate by PR type.

    "Reviewed" = received at least one `LABELED(maintainer-merge)` event
    (same definition as Sections 2 and 4).
    """
    _mm_prs = (
        events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == "maintainer-merge")
        )
        .select("pull_request_id")
        .unique()
    )
    type_rates = (
        prs.with_columns(
            [
                expr_merged_to_master().alias("_merged"),
                ((pl.col("state") == "closed") & ~expr_merged_to_master()).alias(
                    "_abandoned"
                ),
                pl.col("id").is_in(_mm_prs["pull_request_id"]).alias("_reviewed"),
            ]
        )
        .group_by("pr_type")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("_merged").sum().alias("merged"),
                pl.col("_abandoned").sum().alias("abandoned"),
                pl.col("_reviewed").sum().alias("reviewed"),
            ]
        )
        .with_columns(
            [
                (pl.col("merged") / pl.col("n")).alias("merged_rate"),
                (pl.col("abandoned") / pl.col("n")).alias("abandoned_rate"),
                (pl.col("reviewed") / pl.col("n")).alias("reviewed_rate"),
            ]
        )
        .with_columns(pl.col("pr_type").cast(pl.Enum(pr_type_order_list)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    type_rates
    return (type_rates,)


@app.cell
def _(np, plt, type_rates):
    """Grouped-bar chart of merged / reviewed / abandoned rates by PR type.
    Types with `n < 50` are still shown but flagged via the n-label above
    each cluster — useful for spotting which buckets the rate is noisy in."""
    _rows = list(type_rates.iter_rows(named=True))
    _labels = [r["pr_type"] for r in _rows]
    _metrics = ["merged_rate", "reviewed_rate", "abandoned_rate"]
    _colors = ["#4c9", "#39c", "#c63"]
    _x = np.arange(len(_labels))
    _w = 0.25
    _fig, _ax = plt.subplots(figsize=(11, 4.5))
    for _i, (_m, _c) in enumerate(zip(_metrics, _colors)):
        _vals = [r[_m] for r in _rows]
        _ax.bar(_x + (_i - 1) * _w, _vals, _w, label=_m.replace("_rate", ""), color=_c)
    _ax.set_xticks(_x, _labels, rotation=30, ha="right")
    _ax.set_ylim(0, 1)
    _ax.set_ylabel("Share of cohort")
    _ax.set_title("Outcome rates by PR type")
    _ax.legend(loc="upper right")
    _ax.grid(axis="y", alpha=0.3)
    for _i, _r in enumerate(_rows):
        _ax.text(
            _i,
            1.02,
            f"n={_r['n']:,}",
            ha="center",
            fontsize=8,
            transform=_ax.get_xaxis_transform(),
        )
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(merged_prs, pl, pr_type_order_list):
    """Per-type TTM percentiles (merged-to-master only)."""
    ttm_by_type = (
        merged_prs.group_by("pr_type")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("ttm_days").median().alias("median_d"),
                pl.col("ttm_days").quantile(0.75).alias("p75_d"),
                pl.col("ttm_days").quantile(0.9).alias("p90_d"),
            ]
        )
        .with_columns(pl.col("pr_type").cast(pl.Enum(pr_type_order_list)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    ttm_by_type
    return


@app.cell
def _(merged_prs, pl, plt, pr_type_order_list):
    """TTM box plot by PR type. Clipped to <=60d for readability; the
    table above carries the unclipped percentiles."""
    _data = [
        merged_prs.filter(pl.col("pr_type") == _t)["ttm_days"].to_numpy()
        for _t in pr_type_order_list
    ]
    _fig, _ax = plt.subplots(figsize=(11, 4.5))
    _ax.boxplot(
        [_d[_d <= 60] for _d in _data],
        tick_labels=pr_type_order_list,
        showfliers=False,
        widths=0.6,
    )
    _ax.set_ylabel("Time to merge (days, ≤60d shown)")
    _ax.set_xlabel("PR type")
    _ax.set_title("TTM by PR type")
    plt.setp(_ax.get_xticklabels(), rotation=30, ha="right")
    _ax.grid(axis="y", alpha=0.3)
    _ns = [len(_d) for _d in _data]
    for _i, _n in enumerate(_ns):
        _ax.text(_i + 1, _ax.get_ylim()[1] * 0.95, f"n={_n}", ha="center", fontsize=8)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(merged_prs, pl, pr_type_order_list):
    """Type × lines-bucket cross-tab on merged PRs. Useful sanity check:
    `chore` skews small and `refactor`/`feat` skew larger, so per-type
    TTM differences should be partly explained by size mix."""
    type_size_mix = (
        merged_prs.group_by(["pr_type", "lines_bucket"])
        .agg(pl.len().alias("n"))
        .pivot(on="lines_bucket", index="pr_type", values="n")
        .fill_null(0)
        .with_columns(pl.col("pr_type").cast(pl.Enum(pr_type_order_list)).alias("_o"))
        .sort("_o")
        .drop("_o")
    )
    type_size_mix
    return


@app.cell
def _(mo):
    mo.md("""
    ## Notes

    - "First-time" author is keyed off the dataset snapshot, not the
      GitHub-wide history. An author who first appeared in mathlib4
      in 2021 is treated as "returning" on their 2025 PR; an author
      whose first mathlib4 PR is this week is "first-time" even if
      they have years of OSS history elsewhere.
    - The author-sequence bin (`1`, `2-5`, `6-20`, `21+`) is the
      closest thing to "early-career-in-mathlib4". Use it instead of
      `is_first_pr` if you want the trend across the first 20 PRs
      rather than a binary split.
    - Reviewer attribution uses the same 10-minute window as Theme 2.
      Unattributed `maintainer-merge` events (~1-2%) drop out of the
      Lorenz / Gini computation entirely.
    - Started-as-draft uses `READY_FOR_REVIEW` / `CONVERT_TO_DRAFT`
      events when available, falling back to the `prs.is_draft`
      snapshot for PRs without any draft-state events. The fallback
      mostly catches currently-open drafts that have never been
      marked ready - these are correctly flagged as
      `started_as_draft=True`.
    - Time-to-first-court-exit excludes PRs whose first court
      interval is still open at `asof` (so the metric stays well
      defined). Bigger PRs have more open first intervals than
      smaller ones; the box-plot Ns reflect that selection.
    - WIP-label start is now broken out as Section 4b via
      `had_wip_label_at_open`. It anchors on the *first*
      `LABELED(WIP)` event within 10 minutes of `gh_created_at` —
      enough to capture the front mode of the bimodal apply-time
      distribution without false-positive on PRs that drift into WIP
      later. The 2×2 with `started_as_draft` shows the two cuts are
      largely orthogonal on mathlib4 (only ~5 % overlap on merged
      PRs); they capture distinct populations rather than relabels
      of each other.
    """)
    return


if __name__ == "__main__":
    app.run()
