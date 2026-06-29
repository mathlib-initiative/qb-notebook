"""Story A — Anatomy of a merge."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
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
        # parquet). plotly backs the lifecycle Sankey (pure-Python wheel;
        # kaleido — its PNG-export backend — has no Pyodide build, so the
        # offline export cells are guarded behind `not is_wasm` below).
        # tzdata: Pyodide ships no system zoneinfo database, so any
        # materialization of tz-aware datetimes (e.g. `.to_dicts()` on a
        # UTC column) raises ZoneInfoNotFoundError until this is installed.
        _ = await _micropip.install(
            [
                "polars",
                "pandas",
                "numpy",
                "pyarrow",
                "matplotlib",
                "scipy",
                "pyyaml",
                "plotly",
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


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Anatomy of a merge

    For each PR in the selected cohort we reconstruct a milestone
    timeline and route every PR through a lifecycle Sankey. The
    segments, in roughly chronological order:

    1. **opened** — `gh_created_at`.
    2. **1 queue cycle** — at least one review round completed;
       underlying milestone is `first_touch_at` (earliest non-author,
       non-bot `REVIEW_*` / `ISSUE_COMMENTED` event). PRs that never
       receive a review or comment exit directly from `opened` to a
       terminal state.
    3. **2 queue cycles** / **3+ queue cycles** — extra cycle nodes
       inserted for PRs that needed a second or third review round
       before `maintainer-merge` (`n_queue_cycles_before_mm`). PRs
       signed off after one round exit straight from `1 queue cycle`.
    4. **maintainer-merge** — first `LABELED(maintainer-merge)`
       (~99 % attributed to a human via the bot-trigger heuristic).
    5. **bors r+** — first `LABELED(ready-to-merge)`; bors has
       accepted the PR for the queue.
    6. **delegated** — PR ever carried the `delegated` label; takes
       precedence over `bors r+` in the Sankey routing when both apply.
    7. **merged** / **closed** / **open** — terminal states from the
       bors-aware effective merge timestamp
       (`expr_merged_at_effective`), closure without a merge, or
       still-open at snapshot time.

    For each stage we then plot the duration distribution on
    log-spaced bins — PR open-durations are close to log-normal (see
    `pr_open_durations.ipynb`), so log binning + lognormal-fit
    overlays make the shape readable.

    All cohort-dependent cells refire on the dropdown change.
    """)
    return


@app.cell(hide_code=True)
def _(is_wasm):
    import sys
    from pathlib import Path

    if not is_wasm:
        # `__file__` is undefined in the WASM runtime; only needed to find the
        # repo root for the local kernel (where qb_notebook lives on disk).
        _repo_root = Path(__file__).resolve().parents[1]
        if str(_repo_root) not in sys.path:
            sys.path.insert(0, str(_repo_root))

    import json
    import re
    from datetime import datetime, timezone

    import matplotlib.pyplot as plt
    import numpy as np
    import plotly.graph_objects as go
    import polars as pl
    from scipy.optimize import minimize
    from scipy.stats import lognorm, nbinom

    import plotly.io as pio

    # The Pyodide/WASM runtime has no default plotly renderer, so
    # `pio.renderers.default` is "" and `pio.renderers[""]` raises KeyError.
    # The modebar PNG-scale tweak only applies to the local kernel anyway.
    if pio.renderers.default:
        pio.renderers[pio.renderers.default].config = {
            "toImageButtonOptions": {"format": "png", "scale": 3}
        }
    # Match the 3× plotly modebar PNG resolution for matplotlib outputs
    # (default dpi is 100). marimo scales the displayed image width
    # inversely so the figure looks the same on screen but the
    # right-click-saved PNG is at the higher pixel density.
    plt.rcParams["figure.dpi"] = 300

    from qb_notebook.data_io import load_pr_interval_data
    from qb_notebook.filters import (
        expr_merged_at_effective,
        expr_merged_to_master,
    )
    from qb_notebook.pr_shape import (
        DEFAULT_LINES_BREAKS,
        author_cohort,
        bucket_labels,
        pr_type,
        pr_type_order,
        size_buckets,
    )
    from qb_notebook.review_states import (
        DEFAULT_BOT_ACTORS,
        MATHLIB_LABEL_RETIRED_AT,
        inline_comment_stats,
        label_intervals,
        labels_active_at,
        pipeline_stages,
        queue_window_intervals,
    )
    from qb_notebook.wasm_io import load_slimmed_data

    return (
        DEFAULT_BOT_ACTORS,
        DEFAULT_LINES_BREAKS,
        MATHLIB_LABEL_RETIRED_AT,
        Path,
        author_cohort,
        bucket_labels,
        datetime,
        expr_merged_at_effective,
        expr_merged_to_master,
        go,
        inline_comment_stats,
        json,
        label_intervals,
        labels_active_at,
        load_pr_interval_data,
        load_slimmed_data,
        lognorm,
        minimize,
        nbinom,
        np,
        pipeline_stages,
        pl,
        plt,
        pr_type,
        pr_type_order,
        queue_window_intervals,
        re,
        size_buckets,
        timezone,
    )


@app.cell(hide_code=True)
def _(
    Path,
    is_wasm,
    load_pr_interval_data,
    load_slimmed_data,
    mo,
    pl,
):
    """Load parquet + join `core_user` so PRs carry `author_login`. In WASM the
    tables (incl. core_user) ship slimmed under public/ (see
    scripts/export_wasm_data.py); locally they load from the raw data dir and
    core_user is read directly off disk (not a load_pr_interval_data key)."""
    if is_wasm:
        data = load_slimmed_data(str(mo.notebook_location() / "public"))
        _users = data["core_user"]
    else:
        _data_dir = Path(__file__).resolve().parents[1] / "data"
        data = load_pr_interval_data(_data_dir)
        _users = pl.read_parquet(_data_dir / "core_user.parquet")
    events = data["events"]
    prs_raw = data["prs"]
    queue_windows = data["queue_windows"]
    inline_comments = data.get("inline_comments")

    users = _users.select(
        pl.col("id").alias("author_id"),
        pl.col("github_login").alias("author_login"),
    )
    # Snapshot time, not wall-clock now(): these notebooks read a frozen data
    # snapshot (especially the WASM export), so anchoring relative windows to a
    # live clock drifts past the last observed event and empties every "last N
    # days" window. `events.occurred_at` is the latest column surviving slimming
    # and bounds the other timestamps. Per AGENTS.md, anchor windows to max(date).
    asof = events["occurred_at"].max()
    return asof, events, inline_comments, prs_raw, queue_windows, users


@app.cell(hide_code=True)
def _(
    DEFAULT_LINES_BREAKS,
    asof,
    author_cohort,
    bucket_labels,
    expr_merged_at_effective,
    expr_merged_to_master,
    pl,
    pr_type,
    prs_raw,
    size_buckets,
    users,
):
    """Filter to human PRs against `master`, attach shape + cohort columns,
    and derive a bors-aware `merged_at_effective` (null for non-merges)."""
    _MATHLIB_REPO_ID = 1
    prs_enriched = (
        prs_raw.filter(pl.col("repository_id") == _MATHLIB_REPO_ID)
        .filter(pl.col("base_ref_name") == "master")
        .join(users, on="author_id", how="left")
        .with_columns(
            pl.when(expr_merged_to_master())
            .then(expr_merged_at_effective())
            .otherwise(None)
            .alias("merged_at_effective"),
        )
    )
    prs_enriched = size_buckets(prs_enriched)
    prs_enriched = pr_type(prs_enriched)
    prs_enriched = author_cohort(prs_enriched)
    prs_enriched = prs_enriched.with_columns(
        pl.col("merged_at_effective").is_not_null().alias("is_merged"),
        pl.col("closed_at").is_not_null().alias("is_closed"),
    ).with_columns(
        pl.when(pl.col("is_merged"))
        .then(pl.lit("merged"))
        .when(pl.col("is_closed"))
        .then(pl.lit("closed_unmerged"))
        .otherwise(pl.lit("still_open"))
        .alias("outcome"),
    )

    lines_bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    _ = asof  # threading asof through so downstream cells can depend on it
    return lines_bucket_order, prs_enriched


@app.cell(hide_code=True)
def _(
    MATHLIB_LABEL_RETIRED_AT,
    asof,
    events,
    label_intervals,
    pl,
    prs_enriched,
):
    """`t-*` label intervals over the full timeline (open intervals run to
    `asof`). We do *not* clamp at PR close: GitHub doesn't auto-remove
    labels on close, and the half-open `[start, end_effective)` lookup in
    `labels_active_at` would miss merges whose `merged_at_effective`
    equals the PR's `closed_at`. Leaving the intervals open through
    `asof` is the same convention `area_health.py` uses for merge-time
    area attribution."""
    _t_labels = (
        events.filter(pl.col("type") == "LABELED")
        .filter(pl.col("label_name").str.starts_with("t-"))
        .select("label_name")
        .unique()
        .get_column("label_name")
        .to_list()
    )
    t_intervals = label_intervals(
        events,
        _t_labels,
        asof=asof,
        label_asof_overrides=MATHLIB_LABEL_RETIRED_AT,
    )
    _ = prs_enriched  # kept in signature for cohort wiring; cell uses asof only
    return (t_intervals,)


@app.cell(hide_code=True)
def _(mo):
    """Cohort dropdown. Post-MM is the default since the `maintainer-merge`
    label only exists from 2024-02-15, so the headline funnel is only
    meaningful for PRs whose lifecycle could have included that stage.
    All-time is included for the long-baseline comparison."""
    cohort = mo.ui.dropdown(
        options={
            "post-MM (2024-02-15+)": "post_mm",
            "pre-MM (pre-2024-02-15)": "pre_mm",
            "pre-bors (pre-2022-08-01)": "pre_bors",
            "bors → pre-MM (2022-08-01 to 2024-02-15)": "bors_pre_mm",
            "2024 (post-MM)": "2024",
            "2025": "2025",
            "all-time": "all",
        },
        value="post-MM (2024-02-15+)",
        label="Cohort",
    )

    show_cycle_branches = mo.ui.checkbox(
        value=True,
        label="Sankey: insert sequential queue-cycle nodes after `1 queue cycle`",
    )
    # Shared height for §1 and §1b Sankeys. Plotly Sankey has no
    # native scroll-zoom (no zoomable axes), so a slider is the
    # interactive way to fit the figure inside the cell output.
    sankey_height = mo.ui.slider(
        start=380,
        stop=1000,
        step=20,
        value=520,
        label="Sankey height (px)",
        show_value=True,
    )
    mo.hstack([cohort, show_cycle_branches, sankey_height])
    return cohort, sankey_height, show_cycle_branches


@app.cell(hide_code=True)
def _(pr_type_order, t_intervals):
    """Filter-option menus for the topic-label and PR-type pickers.
    Sourced from the full mathlib history (not the current cohort) so
    the menus don't shrink as the user narrows the date range.

    `TOPIC_NONE` is a pseudo-bucket for PRs that never carried any t-*
    label — uncheck it to exclude unlabeled PRs from the cohort, leave
    it checked alongside specific labels to keep them in."""
    TOPIC_NONE = "(none)"
    available_topics = sorted(
        t_intervals.get_column("label_name").unique().to_list()
    ) + [TOPIC_NONE]
    available_pr_types = pr_type_order()
    return TOPIC_NONE, available_pr_types, available_topics


@app.cell(hide_code=True)
def _(mo):
    """All/None buttons for the topic filter. Clicking either rebuilds
    the checkbox grid below with new defaults — each button is wired as
    a click counter (`value=0` + `on_click=lambda v: v + 1`), and the
    downstream cell compares the two to pick the winning default
    (ties resolve to All)."""
    topic_all_btn = mo.ui.button(label="All topics", value=0, on_click=lambda v: v + 1)
    topic_none_btn = mo.ui.button(label="None", value=0, on_click=lambda v: v + 1)
    return topic_all_btn, topic_none_btn


@app.cell(hide_code=True)
def _(available_topics, mo, topic_all_btn, topic_none_btn):
    """Topic-label checkbox grid. A PR passes the filter iff it ever
    carried at least one of the checked t-* labels (plus the unlabeled
    bucket if `(none)` is checked). Default is All so the page renders
    with the same cohort as before filters existed."""
    _default = topic_all_btn.value >= topic_none_btn.value
    topic_checks = mo.ui.array(
        [mo.ui.checkbox(value=_default, label=lbl) for lbl in available_topics]
    )
    return (topic_checks,)


@app.cell(hide_code=True)
def _(available_topics, mo, topic_all_btn, topic_checks, topic_none_btn):
    """Render the topic filter. Lives in a separate cell from
    `topic_checks` because marimo forbids reading a UIElement's
    `.value` in the cell that created it (needed here for the
    `N/M selected` header count)."""
    _n_selected = sum(1 for v in topic_checks.value if v)
    mo.accordion(
        {
            f"Topic filter ({_n_selected}/{len(available_topics)} selected)": mo.vstack(
                [
                    mo.hstack(
                        [topic_all_btn, topic_none_btn],
                        justify="start",
                    ),
                    mo.hstack(list(topic_checks), wrap=True, justify="start"),
                ]
            )
        }
    )
    return


@app.cell(hide_code=True)
def _(mo):
    """All/None buttons for the PR-type filter. Same click-counter
    pattern as the topic buttons."""
    pr_type_all_btn = mo.ui.button(label="All types", value=0, on_click=lambda v: v + 1)
    pr_type_none_btn = mo.ui.button(label="None", value=0, on_click=lambda v: v + 1)
    return pr_type_all_btn, pr_type_none_btn


@app.cell(hide_code=True)
def _(available_pr_types, mo, pr_type_all_btn, pr_type_none_btn):
    """PR-type checkbox grid (parsed conventional-commit prefix)."""
    _default = pr_type_all_btn.value >= pr_type_none_btn.value
    pr_type_checks = mo.ui.array(
        [mo.ui.checkbox(value=_default, label=t) for t in available_pr_types]
    )
    return (pr_type_checks,)


@app.cell(hide_code=True)
def _(
    available_pr_types,
    mo,
    pr_type_all_btn,
    pr_type_checks,
    pr_type_none_btn,
):
    """Render the PR-type filter. Split from `pr_type_checks` for the
    same reason as the topic filter — see that cell."""
    _n_selected = sum(1 for v in pr_type_checks.value if v)
    mo.accordion(
        {
            f"PR-type filter ({_n_selected}/{len(available_pr_types)} selected)": mo.vstack(
                [
                    mo.hstack(
                        [pr_type_all_btn, pr_type_none_btn],
                        justify="start",
                    ),
                    mo.hstack(list(pr_type_checks), wrap=True, justify="start"),
                ]
            )
        }
    )
    return


@app.cell(hide_code=True)
def _(
    TOPIC_NONE,
    available_pr_types,
    available_topics,
    cohort,
    datetime,
    pl,
    pr_type_checks,
    prs_enriched,
    t_intervals,
    timezone,
    topic_checks,
):
    """Apply the cohort + topic + PR-type filters to `prs_enriched`.
    Date filter is on `gh_created_at`; topic filter keeps PRs whose ID
    appears in `t_intervals` under any selected t-* label, and unions
    in the unlabeled bucket when `TOPIC_NONE` is checked; PR-type
    filter is a column-level `is_in` on the parsed conventional prefix.
    If a sub-filter has nothing selected, the cohort collapses to
    empty — that matches the literal "no PRs match" reading."""
    _COHORT_BOUNDS = {
        "post_mm": (datetime(2024, 2, 15, tzinfo=timezone.utc), None),
        "pre_mm": (None, datetime(2024, 2, 15, tzinfo=timezone.utc)),
        "pre_bors": (None, datetime(2022, 8, 1, tzinfo=timezone.utc)),
        "bors_pre_mm": (
            datetime(2022, 8, 1, tzinfo=timezone.utc),
            datetime(2024, 2, 15, tzinfo=timezone.utc),
        ),
        "2024": (
            datetime(2024, 2, 15, tzinfo=timezone.utc),
            datetime(2025, 1, 1, tzinfo=timezone.utc),
        ),
        "2025": (
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        "all": (None, None),
    }
    _lo, _hi = _COHORT_BOUNDS[cohort.value]
    _expr = pl.lit(True)
    if _lo is not None:
        _expr = _expr & (pl.col("gh_created_at") >= _lo)
    if _hi is not None:
        _expr = _expr & (pl.col("gh_created_at") < _hi)
    prs_cohort = prs_enriched.filter(_expr)

    _selected_topics = [t for t, v in zip(available_topics, topic_checks.value) if v]
    if len(_selected_topics) < len(available_topics):
        _selected_real = [t for t in _selected_topics if t != TOPIC_NONE]
        _none_selected = TOPIC_NONE in _selected_topics
        _real_ids = (
            t_intervals.filter(pl.col("label_name").is_in(_selected_real))
            .get_column("pull_request_id")
            .unique()
        )
        _keep_expr = pl.col("id").is_in(_real_ids)
        if _none_selected:
            _all_t_ids = t_intervals.get_column("pull_request_id").unique()
            _keep_expr = _keep_expr | ~pl.col("id").is_in(_all_t_ids)
        prs_cohort = prs_cohort.filter(_keep_expr)

    _selected_types = [t for t, v in zip(available_pr_types, pr_type_checks.value) if v]
    if len(_selected_types) < len(available_pr_types):
        prs_cohort = prs_cohort.filter(pl.col("pr_type").is_in(_selected_types))

    cohort_label = cohort.value
    return cohort_label, prs_cohort


@app.cell(hide_code=True)
def _(
    DEFAULT_BOT_ACTORS,
    asof,
    events,
    pipeline_stages,
    pl,
    prs_cohort,
    queue_window_intervals,
    queue_windows,
):
    """Per-PR pipeline frame + queue-cycle count before MM + delegated flag.

    `pipeline_stages` already handles first-touch, MM, RTM, and merged
    timestamps with non-monotonic-safe deltas. We layer on:

    - `first_delegated_at` from `delegated` LABELED events (the third
      sign-off path alongside MM and direct bors r+).
    - `n_queue_cycles_before_mm` from `queue_window_intervals` —
      number of queue windows that opened before MM (or total cycles if
      the PR never reached MM).
    """
    pipeline = pipeline_stages(
        prs_cohort,
        events,
        pr_merged_col="merged_at_effective",
    )

    _deleg_first = (
        events.filter(
            (pl.col("type") == "LABELED") & (pl.col("label_name") == "delegated")
        )
        .group_by("pull_request_id")
        .agg(pl.col("occurred_at").min().alias("first_delegated_at"))
    )

    _qw = queue_window_intervals(queue_windows, asof=asof)
    _qw_per_pr = (
        _qw.join(
            pipeline.select("pull_request_id", "first_maintainer_merge_at"),
            on="pull_request_id",
            how="inner",
        )
        .with_columns(
            (
                pl.col("first_maintainer_merge_at").is_null()
                | (pl.col("start") < pl.col("first_maintainer_merge_at"))
            ).alias("_before_mm"),
        )
        .group_by("pull_request_id")
        .agg(
            pl.col("_before_mm").sum().cast(pl.Int64).alias("n_queue_cycles_before_mm"),
            pl.len().cast(pl.Int64).alias("n_queue_cycles_total"),
        )
    )

    pipeline = (
        pipeline.join(_deleg_first, on="pull_request_id", how="left")
        .join(_qw_per_pr, on="pull_request_id", how="left")
        .with_columns(
            pl.col("n_queue_cycles_before_mm").fill_null(0),
            pl.col("n_queue_cycles_total").fill_null(0),
            pl.col("first_delegated_at").is_not_null().alias("had_delegated"),
        )
    )

    _ = DEFAULT_BOT_ACTORS  # exported by review_states; not used here directly
    return (pipeline,)


@app.cell(hide_code=True)
def _(pipeline, pl, prs_cohort):
    """Wide per-PR frame: pipeline + shape/area-friendly attributes +
    terminal-state classification used downstream by the Sankey, the
    slice tables, and path classification."""

    _pr_attrs = prs_cohort.select(
        pl.col("id").alias("pull_request_id"),
        "number",
        "title",
        "lines_changed",
        "lines_bucket",
        "files_bucket",
        "pr_type",
        "is_first_pr",
        "outcome",
        "is_merged",
        "is_closed",
        "additions",
        "deletions",
        "changed_files_count",
    )

    pr_pipeline = pipeline.join(_pr_attrs, on="pull_request_id", how="inner")

    pr_pipeline = pr_pipeline.with_columns(
        # Terminal state for the Sankey + path table.
        pl.when(pl.col("is_merged"))
        .then(pl.lit("merged"))
        .when(pl.col("is_closed"))
        .then(pl.lit("closed_unmerged"))
        .otherwise(pl.lit("still_open"))
        .alias("terminal"),
        # Successful merges: classify by which sign-off path the PR took.
        pl.when(~pl.col("is_merged"))
        .then(pl.lit(None, dtype=pl.String))
        .when(pl.col("had_delegated"))
        .then(pl.lit("delegated"))
        .when(pl.col("first_ready_to_merge_at").is_not_null())
        .then(pl.lit("bors"))
        .when(pl.col("first_maintainer_merge_at").is_not_null())
        .then(pl.lit("mm_no_rtm"))
        .otherwise(pl.lit("direct"))
        .alias("merge_path"),
        # Queue-cycle bucket for the optional Sankey branches.
        pl.when(pl.col("n_queue_cycles_before_mm") <= 1)
        .then(pl.lit("1"))
        .when(pl.col("n_queue_cycles_before_mm") == 2)
        .then(pl.lit("2"))
        .otherwise(pl.lit("3+"))
        .alias("cycle_bucket"),
    )
    return (pr_pipeline,)


@app.cell(hide_code=True)
def _(labels_active_at, pl, pr_pipeline, t_intervals):
    """Attribute merged PRs to the `t-*` label active at their merge time.
    Each PR can have multiple active `t-*` labels (e.g. `t-algebra` +
    `t-category-theory`); we keep all assignments so per-area counts
    sum to ≥ merged-count, then derive a single canonical area
    (alphabetically first) per PR for slice tables that need 1 row / PR."""
    _points = pr_pipeline.filter(pl.col("is_merged")).select(
        "pull_request_id",
        pl.col("merged_at_effective").alias("at"),
    )
    pr_topic_long = labels_active_at(
        t_intervals,
        _points,
        point_time_col="at",
    ).select(
        "pull_request_id",
        pl.col("label_name").alias("topic_area"),
    )
    pr_topic_one = pr_topic_long.sort(["pull_request_id", "topic_area"]).unique(
        subset=["pull_request_id"], keep="first"
    )
    return (pr_topic_one,)


@app.cell(hide_code=True)
def _(cohort_label, mo, pl, pr_pipeline):
    """Cohort summary table — milestone counts + drop-off percentages."""
    _n = pr_pipeline.height
    _n_touched = pr_pipeline.filter(pl.col("first_touch_at").is_not_null()).height
    _n_mm = pr_pipeline.filter(pl.col("first_maintainer_merge_at").is_not_null()).height
    _n_rtm = pr_pipeline.filter(pl.col("first_ready_to_merge_at").is_not_null()).height
    _n_deleg = pr_pipeline.filter(pl.col("had_delegated")).height
    _n_merged = pr_pipeline.filter(pl.col("is_merged")).height
    _n_closed = pr_pipeline.filter(pl.col("terminal") == "closed_unmerged").height
    _n_open = pr_pipeline.filter(pl.col("terminal") == "still_open").height

    def _pct(num: int) -> str:
        return f"{100 * num / max(_n, 1):.1f}%"

    cohort_summary = pl.DataFrame(
        [
            {
                "milestone": "opened (cohort total)",
                "count": _n,
                "pct_of_cohort": "100.0%",
            },
            {
                "milestone": "first non-author review",
                "count": _n_touched,
                "pct_of_cohort": _pct(_n_touched),
            },
            {
                "milestone": "maintainer-merge applied",
                "count": _n_mm,
                "pct_of_cohort": _pct(_n_mm),
            },
            {
                "milestone": "ready-to-merge applied",
                "count": _n_rtm,
                "pct_of_cohort": _pct(_n_rtm),
            },
            {
                "milestone": "delegated label applied",
                "count": _n_deleg,
                "pct_of_cohort": _pct(_n_deleg),
            },
            {
                "milestone": "merged (terminal)",
                "count": _n_merged,
                "pct_of_cohort": _pct(_n_merged),
            },
            {
                "milestone": "closed unmerged (terminal)",
                "count": _n_closed,
                "pct_of_cohort": _pct(_n_closed),
            },
            {
                "milestone": "still open (terminal)",
                "count": _n_open,
                "pct_of_cohort": _pct(_n_open),
            },
        ]
    )
    mo.md(f"### Cohort `{cohort_label}` — milestone counts")
    cohort_summary
    return (cohort_summary,)


@app.cell(disabled=True, hide_code=True)
def _(mo):
    mo.md("""
    ## 1. Lifecycle Sankey

    Each PR is classified into a single path through the milestone nodes
    based on whichever stages it reached and its terminal state. The
    `bors` vs `delegated` split runs from the `maintainer-merge` node
    forward: a PR with the `delegated` label ever applied is routed via
    the delegated node even if it also got `ready-to-merge` (typical:
    delegated → author runs `bors r+` themselves → RTM applied → merge).

    Toggle the checkbox above to insert "needed-another-round" nodes
    after the `1 queue cycle` node. `1 queue cycle` itself is the
    PR's first review cycle (entered when a non-author / non-bot first
    reviews or comments — the underlying milestone is `first_touch_at`),
    so the extra cycle nodes are only emitted when a PR's
    `n_queue_cycles_before_mm` was 2 or 3+. The link
    `1 queue cycle → 2 queue cycles` reads as "needed another review
    round"; the link `1 queue cycle → MM` reads as "got signed off
    after one round".
    """)
    return


@app.cell(disabled=True, hide_code=True)
def _(go, pr_pipeline, sankey_height, show_cycle_branches):
    """Build the Sankey from per-PR path classifications.

    Each PR contributes one path = sequence of nodes; the function below
    accumulates per-edge counts and hands them to plotly. The cycle-branch
    toggle inserts "2 queue cycles" / "3+ queue cycles" nodes after
    `1 queue cycle` for PRs that needed more than one review round —
    `1 queue cycle` itself (the first-review milestone) stands in for
    the first cycle, so `1 queue cycle → MM` means "got signed off
    after one round" and `1 queue cycle → 2 queue cycles` means
    "needed another round".
    """
    show_cycles = bool(show_cycle_branches.value)

    def _path_for(row: dict) -> list[str]:
        steps = ["opened"]
        if row["first_touch_at"] is not None:
            steps.append("1 queue cycle")
            if show_cycles:
                # `1 queue cycle` is the implicit first-cycle node, so
                # the extra cycle nodes only appear when the PR needed
                # more than one round. Bucket "2" emits one extra node,
                # bucket "3+" emits both. Side exits from each cycle
                # node to MM / bors r+ / delegated / terminal are
                # picked up by the steps appended below.
                _bucket = row["cycle_bucket"]
                if _bucket in ("2", "3+"):
                    steps.append("2 queue cycles")
                if _bucket == "3+":
                    steps.append("3+ queue cycles")
        if row["first_maintainer_merge_at"] is not None:
            steps.append("maintainer-merge")
        # `delegated` and `bors r+` nodes are visited based on the labels
        # alone (independent of MM). Many PRs skip MM and go straight to
        # `bors r+` (no prior `maintainer merge` comment, just `bors r+`).
        if row["had_delegated"]:
            steps.append("delegated")
        elif row["first_ready_to_merge_at"] is not None:
            steps.append("bors r+")
        # Terminal
        if row["terminal"] == "merged":
            steps.append("merged")
        elif row["terminal"] == "closed_unmerged":
            steps.append("closed")
        else:
            steps.append("open")
        return steps

    _rows = pr_pipeline.select(
        "first_touch_at",
        "first_maintainer_merge_at",
        "first_ready_to_merge_at",
        "had_delegated",
        "terminal",
        "cycle_bucket",
    ).to_dicts()

    _edge_counts: dict[tuple[str, str], int] = {}
    for _r in _rows:
        _steps = _path_for(_r)
        for _a, _b in zip(_steps, _steps[1:]):
            _edge_counts[(_a, _b)] = _edge_counts.get((_a, _b), 0) + 1

    # Stable node ordering — controls the column layout. `1 queue cycle`
    # stands in for the first cycle (underlying milestone:
    # `first_touch_at`); the extra cycle nodes only fire for PRs that
    # needed a second / third round.
    _node_order = [
        "opened",
        "1 queue cycle",
        "2 queue cycles",
        "3+ queue cycles",
        "maintainer-merge",
        "bors r+",
        "delegated",
        "merged",
        "closed",
        "open",
    ]
    _present_nodes = {n for edge in _edge_counts for n in edge}
    _nodes = [n for n in _node_order if n in _present_nodes]
    _node_idx = {n: i for i, n in enumerate(_nodes)}

    _src, _tgt, _val = [], [], []
    for (_a, _b), _v in _edge_counts.items():
        _src.append(_node_idx[_a])
        _tgt.append(_node_idx[_b])
        _val.append(_v)

    _node_colors = {
        "opened": "#888",
        "1 queue cycle": "#4a90d9",
        "2 queue cycles": "#6ea7d8",
        "3+ queue cycles": "#3f7fbe",
        "maintainer-merge": "#c63",
        "bors r+": "#73a946",
        "delegated": "#9d72c7",
        "merged": "#2c7a2c",
        "closed": "#a33",
        "open": "#bbb",
    }

    # Explicit node positions (x = column, y = vertical) to control
    # ribbon paths. Plotly's auto-layout uses a barycenter heuristic
    # that doesn't fully avoid crossings on this topology (cycle nodes
    # fanning to MM + signoff + terminal at varying distances). Pinning
    # cycle nodes near the top keeps "continue to next cycle" ribbons
    # nearly horizontal so the long "exit to MM/bors/delegated" ribbons
    # can drop below them without crossing.
    _NODE_X = {
        "opened": 0.001,
        "1 queue cycle": 0.124,
        "2 queue cycles": 0.307,
        "3+ queue cycles": 0.499,
        "maintainer-merge": 0.620,
        "bors r+": 0.828,
        "delegated": 0.830,
        "merged": 0.999,
        "closed": 0.999,
        "open": 0.999,
    }
    _NODE_Y = {
        "opened": 0.500,
        "1 queue cycle": 0.478,
        "2 queue cycles": 0.507,
        "3+ queue cycles": 0.532,
        "maintainer-merge": 0.085,
        "bors r+": 0.218,
        "delegated": 0.567,
        "merged": 0.310,
        "closed": 0.735,
        "open": 0.853,
    }

    # Sort links so within each source node, ribbons stack top-to-bottom
    # by target y. This pairs with the pinned positions above: Plotly
    # draws ribbons in array order, so sorting here is what actually
    # prevents the cycle→MM and cycle→cycle ribbons from twisting.
    _link_order = sorted(
        range(len(_src)),
        key=lambda i: (
            _NODE_X[_nodes[_src[i]]],
            _NODE_Y[_nodes[_src[i]]],
            _NODE_Y[_nodes[_tgt[i]]],
        ),
    )
    _src = [_src[i] for i in _link_order]
    _tgt = [_tgt[i] for i in _link_order]
    _val = [_val[i] for i in _link_order]

    # Tint each node label to match its segment color and give it a
    # per-node SVG outline (dark for light segments, light for dark).
    # Plotly passes the span `style` attribute through to the SVG
    # `<tspan>` as-is (rewriting `color:` to `fill:`), so SVG-native
    # `stroke` + `paint-order:stroke fill` reliably produces a halo
    # behind the glyphs — `text-shadow` on SVG tspans is ignored by
    # most browsers, and the trace-level `textfont.shadow` is a single
    # value (no per-node array), so neither of those options work here.
    def _halo_color(bg_hex: str) -> str:
        h = bg_hex.lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
        return "black" if luminance > 0.55 else "white"

    _node_html_labels = []
    for _n in _nodes:
        _c = _node_colors.get(_n, "#999")
        _halo = _halo_color(_c)
        _node_html_labels.append(
            f'<span style="color:{_c};stroke:{_halo};stroke-width:4;'
            f'paint-order:stroke fill">{_n}</span>'
        )

    sankey_fig = go.Figure(
        data=[
            go.Sankey(
                # `freeform` lets dragged nodes stay where you put them
                # instead of snapping back to the column grid; combined
                # with the taller figure + extra padding it gives enough
                # room to hand-tune label overlaps in the cycle column.
                arrangement="freeform",
                node=dict(
                    label=_node_html_labels,
                    color=[_node_colors.get(n, "#999") for n in _nodes],
                    x=[_NODE_X[n] for n in _nodes],
                    y=[_NODE_Y[n] for n in _nodes],
                    pad=28,
                    thickness=18,
                ),
                link=dict(source=_src, target=_tgt, value=_val),
            )
        ]
    )
    sankey_fig.update_layout(
        title=dict(
            text="mathlib4 PR lifecycle flow (2024-02-15 to 2026-05-21)",
            font=dict(color="black"),
        ),
        font=dict(size=20),
        height=int(sankey_height.value),
        margin=dict(l=10, r=10, t=60, b=20),
    )
    sankey_fig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 1b. Slide-friendly focus-mode Sankey

    Same lifecycle as §1, but the extra-cycle nodes are always shown
    and the figure ships with a built-in per-node focus dropdown
    (plotly `updatemenus`). Pick a node from the dropdown inside the
    figure: every ribbon entering or leaving that node lights up, the
    rest dim, and the title shrinks to `node — N in · M out`. "All
    segments" resets to the full view. Designed for slide builds —
    export with `sankey_focus_fig.write_html("path.html", include_plotlyjs="cdn")`
    and either link the HTML from the deck or screenshot each dropdown
    state for a static build sequence.
    """)
    return


@app.cell(hide_code=True)
def _(go, pr_pipeline, sankey_height):
    """Focus-mode Sankey: reuses §1's edge accumulation but always
    includes cycle nodes and embeds a plotly-native focus dropdown so a
    single exported HTML covers every slide build."""

    def _path_for(row: dict) -> list[str]:
        steps = ["opened"]
        if row["first_touch_at"] is not None:
            steps.append("1 queue cycle")
            _bucket = row["cycle_bucket"]
            if _bucket in ("2", "3+"):
                steps.append("2 queue cycles")
            if _bucket == "3+":
                steps.append("3+ queue cycles")
        if row["first_maintainer_merge_at"] is not None:
            steps.append("maintainer-merge")
        if row["had_delegated"]:
            steps.append("delegated")
        elif row["first_ready_to_merge_at"] is not None:
            steps.append("bors r+")
        if row["terminal"] == "merged":
            steps.append("merged")
        elif row["terminal"] == "closed_unmerged":
            steps.append("closed")
        else:
            steps.append("open")
        return steps

    _rows = pr_pipeline.select(
        "first_touch_at",
        "first_maintainer_merge_at",
        "first_ready_to_merge_at",
        "had_delegated",
        "terminal",
        "cycle_bucket",
    ).to_dicts()

    _edge_counts: dict[tuple[str, str], int] = {}
    for _r in _rows:
        _steps = _path_for(_r)
        for _a, _b in zip(_steps, _steps[1:]):
            _edge_counts[(_a, _b)] = _edge_counts.get((_a, _b), 0) + 1

    _node_order = [
        "opened",
        "1 queue cycle",
        "2 queue cycles",
        "3+ queue cycles",
        "maintainer-merge",
        "bors r+",
        "delegated",
        "merged",
        "closed",
        "open",
    ]
    _present = {n for edge in _edge_counts for n in edge}
    _nodes = [n for n in _node_order if n in _present]
    _node_idx = {n: i for i, n in enumerate(_nodes)}

    _node_colors_map = {
        "opened": "#888888",
        "1 queue cycle": "#9cd4ff",
        "2 queue cycles": "#489cdb",
        "3+ queue cycles": "#055999",
        "maintainer-merge": "#cc6633",
        "bors r+": "#73a946",
        "delegated": "#9d72c7",
        "merged": "#2c7a2c",
        "closed": "#aa3333",
        "open": "#bbbbbb",
    }

    # Explicit node positions (x = column, y = vertical) to control
    # ribbon paths — same layout as §1; see that cell for the rationale.
    _NODE_X = {
        "opened": 0.001,
        "1 queue cycle": 0.124,
        "2 queue cycles": 0.307,
        "3+ queue cycles": 0.499,
        "maintainer-merge": 0.620,
        "bors r+": 0.828,
        "delegated": 0.830,
        "merged": 0.999,
        "closed": 0.999,
        "open": 0.999,
    }
    _NODE_Y = {
        "opened": 0.500,
        "1 queue cycle": 0.478,
        "2 queue cycles": 0.507,
        "3+ queue cycles": 0.532,
        "maintainer-merge": 0.085,
        "bors r+": 0.218,
        "delegated": 0.567,
        "merged": 0.310,
        "closed": 0.735,
        "open": 0.853,
    }

    # Per-link arrays, sorted so ribbons stack top-to-bottom by target y
    # within each source node. `_edges_list` is permuted in lockstep so
    # the focus-mode `link.color` lookup stays positionally aligned with
    # `_src` / `_tgt` / `_val`.
    _src_unsorted, _tgt_unsorted, _val_unsorted, _edges_unsorted = [], [], [], []
    for (_a, _b), _v in _edge_counts.items():
        _src_unsorted.append(_node_idx[_a])
        _tgt_unsorted.append(_node_idx[_b])
        _val_unsorted.append(_v)
        _edges_unsorted.append((_a, _b))

    _link_order = sorted(
        range(len(_src_unsorted)),
        key=lambda i: (
            _NODE_X[_nodes[_src_unsorted[i]]],
            _NODE_Y[_nodes[_src_unsorted[i]]],
            _NODE_Y[_nodes[_tgt_unsorted[i]]],
        ),
    )
    _src = [_src_unsorted[i] for i in _link_order]
    _tgt = [_tgt_unsorted[i] for i in _link_order]
    _val = [_val_unsorted[i] for i in _link_order]
    _edges_list = [_edges_unsorted[i] for i in _link_order]

    def _sum_into(node: str) -> int:
        return sum(v for (a, b), v in _edge_counts.items() if b == node)

    def _sum_from(node: str) -> int:
        return sum(v for (a, b), v in _edge_counts.items() if a == node)

    # One focus per node: clicking the dropdown entry highlights every
    # ribbon entering or leaving that node and rewrites the title to a
    # short `node — N in · M out` summary. "All segments" resets the
    # view. Source nodes (no incoming) and sink nodes (no outgoing) get
    # the corresponding half omitted so the title stays compact.
    def _node_title(node: str) -> str:
        # Every PR is counted exactly once both into and out of an
        # internal node (paths terminate at merged / closed / still-open
        # sinks), so `n_in == n_out` for internals. Source / sink nodes
        # only have one side. `max(...)` collapses all three cases to a
        # single "N PRs" count.
        n = max(_sum_into(node), _sum_from(node))
        return f"<b>{node}</b> — {n:,} PRs"

    def _node_edges(node: str) -> set:
        return {(a, b) for (a, b) in _edges_list if a == node or b == node}

    _n_opened = _sum_from("opened")
    # Each focus carries the focus-node name (or None for the "All
    # segments" reset) so `_colors_for` can split incoming vs outgoing
    # ribbons by direction. `_node_edges` is unused now but kept above
    # in case a future focus mode wants undirected highlighting.
    _ = _node_edges
    _focuses = [
        (
            "All segments",
            None,
            f"<b>Full lifecycle</b> — {_n_opened:,} PRs",
        ),
    ]
    for _n in _nodes:
        _focuses.append((_n, _n, _node_title(_n)))

    _DIM = "rgba(200,200,200,0.18)"

    def _hex_to_rgba(h: str, alpha: float) -> str:
        h = h.lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        r, g, bl = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{bl},{alpha})"

    _NODE_RGBA = {n: _hex_to_rgba(c, 0.55) for n, c in _node_colors_map.items()}

    def _colors_for(focus_node: str | None) -> list[str]:
        # Ribbons inherit their non-focus endpoint's node color, so each
        # flow visually points to the segment it connects to. In the
        # "All segments" view (no focus), we fall back to coloring by
        # source — the conventional Plotly-Sankey default.
        if focus_node is None:
            return [_NODE_RGBA.get(a, _DIM) for (a, b) in _edges_list]
        out = []
        for a, b in _edges_list:
            if b == focus_node:
                out.append(_NODE_RGBA.get(a, _DIM))  # incoming → color by source
            elif a == focus_node:
                out.append(_NODE_RGBA.get(b, _DIM))  # outgoing → color by target
            else:
                out.append(_DIM)
        return out

    # Tint each node label to match its segment color and give it a
    # per-node SVG outline (dark for light segments, light for dark).
    # Plotly passes the span `style` attribute through to the SVG
    # `<tspan>` as-is (rewriting `color:` to `fill:`), so SVG-native
    # `stroke` + `paint-order:stroke fill` reliably produces a halo
    # behind the glyphs — `text-shadow` on SVG tspans is ignored by
    # most browsers, and the trace-level `textfont.shadow` is a single
    # value (no per-node array), so neither of those options work here.
    def _halo_color(bg_hex: str) -> str:
        h = bg_hex.lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
        return "#333" if luminance > 0.55 else "white"

    _node_html_labels = []
    for _n in _nodes:
        _c = _node_colors_map.get(_n, "#999")
        _halo = _halo_color(_c)
        _node_html_labels.append(
            f'<span style="color:{_c};stroke:{_halo};stroke-width:{2 if _halo == "#333" else 5};'
            f'paint-order:stroke fill">{_n}</span>'
        )

    sankey_focus_fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="freeform",
                node=dict(
                    label=_node_html_labels,
                    color=[_node_colors_map.get(n, "#999") for n in _nodes],
                    x=[_NODE_X[n] for n in _nodes],
                    y=[_NODE_Y[n] for n in _nodes],
                    pad=28,
                    thickness=18,
                ),
                link=dict(
                    source=_src,
                    target=_tgt,
                    value=_val,
                    color=_colors_for(_focuses[0][1]),
                ),
            )
        ]
    )

    _buttons = [
        dict(
            label=name,
            method="update",
            args=[
                # restyle: per-link colors. Plotly expects the array
                # wrapped in a list (one entry per trace being updated).
                {"link.color": [_colors_for(edges)]},
                # relayout: headline counts for this focus.
                {"title.text": title},
            ],
        )
        for (name, edges, title) in _focuses
    ]

    sankey_focus_fig.update_layout(
        title=dict(text=_focuses[0][2], font=dict(color="black")),
        updatemenus=[
            dict(
                buttons=_buttons,
                direction="up",
                showactive=True,
                x=0.01,
                y=-0.02,
                xanchor="left",
                yanchor="top",
                pad=dict(r=10, t=4, b=4),
                bgcolor="white",
                bordercolor="#bbb",
            )
        ],
        height=int(sankey_height.value),
        margin=dict(l=10, r=10, t=60, b=70),
        font=dict(size=20),
    )

    # Exported for §1c (downstream breakdown cell). Copied out of the
    # `_`-prefixed cell-locals so a single source of truth feeds the
    # Sankey, the focus dropdown, and the breakdown bar charts/tables.
    focus_edge_counts = dict(_edge_counts)
    focus_nodes = list(_nodes)
    focus_node_colors = dict(_node_colors_map)

    # Builder for static PNG export — same geometry/colors as the
    # interactive figure but with a focus baked in as the initial state
    # and no `updatemenus` (kaleido screenshots respect only the static
    # config). `focus_node=None` keeps the "All segments" view.
    def make_focus_sankey_static(focus_node):
        if focus_node is None:
            _title = _focuses[0][2]
        else:
            _title = next(t for (_, fn, t) in _focuses if fn == focus_node)
        _fig = go.Figure(
            data=[
                go.Sankey(
                    arrangement="freeform",
                    node=dict(
                        label=_node_html_labels,
                        color=[_node_colors_map.get(n, "#999") for n in _nodes],
                        x=[_NODE_X[n] for n in _nodes],
                        y=[_NODE_Y[n] for n in _nodes],
                        pad=28,
                        thickness=18,
                    ),
                    link=dict(
                        source=_src,
                        target=_tgt,
                        value=_val,
                        color=_colors_for(focus_node),
                    ),
                )
            ]
        )
        _fig.update_layout(
            title=dict(text=_title, font=dict(color="black")),
            height=int(sankey_height.value),
            margin=dict(l=10, r=10, t=60, b=30),
            font=dict(size=20),
        )
        return _fig

    sankey_focus_fig
    return (
        focus_edge_counts,
        focus_node_colors,
        focus_nodes,
        make_focus_sankey_static,
    )


@app.cell(hide_code=True)
def _(mo):
    """Authoring helper: drag the Sankey nodes around, click this button,
    and the new `_NODE_X` / `_NODE_Y` dicts are printed to the browser
    console (and copied to the clipboard) ready to paste over the
    pinned positions in §1 and §1b.

    Renders via `mo.iframe` because `mo.Html` strips `<script>` tags
    (per the `mo.iframe` docstring). From inside the iframe we walk
    `window.parent.document` to reach the `<marimo-plotly>` shadow
    DOMs in the host page, and we call `parent.navigator.clipboard`
    so the write inherits the host page's clipboard permissions
    (an iframe without `allow="clipboard-write"` would otherwise be
    blocked)."""
    _html = """
    <button id="qb-dump-sankey-btn"
        style="padding:6px 12px;cursor:pointer;
        border:1px solid #bbb;border-radius:4px;background:#fafafa;">
      Dump Sankey node positions (console + clipboard)
    </button>
    <script>
    (() => {
      const btn = document.getElementById('qb-dump-sankey-btn');
      console.log('[qb] Sankey-dump button wired');
      btn.addEventListener('click', () => {
        const doc = window.parent.document;
        const fmt = (n) => Number(n).toFixed(3);
        const gds = [];
        const walk = (root) => {
          gds.push(...root.querySelectorAll('.js-plotly-plot'));
          root.querySelectorAll('*').forEach((el) => {
            if (el.shadowRoot) walk(el.shadowRoot);
          });
        };
        walk(doc);
        const lines = [];
        gds.forEach((gd, gi) => {
          (gd.data || []).forEach((tr, ti) => {
            if (tr.type !== 'sankey') return;
            const {label, x, y} = tr.node;
            const title = (gd.layout && gd.layout.title &&
              (gd.layout.title.text || gd.layout.title)) || '';
            lines.push(`# plot ${gi}, trace ${ti} — ${title}`);
            lines.push('_NODE_X = {');
            label.forEach((l, i) => lines.push(
              `    ${JSON.stringify(l)}: ${fmt(x[i])},`));
            lines.push('}');
            lines.push('_NODE_Y = {');
            label.forEach((l, i) => lines.push(
              `    ${JSON.stringify(l)}: ${fmt(y[i])},`));
            lines.push('}');
          });
        });
        const text = lines.join('\\n');
        console.log(text || '(no sankey plots found)');
        const clip = window.parent.navigator.clipboard;
        if (clip && text) {
          clip.writeText(text).then(
            () => console.log('(copied to clipboard)'),
            (e) => console.warn('clipboard write failed:', e),
          );
        }
      });
    })();
    </script>
    """
    mo.iframe(_html, height="50px")
    return


@app.cell(hide_code=True)
def _(focus_nodes, mo):
    """Node-selector for the §1c breakdown. Independent of the plotly
    `updatemenus` dropdown inside §1b — that one only restyles ribbon
    colors and a title inside the figure; it can't drive a downstream
    cell because marimo never sees which option is selected."""
    breakdown_node = mo.ui.dropdown(
        options=focus_nodes,
        value=focus_nodes[0] if focus_nodes else None,
        label="Breakdown node",
    )
    breakdown_node
    return (breakdown_node,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 1c. In/out flow breakdown for the selected node

    Pick a node above to see exactly which segments feed into it and
    where they go next, with PR counts and share-of-flow. Source nodes
    (`opened`) only have an outgoing table; sink nodes (`merged`,
    `closed unmerged`, `still open`) only have an incoming one.
    """)
    return


@app.cell(hide_code=True)
def _(breakdown_node, focus_edge_counts, focus_node_colors, plt):
    """Render the in/out flow breakdown for the selected `breakdown_node`
    as side-by-side horizontal bar charts. The figure builder is also
    exported so the save loop in the Export section can render one PNG
    per node without duplicating the matplotlib code."""

    def make_breakdown_fig(node):
        """Static-export builder: returns a fresh matplotlib Figure for
        `node`'s in/out flow breakdown. Both the on-screen cell and the
        save loop go through this so the exported PNGs match the
        in-notebook view exactly."""
        in_edges = sorted(
            ((a, v) for (a, b), v in focus_edge_counts.items() if b == node),
            key=lambda t: -t[1],
        )
        out_edges = sorted(
            ((b, v) for (a, b), v in focus_edge_counts.items() if a == node),
            key=lambda t: -t[1],
        )
        n_in = sum(v for _, v in in_edges)
        n_out = sum(v for _, v in out_edges)

        fig, axes = plt.subplots(1, 2, figsize=(10, 3.2))
        for ax, edges, total, title in (
            (axes[0], in_edges, n_in, f"incoming — {n_in:,} PRs"),
            (axes[1], out_edges, n_out, f"outgoing — {n_out:,} PRs"),
        ):
            if not edges:
                ax.set_axis_off()
                ax.text(0.5, 0.5, "(none)", ha="center", va="center", color="#888")
                ax.set_title(title, fontsize=10)
                continue
            labels = [n for n, _ in edges]
            values = [v for _, v in edges]
            colors = [focus_node_colors.get(n, "#888") for n in labels]
            y = range(len(labels))
            ax.barh(y, values, color=colors, edgecolor="white")
            ax.set_yticks(list(y))
            ax.set_yticklabels(labels)
            ax.invert_yaxis()
            for i, v in enumerate(values):
                share = v / total if total else 0
                ax.text(
                    v,
                    i,
                    f"  {v:,} ({share:.0%})",
                    va="center",
                    fontsize=9,
                    color="#333",
                )
            ax.set_title(title, fontsize=10)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.tick_params(axis="x", labelsize=8)
            ax.set_xlim(0, max(values) * 1.25)
        fig.suptitle(f"{node} — flow breakdown", fontsize=12)
        fig.tight_layout()
        return fig

    make_breakdown_fig(breakdown_node.value)
    return (make_breakdown_fig,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 2. Per-stage duration distributions

    Each stage is the delta between two consecutive milestones (e.g.
    stage 2 = `first_touch_at` → `first_maintainer_merge_at`). Stage
    deltas are null when either endpoint is missing or events are in
    non-monotonic order, so each histogram shows only the PRs that
    actually traversed that stage.

    Bins are geometrically spaced (`np.logspace`) — PR open-durations are
    close to log-normal so log-binning shows the bulk and tail in one
    view. A lognormal fit (`scipy.stats.lognorm` with `floc=0`) is
    overlaid on each panel.

    **The `first review → MM` panel is filtered to the iterated regime
    (≥ 60 s).** Stage 2 is bimodal: ~45 % of post-MM PRs are
    *maintainer-first*, where the very first non-author / non-bot event
    is itself the sign-off (a `REVIEW_APPROVED`, or an `ISSUE_COMMENTED`
    carrying the `maintainer merge` trigger phrase), so the
    `github-actions` bot applies the label within ~10–20 s and stage 2
    collapses to the bot-latency floor. To keep this panel readable as
    "actual review-cycle duration before sign-off", maintainer-first
    PRs are excluded here; §2b restores the full distribution and
    surfaces the maintainer-first regime on its own.
    """)
    return


@app.cell(hide_code=True)
def _(lognorm, np, pl, plt, pr_pipeline):
    """4-panel log-binned histogram + lognormal fit, one panel per stage.
    Stage 2 (`first review → MM`) is filtered to the iterated regime
    (≥ 60 s) so the bot-latency-bounded maintainer-first PRs don't
    flatten the panel; see §2b for the full bimodal distribution."""
    _MIN_S2_SECONDS = 60.0  # exclude maintainer-first (bot-latency floor)
    _STAGES = [
        ("seconds_open_to_first_touch", "open → first review", None),
        (
            "seconds_first_touch_to_maintainer_merge",
            "first review → maintainer-merge (iterated, ≥ 60 s)",
            _MIN_S2_SECONDS,
        ),
        (
            "seconds_maintainer_merge_to_ready_to_merge",
            "MM → ready-to-merge",
            None,
        ),
        ("seconds_ready_to_merge_to_merged", "RTM → merged", None),
    ]

    def _draw(ax, x_days: np.ndarray, title: str, *, bins: int = 60) -> None:
        x = x_days[x_days > 0]
        if x.size < 5:
            ax.set_title(f"{title} (n={x.size}, insufficient)")
            ax.set_xscale("log")
            return
        lo = max(x.min(), np.nextafter(0, 1))
        hi = x.max()
        edges = np.logspace(np.log10(lo), np.log10(hi), bins + 1)
        centers = np.sqrt(edges[:-1] * edges[1:])
        counts, _ = np.histogram(x, bins=edges)
        y_step = np.r_[counts, counts[-1]]
        ax.step(edges, y_step, where="post", linewidth=1.2, color="#4a90d9")
        try:
            sigma, _loc, scale = lognorm.fit(x, floc=0)
            mu = np.log(scale)
            cdf = lognorm.cdf(edges, s=sigma, loc=0, scale=scale)
            expected = x.size * np.diff(cdf)
            ax.plot(
                centers,
                expected,
                color="#c63",
                linewidth=2.0,
                label=f"lognormal μ={mu:.2f}, σ={sigma:.2f}",
            )
            ax.legend(fontsize=8)
        except Exception:  # pragma: no cover — defensive on small samples
            pass
        med = float(np.median(x))
        p90 = float(np.percentile(x, 90))
        ax.axvline(med, color="#444", linestyle="--", linewidth=0.8)
        ax.set_xscale("log")
        ax.set_title(f"{title}\nn={x.size}, median={med:.2f}d, p90={p90:.2f}d")
        ax.set_xlabel("duration (days, log scale)")
        ax.set_ylabel("PRs / log bin")

    stage_dist_fig, _axes = plt.subplots(2, 2, figsize=(13, 8))
    for _ax, (_col, _title, _min_seconds) in zip(_axes.ravel(), _STAGES):
        _vals = (
            pr_pipeline.filter(pl.col(_col).is_not_null())
            .select((pl.col(_col) / 86400.0).alias("days"))
            .get_column("days")
            .to_numpy()
        )
        if _min_seconds is not None:
            _vals = _vals[_vals >= (_min_seconds / 86400.0)]
        _draw(_ax, _vals, _title)
    stage_dist_fig.suptitle(
        "Per-stage duration distributions (log bins + lognormal fit)"
    )
    stage_dist_fig.tight_layout()
    stage_dist_fig
    return (stage_dist_fig,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### 2b. `first review → MM` — full bimodal distribution

    The §2 stage-2 panel is filtered to the iterated regime so the
    lognormal fit isn't distorted by the bot-latency floor. The two
    panels below restore the full picture:
    - **Left:** maintainer-first PRs only (< 60 s) — essentially the
      `github-actions` bot-latency distribution. A maintainer's
      approval or `maintainer merge` comment triggers MM in ~10–20 s.
    - **Right:** both regimes overlaid on a single axis with the split
      line at 60 s, showing the bimodality at a glance. The lognormal
      fit is run on the iterated tail only.
    """)
    return


@app.cell(hide_code=True)
def _(lognorm, np, pl, plt, pr_pipeline):
    """Side-by-side stage-2 panels: maintainer-first only + bimodal overlay."""
    _SPLIT_S2_DAYS = 60.0 / 86400.0
    _vals = (
        pr_pipeline.filter(
            pl.col("seconds_first_touch_to_maintainer_merge").is_not_null()
        )
        .select(
            (pl.col("seconds_first_touch_to_maintainer_merge") / 86400.0).alias("days")
        )
        .get_column("days")
        .to_numpy()
    )
    _vals = _vals[_vals > 0]
    _fast = _vals[_vals < _SPLIT_S2_DAYS]
    _slow = _vals[_vals >= _SPLIT_S2_DAYS]

    def _maintainer_first(ax, arr, *, bins: int = 60) -> None:
        if arr.size < 5:
            ax.set_title(f"maintainer-first (n={arr.size}, insufficient)")
            ax.set_xscale("log")
            return
        lo = max(arr.min(), np.nextafter(0, 1))
        hi = arr.max()
        edges = np.logspace(np.log10(lo), np.log10(hi), bins + 1)
        counts, _ = np.histogram(arr, bins=edges)
        y_step = np.r_[counts, counts[-1]]
        ax.step(edges, y_step, where="post", linewidth=1.2, color="#bd7eb6")
        med = float(np.median(arr))
        p90 = float(np.percentile(arr, 90))
        ax.axvline(med, color="#444", linestyle="--", linewidth=0.8)
        ax.set_xscale("log")
        ax.set_title(
            f"maintainer-first (< 60 s)\n"
            f"n={arr.size} ({100 * arr.size / _vals.size:.1f}%), "
            f"median={med * 86400:.0f}s, p90={p90 * 86400:.0f}s"
        )
        ax.set_xlabel("duration (days, log scale)")
        ax.set_ylabel("PRs / log bin")

    def _overlay(ax, fast, slow, *, bins: int = 60) -> None:
        x = np.concatenate([fast, slow])
        if x.size < 5:
            ax.set_title(f"overlay (n={x.size}, insufficient)")
            ax.set_xscale("log")
            return
        lo = max(x.min(), np.nextafter(0, 1))
        hi = x.max()
        edges = np.logspace(np.log10(lo), np.log10(hi), bins + 1)
        centers = np.sqrt(edges[:-1] * edges[1:])
        for arr, color, label in [
            (
                fast,
                "#bd7eb6",
                f"maintainer-first (n={fast.size}, {100 * fast.size / x.size:.0f}%)",
            ),
            (
                slow,
                "#4a90d9",
                f"iterated (n={slow.size}, {100 * slow.size / x.size:.0f}%)",
            ),
        ]:
            if arr.size > 0:
                counts, _ = np.histogram(arr, bins=edges)
                y_step = np.r_[counts, counts[-1]]
                ax.step(
                    edges, y_step, where="post", linewidth=1.2, color=color, label=label
                )
        ax.axvline(_SPLIT_S2_DAYS, color="#888", linestyle=":", linewidth=0.8)
        try:
            if slow.size >= 5:
                sigma, _loc, scale = lognorm.fit(slow, floc=0)
                mu = np.log(scale)
                cdf = lognorm.cdf(edges, s=sigma, loc=0, scale=scale)
                expected = slow.size * np.diff(cdf)
                ax.plot(
                    centers,
                    expected,
                    color="#c63",
                    linewidth=2.0,
                    label=f"lognormal (iterated) μ={mu:.2f}, σ={sigma:.2f}",
                )
        except Exception:
            pass
        ax.legend(fontsize=7)
        med = float(np.median(x))
        p90 = float(np.percentile(x, 90))
        ax.axvline(med, color="#444", linestyle="--", linewidth=0.8)
        ax.set_xscale("log")
        ax.set_title(
            f"both regimes (split at 60 s)\n"
            f"n={x.size}, median={med:.2f}d, p90={p90:.2f}d"
        )
        ax.set_xlabel("duration (days, log scale)")
        ax.set_ylabel("PRs / log bin")

    stage2_split_fig, _axes = plt.subplots(1, 2, figsize=(13, 4))
    _maintainer_first(_axes[0], _fast)
    _overlay(_axes[1], _fast, _slow)
    stage2_split_fig.suptitle(
        "Stage 2 (first review → maintainer-merge) — full bimodal distribution"
    )
    stage2_split_fig.tight_layout()
    stage2_split_fig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### 2c. Sign-off path to RTM — MM-direct vs delegated

    §2's `MM → ready-to-merge` panel pools two structurally different
    paths: PRs whose RTM was applied directly after MM (`MM → bors r+`),
    and PRs that picked up `delegated` along the way
    (`MM → delegated → bors r+`, typically with the author running
    `bors r+` themselves once delegated). Both contribute to the same
    `seconds_maintainer_merge_to_ready_to_merge` delta because that
    column ignores the `delegated` label.

    Splitting the two:
    - **Left:** MM → RTM duration for PRs that were *not* delegated
      (the `bors` path in §1's Sankey).
    - **Middle:** MM → RTM duration for PRs that *were* delegated and
      had both MM and RTM applied. Same delta as §2's pooled panel,
      restricted to the delegated cohort — pairs with the left panel
      to show whether the MM→RTM clock differs by sign-off path.
    - **Right:** delegated → RTM duration for PRs that *were*
      delegated and reached RTM — the time from delegation to
      `bors r+`. MM is not required (some delegated PRs skip MM
      entirely), so this panel covers a partly different cohort.
    """)
    return


@app.cell(hide_code=True)
def _(lognorm, np, pl, plt, pr_pipeline):
    """Three-panel split: MM→RTM for non-delegated PRs, MM→RTM for
    delegated PRs, and delegated→RTM for delegated PRs that reached
    RTM. The two MM→RTM panels (left + middle) partition the cohort
    used by §2's pooled MM→RTM panel; the right panel uses a different
    duration entirely."""
    _mm_rtm_not_delegated_days = (
        pr_pipeline.filter(
            ~pl.col("had_delegated")
            & pl.col("seconds_maintainer_merge_to_ready_to_merge").is_not_null()
        )
        .select(
            (pl.col("seconds_maintainer_merge_to_ready_to_merge") / 86400.0).alias(
                "days"
            )
        )
        .get_column("days")
        .to_numpy()
    )
    _mm_rtm_delegated_days = (
        pr_pipeline.filter(
            pl.col("had_delegated")
            & pl.col("seconds_maintainer_merge_to_ready_to_merge").is_not_null()
        )
        .select(
            (pl.col("seconds_maintainer_merge_to_ready_to_merge") / 86400.0).alias(
                "days"
            )
        )
        .get_column("days")
        .to_numpy()
    )
    _deleg_rtm_days = (
        pr_pipeline.filter(
            pl.col("had_delegated") & pl.col("first_ready_to_merge_at").is_not_null()
        )
        .with_columns(
            (pl.col("first_ready_to_merge_at") - pl.col("first_delegated_at"))
            .dt.total_seconds()
            .cast(pl.Float64)
            .alias("_secs_deleg_to_rtm")
        )
        .filter(pl.col("_secs_deleg_to_rtm") >= 0)
        .select((pl.col("_secs_deleg_to_rtm") / 86400.0).alias("days"))
        .get_column("days")
        .to_numpy()
    )

    def _draw(
        ax, x_days: np.ndarray, title: str, color: str, *, bins: int = 60
    ) -> None:
        x = x_days[x_days > 0]
        if x.size < 5:
            ax.set_title(f"{title} (n={x.size}, insufficient)")
            ax.set_xscale("log")
            return
        lo = max(x.min(), np.nextafter(0, 1))
        hi = x.max()
        edges = np.logspace(np.log10(lo), np.log10(hi), bins + 1)
        centers = np.sqrt(edges[:-1] * edges[1:])
        counts, _ = np.histogram(x, bins=edges)
        y_step = np.r_[counts, counts[-1]]
        ax.step(edges, y_step, where="post", linewidth=1.2, color=color)
        try:
            sigma, _loc, scale = lognorm.fit(x, floc=0)
            mu = np.log(scale)
            cdf = lognorm.cdf(edges, s=sigma, loc=0, scale=scale)
            expected = x.size * np.diff(cdf)
            ax.plot(
                centers,
                expected,
                color="#c63",
                linewidth=2.0,
                label=f"lognormal μ={mu:.2f}, σ={sigma:.2f}",
            )
            ax.legend(fontsize=8)
        except Exception:  # pragma: no cover — defensive on small samples
            pass
        med = float(np.median(x))
        p90 = float(np.percentile(x, 90))
        ax.axvline(med, color="#444", linestyle="--", linewidth=0.8)
        ax.set_xscale("log")
        ax.set_title(f"{title}\nn={x.size}, median={med:.2f}d, p90={p90:.2f}d")
        ax.set_xlabel("duration (days, log scale)")
        ax.set_ylabel("PRs / log bin")

    stage_signoff_fig, _axes = plt.subplots(1, 3, figsize=(16, 4))
    _draw(_axes[0], _mm_rtm_not_delegated_days, "MM → RTM (not delegated)", "#73a946")
    _draw(_axes[1], _mm_rtm_delegated_days, "MM → RTM (delegated)", "#4a90d9")
    _draw(_axes[2], _deleg_rtm_days, "delegated → RTM", "#9d72c7")
    stage_signoff_fig.suptitle(
        "Sign-off paths to ready-to-merge: MM-direct vs delegated"
    )
    stage_signoff_fig.tight_layout()
    stage_signoff_fig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 3. Stage share of total time-to-merge

    For merged PRs with all 4 stages non-null, compute each PR's
    per-stage fraction `stage_X_seconds / open→merged_seconds`, then
    average across PRs. Because each PR's four fractions sum to 1.0 by
    construction, the mean fractions sum to exactly 100 % — this gives
    an honest "typical PR's lifecycle decomposition" without the
    median-of-sums ≠ sum-of-medians pitfall. Fractions are bounded in
    `[0, 1]` so the mean is robust to absolute-duration outliers.

    The per-stage median / p90 table below is unchanged — it reports
    each stage's median across all merged PRs that traversed it, useful
    for "how long is a typical stage 2?" rather than "what share of a
    typical PR's TTM is stage 2?".
    """)
    return


@app.cell(hide_code=True)
def _(np, pl, plt, pr_pipeline):
    """Stage shares = mean of per-PR fractions across merged PRs with
    all 4 stages non-null (sums to 100% by construction). Also returns
    a per-stage median/p90 duration table for reference."""
    _STAGE_COLS = [
        ("open → first review", "seconds_open_to_first_touch", "#4a90d9"),
        ("first review → MM", "seconds_first_touch_to_maintainer_merge", "#c63"),
        ("MM → RTM", "seconds_maintainer_merge_to_ready_to_merge", "#73a946"),
        ("RTM → merged", "seconds_ready_to_merge_to_merged", "#9d72c7"),
    ]

    _merged_only = pr_pipeline.filter(pl.col("is_merged"))

    # Informational per-stage stats (median/p90 of absolute durations,
    # across all merged PRs that traversed the stage).
    _rows = []
    for _label, _col, _color in _STAGE_COLS:
        _vals = (
            _merged_only.filter(pl.col(_col).is_not_null())
            .select((pl.col(_col) / 86400.0).alias("days"))
            .get_column("days")
            .to_numpy()
        )
        _med = float(np.median(_vals)) if _vals.size else 0.0
        _p90 = float(np.percentile(_vals, 90)) if _vals.size else 0.0
        _rows.append(
            {
                "stage": _label,
                "n_prs_with_stage": int(_vals.size),
                "median_days": round(_med, 2),
                "p90_days": round(_p90, 2),
            }
        )
    stage_medians = pl.DataFrame(_rows)

    # Per-PR fraction decomposition: restrict to merged PRs with all 4
    # stage deltas non-null and a positive open→merged. Each PR's four
    # fractions sum to 1.0 → the mean across PRs is additive.
    _all4 = _merged_only.filter(pl.col("seconds_open_to_merged") > 0)
    for _, _col, _ in _STAGE_COLS:
        _all4 = _all4.filter(pl.col(_col).is_not_null())

    _mean_shares = []
    for _label, _col, _ in _STAGE_COLS:
        _f = (
            _all4.select(
                (pl.col(_col) / pl.col("seconds_open_to_merged")).alias("frac")
            )
            .get_column("frac")
            .to_numpy()
        )
        _mean_shares.append(float(np.mean(_f)) if _f.size else 0.0)

    _n_all4 = _all4.height
    _ttm_med = (
        float(
            np.median(_all4.get_column("seconds_open_to_merged").to_numpy() / 86400.0)
        )
        if _n_all4
        else 0.0
    )

    stage_share_fig, _ax = plt.subplots(figsize=(11, 1.6))
    _left = 0.0
    for (_label, _col, _color), _share in zip(_STAGE_COLS, _mean_shares):
        _ax.barh([0], [_share], left=_left, color=_color, edgecolor="white")
        _ax.text(
            _left + _share / 2,
            0,
            f"{_label}\n{_share:.0%}",
            ha="center",
            va="center",
            fontsize=8,
            color="white" if _share > 0.1 else "black",
        )
        _left += _share
    _ax.set_xlim(0, 1)
    _ax.set_yticks([])
    _ax.set_xlabel("Mean per-PR share of TTM")
    _ax.set_title(
        f"Stage share of typical merged PR's TTM "
        f"(n={_n_all4} with all 4 stages non-null, median TTM = {_ttm_med:.2f} days)"
    )
    stage_share_fig.tight_layout()
    stage_share_fig
    return stage_medians, stage_share_fig


@app.cell
def _(mo, stage_medians):
    mo.md("**Per-stage median / p90 (merged PRs only):**")
    stage_medians
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4. Queue cycles before maintainer-merge

    Each PR's queue-window count (ruleset 3) is partitioned at the first
    `maintainer-merge` timestamp; cycles that opened before MM count
    toward "cycles before MM", later cycles are post-MM ping-pong. PRs
    that never reached MM contribute their full cycle count. The
    distribution is heavily skewed — most merged PRs reach MM in one
    queue cycle.

    The left panel uses a log Y axis with one bar per integer count
    and the tail pooled into the rightmost `X+` bin at the per-cohort
    p99 (matches §6's idiom). A shifted-geometric MLE fit on support
    `k ≥ 1` is overlaid (each cycle has an independent probability `q`
    of being the last before MM); the pooled bin's expected mass uses
    the survival probability `(1 - q)^(cutoff - 1)`. A clean geometric
    would be a straight line on log Y; deviations show where the "each
    cycle independently signs off" model breaks down. Bin 0 (PRs that
    reached MM with no prior queue window) is excluded from the fit —
    it's a small, separate population.

    The right panel plots median TTM and median stage-2 latency
    against `n_queue_cycles_before_mm` (one point per bucket with at
    least 30 PRs, IQR error bars), with a linear OLS fit overlaid.
    Under a "each cycle adds roughly a constant marginal duration"
    model the points fall on a line; the slope reads as
    days-per-extra-cycle. A downturn or non-monotonicity at high `n`
    is suggestive of survivor effects (PRs that survive many cycles
    tend to have shorter touch-up rounds).
    """)
    return


@app.cell
def _(np, pl, plt, pr_pipeline):
    """Histogram of queue cycles before MM (merged PRs) + median TTM by bucket."""
    _merged = pr_pipeline.filter(pl.col("is_merged"))

    _counts = (
        _merged.group_by("cycle_bucket")
        .agg(
            pl.len().alias("n_prs"),
            pl.col("seconds_open_to_merged").median().alias("median_seconds"),
            pl.col("seconds_first_touch_to_maintainer_merge")
            .median()
            .alias("median_stage2_seconds"),
        )
        .sort("cycle_bucket")
    )

    cycle_table = _counts.with_columns(
        (pl.col("median_seconds") / 86400.0).round(2).alias("median_ttm_days"),
        (pl.col("median_stage2_seconds") / 86400.0)
        .round(2)
        .alias("median_stage2_days"),
    ).drop("median_seconds", "median_stage2_seconds")

    _raw_counts = (
        _merged.group_by("n_queue_cycles_before_mm")
        .agg(pl.len().alias("n_prs"))
        .sort("n_queue_cycles_before_mm")
    )
    _x_full = _raw_counts.get_column("n_queue_cycles_before_mm").to_numpy()
    _y_full = _raw_counts.get_column("n_prs").to_numpy()

    # Pool the tail at p99 to match §6's idiom and avoid a long sparse run
    # of ~zero bars. Recompute fit on the unpooled positive bins so the
    # MLE isn't biased; the pooled bin's expected mass is the survival
    # probability `(1 - q)^(cutoff - 1)`.
    _all_vals = _merged.get_column("n_queue_cycles_before_mm").to_numpy().astype(int)
    _cutoff = max(5, int(np.ceil(np.percentile(_all_vals, 99))))
    _clipped = np.minimum(_all_vals, _cutoff)
    _x = np.arange(0, _cutoff + 1)
    _y = np.bincount(_clipped, minlength=_cutoff + 1)

    cycle_count_fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(13, 4))
    _ax1.bar(_x, _y, color="#4a90d9", edgecolor="white")
    # Shifted-geometric MLE on support {1, 2, ...}: P(k) = (1-q)^(k-1) q,
    # mean = 1/q. Fit on the unpooled positive counts (bin 0 is excluded
    # — PRs that reached MM with no prior queue window are a small
    # separate population).
    _pos_vals = _all_vals[_all_vals >= 1]
    if _pos_vals.size > 0:
        _mean_pos = float(_pos_vals.mean())
        _q = 1.0 / _mean_pos if _mean_pos > 0 else 1.0
        _expected = _pos_vals.size * (1 - _q) ** (_x.astype(float) - 1) * _q
        _expected[0] = np.nan  # no fit on bin 0
        # Pooled tail bin: survival mass P(X ≥ cutoff) = (1-q)^(cutoff-1).
        _expected[-1] = _pos_vals.size * (1 - _q) ** (_cutoff - 1)
        _ax1.plot(
            _x,
            _expected,
            color="#c63",
            marker="o",
            markersize=4,
            linewidth=1.5,
            label=f"geometric MLE on k≥1: q={_q:.3f}, mean={_mean_pos:.2f}",
        )
        _ax1.legend(fontsize=8)
    _ax1.set_yscale("log")
    _labels = [str(i) for i in _x[:-1]] + [f"{_cutoff}+"]
    _ax1.set_xticks(_x)
    _ax1.set_xticklabels(_labels, rotation=0, fontsize=7)
    _ax1.set_xlabel("queue cycles before MM")
    _ax1.set_ylabel("merged PRs (log scale)")
    _ax1.set_title("Cycle count distribution (merged, log Y)")

    # Per-cycle-count medians (no bucketing) + IQR for the fit-friendly plot.
    # Keep only buckets with enough PRs that the median isn't noisy.
    _MIN_N_PER_BUCKET = 30
    _per_cycle = (
        _merged.group_by("n_queue_cycles_before_mm")
        .agg(
            pl.len().alias("n_prs"),
            pl.col("seconds_open_to_merged").median().alias("med_ttm_s"),
            pl.col("seconds_open_to_merged").quantile(0.25).alias("p25_ttm_s"),
            pl.col("seconds_open_to_merged").quantile(0.75).alias("p75_ttm_s"),
            pl.col("seconds_first_touch_to_maintainer_merge")
            .median()
            .alias("med_s2_s"),
            pl.col("seconds_first_touch_to_maintainer_merge")
            .quantile(0.25)
            .alias("p25_s2_s"),
            pl.col("seconds_first_touch_to_maintainer_merge")
            .quantile(0.75)
            .alias("p75_s2_s"),
        )
        .filter(pl.col("n_prs") >= _MIN_N_PER_BUCKET)
        .sort("n_queue_cycles_before_mm")
    )
    _xs_fit = _per_cycle.get_column("n_queue_cycles_before_mm").to_numpy()
    _ttm_med = _per_cycle.get_column("med_ttm_s").to_numpy() / 86400.0
    _ttm_p25 = _per_cycle.get_column("p25_ttm_s").to_numpy() / 86400.0
    _ttm_p75 = _per_cycle.get_column("p75_ttm_s").to_numpy() / 86400.0
    _s2_med = _per_cycle.get_column("med_s2_s").to_numpy() / 86400.0
    _s2_p25 = _per_cycle.get_column("p25_s2_s").to_numpy() / 86400.0
    _s2_p75 = _per_cycle.get_column("p75_s2_s").to_numpy() / 86400.0

    def _ols(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
        if x.size < 2:
            return 0.0, 0.0, float("nan")
        _A = np.vstack([x, np.ones_like(x)]).T
        _slope, _intercept = np.linalg.lstsq(_A, y, rcond=None)[0]
        _pred = _slope * x + _intercept
        _ss_res = float(np.sum((y - _pred) ** 2))
        _ss_tot = float(np.sum((y - y.mean()) ** 2))
        _r2 = 1.0 - _ss_res / _ss_tot if _ss_tot > 0 else float("nan")
        return float(_slope), float(_intercept), _r2

    _ttm_slope, _ttm_int, _ttm_r2 = _ols(_xs_fit, _ttm_med)
    _s2_slope, _s2_int, _s2_r2 = _ols(_xs_fit, _s2_med)

    _ax2.errorbar(
        _xs_fit,
        _ttm_med,
        yerr=[_ttm_med - _ttm_p25, _ttm_p75 - _ttm_med],
        fmt="o",
        color="#73a946",
        markersize=6,
        capsize=3,
        label="median TTM (IQR bars)",
    )
    _ax2.errorbar(
        _xs_fit,
        _s2_med,
        yerr=[_s2_med - _s2_p25, _s2_p75 - _s2_med],
        fmt="o",
        color="#c63",
        markersize=6,
        capsize=3,
        label="median stage 2 (IQR bars)",
    )
    if _xs_fit.size >= 2:
        _xline = np.linspace(float(_xs_fit.min()), float(_xs_fit.max()), 100)
        _ax2.plot(
            _xline,
            _ttm_slope * _xline + _ttm_int,
            "--",
            color="#73a946",
            linewidth=1.0,
            label=(
                f"OLS TTM: {_ttm_slope:.2f} d/cycle + {_ttm_int:.2f} d "
                f"(R²={_ttm_r2:.2f})"
            ),
        )
        _ax2.plot(
            _xline,
            _s2_slope * _xline + _s2_int,
            "--",
            color="#c63",
            linewidth=1.0,
            label=(
                f"OLS s2:  {_s2_slope:.2f} d/cycle + {_s2_int:.2f} d (R²={_s2_r2:.2f})"
            ),
        )
    _ax2.set_xlabel("queue cycles before MM")
    _ax2.set_ylabel("days (median, IQR bars)")
    _ax2.set_title(
        f"TTM & stage 2 vs cycle count (buckets with n ≥ {_MIN_N_PER_BUCKET}, OLS fit)"
    )
    _ax2.legend(fontsize=7)
    cycle_count_fig.tight_layout()
    cycle_count_fig
    return cycle_count_fig, cycle_table


@app.cell
def _(cycle_table, mo):
    mo.md("**Cycle-bucket detail (merged PRs):**")
    cycle_table
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### 4b. Queue-window duration by cycle index

    Does each cycle look the same, or are early cycles systematically
    longer / shorter than later ones? The overlay below shows the
    duration distribution of closed pre-MM queue windows, one curve
    per cycle index (1 = the PR's first queue window, 2 = second, …,
    5+ pooled). Counts per curve are normalized to share so shapes are
    directly comparable across cycle indices despite very different
    sample sizes. If each cycle were i.i.d., the curves would land on
    top of each other.

    Restriction: closed windows only (open windows have no real
    duration) and pre-MM only (post-MM cycles are RTM ping-pong, a
    different process). Ruleset 3 (the dashboard's ruleset) only.
    """)
    return


@app.cell(hide_code=True)
def _(asof, np, pl, plt, pr_pipeline, queue_window_intervals, queue_windows):
    """Overlay per-cycle-index duration histograms for pre-MM queue windows.
    Curves are normalized to share-of-cycle so shapes are comparable
    across indices regardless of n."""
    _qw = (
        queue_window_intervals(queue_windows, asof=asof)
        .filter(~pl.col("is_open"))
        .join(
            pr_pipeline.select("pull_request_id", "first_maintainer_merge_at"),
            on="pull_request_id",
            how="inner",
        )
        .filter(
            pl.col("first_maintainer_merge_at").is_null()
            | (pl.col("start") < pl.col("first_maintainer_merge_at"))
        )
        .with_columns(
            pl.when(pl.col("cycle_index") >= 5)
            .then(5)
            .otherwise(pl.col("cycle_index"))
            .alias("idx_bucket")
        )
    )

    _COLORS = ["#4a90d9", "#73a946", "#c63", "#9d72c7", "#444"]
    _LABELS = ["1st", "2nd", "3rd", "4th", "5th+"]

    qw_idx_fig, _ax = plt.subplots(figsize=(11, 4.5))
    _all = _qw.get_column("duration_days").to_numpy()
    _all = _all[_all > 0]
    if _all.size >= 5:
        _edges = np.logspace(
            np.log10(max(_all.min(), np.nextafter(0, 1))),
            np.log10(_all.max()),
            51,
        )
        for _idx, (_color, _label) in enumerate(zip(_COLORS, _LABELS), start=1):
            _arr = (
                _qw.filter(pl.col("idx_bucket") == _idx)
                .get_column("duration_days")
                .to_numpy()
            )
            _arr = _arr[_arr > 0]
            if _arr.size > 0:
                _counts, _ = np.histogram(_arr, bins=_edges)
                _share = _counts / _counts.sum() if _counts.sum() > 0 else _counts
                _y_step = np.r_[_share, _share[-1]]
                _ax.step(
                    _edges,
                    _y_step,
                    where="post",
                    color=_color,
                    linewidth=1.4,
                    label=(
                        f"{_label} (n={_arr.size}, "
                        f"median={np.median(_arr):.2f}d, p90={np.percentile(_arr, 90):.2f}d)"
                    ),
                )
                _ax.axvline(
                    np.median(_arr),
                    color=_color,
                    linestyle="--",
                    linewidth=0.7,
                    alpha=0.6,
                )
        _ax.set_xscale("log")
        _ax.set_xlabel("queue-window duration (days, log scale)")
        _ax.set_ylabel("share of windows at that cycle index")
        _ax.set_title("Pre-MM queue-window duration by cycle index")
        _ax.legend(fontsize=8)
    qw_idx_fig.tight_layout()
    qw_idx_fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. Slices — stage medians by PR shape

    Per-slice median stage durations (in days, merged PRs only). Each
    table has one row per slice value and one column per stage; a final
    column is the merged-PR count. Slices: `pr_type`, `lines_bucket`,
    topic area, first-PR vs returning author. Stories B/D will go deeper
    on individual slices; here we just expose the shape.
    """)
    return


@app.cell
def _(pl):
    """Helper: per-slice median stage durations + count.

    Returns a wide frame: one row per slice value, columns for each
    stage's median in days plus `n_merged`. Designed to be called
    repeatedly for different slice axes without duplicating the boilerplate.
    """

    def slice_stage_medians(
        df: "pl.DataFrame",
        *,
        slice_col: str,
        slice_order: list[str] | None = None,
    ) -> "pl.DataFrame":
        _STAGE_PAIRS = [
            ("seconds_open_to_first_touch", "open→review"),
            ("seconds_first_touch_to_maintainer_merge", "review→MM"),
            ("seconds_maintainer_merge_to_ready_to_merge", "MM→RTM"),
            ("seconds_ready_to_merge_to_merged", "RTM→merged"),
            ("seconds_open_to_merged", "open→merged"),
        ]
        merged = df.filter(pl.col("is_merged"))
        out = (
            merged.group_by(slice_col)
            .agg(
                pl.len().cast(pl.Int64).alias("n_merged"),
                *[
                    (pl.col(_col).median() / 86400.0).round(2).alias(_name)
                    for _col, _name in _STAGE_PAIRS
                ],
            )
            .filter(pl.col(slice_col).is_not_null())
        )
        if slice_order is not None:
            _order_map = {v: i for i, v in enumerate(slice_order)}
            out = (
                out.with_columns(
                    pl.col(slice_col)
                    .map_elements(
                        lambda v: _order_map.get(v, 99), return_dtype=pl.Int64
                    )
                    .alias("_ord")
                )
                .sort("_ord")
                .drop("_ord")
            )
        else:
            out = out.sort(slice_col)
        return out

    return (slice_stage_medians,)


@app.cell
def _(mo, pr_pipeline, pr_type_order, slice_stage_medians):
    slice_by_pr_type = slice_stage_medians(
        pr_pipeline, slice_col="pr_type", slice_order=list(pr_type_order())
    )
    mo.md("**By PR type:**")
    slice_by_pr_type
    return (slice_by_pr_type,)


@app.cell
def _(lines_bucket_order, mo, pr_pipeline, slice_stage_medians):
    slice_by_lines_bucket = slice_stage_medians(
        pr_pipeline, slice_col="lines_bucket", slice_order=lines_bucket_order
    )
    mo.md("**By lines bucket:**")
    slice_by_lines_bucket
    return (slice_by_lines_bucket,)


@app.cell
def _(mo, pl, pr_pipeline, pr_topic_one, slice_stage_medians):
    """By topic area. Each PR is attributed to one canonical area
    (alphabetically first of its active `t-*` labels at merge time),
    and we restrict to the 10 largest areas in the cohort to keep the
    table readable."""
    _enriched = pr_pipeline.join(pr_topic_one, on="pull_request_id", how="left")
    _top_areas = (
        _enriched.filter(pl.col("is_merged") & pl.col("topic_area").is_not_null())
        .group_by("topic_area")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(10)
        .get_column("topic_area")
        .to_list()
    )
    slice_by_topic_area = slice_stage_medians(
        _enriched.filter(pl.col("topic_area").is_in(_top_areas)),
        slice_col="topic_area",
        slice_order=_top_areas,
    )
    mo.md("**By topic area (top 10 by merged-PR count):**")
    slice_by_topic_area
    return (slice_by_topic_area,)


@app.cell
def _(mo, pl, pr_pipeline, slice_stage_medians):
    """First PR vs returning. Session 11 showed first-PRs get *faster*
    median first review; the downstream stages tell whether the funnel
    closes that gap or reopens one."""
    _df = pr_pipeline.with_columns(
        pl.when(pl.col("is_first_pr"))
        .then(pl.lit("first PR"))
        .otherwise(pl.lit("returning"))
        .alias("author_cohort_label")
    )
    slice_by_author_cohort = slice_stage_medians(
        _df,
        slice_col="author_cohort_label",
        slice_order=["first PR", "returning"],
    )
    mo.md("**By author cohort:**")
    slice_by_author_cohort
    return (slice_by_author_cohort,)


@app.cell
def _(mo):
    mo.md("""
    ## 6. Review activity per PR

    Three review-depth signals per merged PR (with bots filtered out via
    `DEFAULT_BOT_ACTORS`):

    - `n_review_events_by_others` — non-author `REVIEW_*` events
    - `n_issue_comments_by_others` — non-author top-level comments
    - `n_inline_comments_by_others` — inline review comments (from
      `inline_comment_stats`)

    Inline comments are the closest "substantive review depth" proxy the
    dataset offers (comment bodies aren't exported). Each panel uses
    integer X bins (one bar per count) with the tail pooled into the
    rightmost `X+` bin at the per-signal p99; log Y reveals the
    heavy-tail decay. A **negative-binomial** MLE is overlaid as the
    fit reference: NB generalizes the geometric (geom = NB with r=1)
    and accommodates the strong over-dispersion these counts show
    (`var/mean` shown in each panel title — ≈ 4× for review events,
    ≈ 2× for issue comments, ≈ 16× for inline comments). The pooled
    bin's expected mass uses the survival probability
    `P(X ≥ cutoff)`.
    """)
    return


@app.cell
def _(
    DEFAULT_BOT_ACTORS,
    events,
    inline_comment_stats,
    inline_comments,
    pl,
    prs_cohort,
):
    """Per-PR review-activity counts (bots + author excluded)."""
    _author_keys = prs_cohort.select(
        pl.col("id").alias("pull_request_id"),
        pl.col("author_login").str.to_lowercase().alias("_author_lc"),
    )
    _bots_lc = [b.lower() for b in DEFAULT_BOT_ACTORS]

    _review_types = ("REVIEW_APPROVED", "REVIEW_COMMENTED", "REVIEW_CHANGES_REQUESTED")
    _ev_classified = (
        events.filter(pl.col("type").is_in(["ISSUE_COMMENTED", *_review_types]))
        .join(_author_keys, on="pull_request_id", how="inner")
        .with_columns(pl.col("actor_login").str.to_lowercase().alias("_actor_lc"))
        .filter(~pl.col("_actor_lc").is_in(_bots_lc))
        .filter(
            (pl.col("_actor_lc") != pl.col("_author_lc"))
            | pl.col("_author_lc").is_null()
        )
    )

    review_counts = (
        _ev_classified.with_columns(
            pl.col("type").is_in(list(_review_types)).alias("_is_review"),
            (pl.col("type") == "ISSUE_COMMENTED").alias("_is_issue_comment"),
        )
        .group_by("pull_request_id")
        .agg(
            pl.col("_is_review")
            .sum()
            .cast(pl.Int64)
            .alias("n_review_events_by_others"),
            pl.col("_is_issue_comment")
            .sum()
            .cast(pl.Int64)
            .alias("n_issue_comments_by_others"),
        )
    )

    if inline_comments is not None:
        inline_counts = inline_comment_stats(inline_comments, prs_cohort).select(
            "pull_request_id", "n_inline_comments_by_others"
        )
    else:
        inline_counts = pl.DataFrame(
            schema={
                "pull_request_id": pl.Int64,
                "n_inline_comments_by_others": pl.Int64,
            }
        )
    return inline_counts, review_counts


@app.cell
def _(
    inline_counts,
    minimize,
    nbinom,
    np,
    pl,
    plt,
    pr_pipeline,
    review_counts,
):
    """3-panel review-activity distribution (merged PRs only).
    Linear X with one bar per integer count, tail pooled at the
    rightmost p99 bin; log Y reveals the geometric-like tail. A
    negative-binomial MLE is overlaid (NB generalizes the geometric —
    geom = NB with r=1 — and handles the heavy over-dispersion of
    these counts, particularly inline comments where var/mean ≈ 16)."""
    _merged = (
        pr_pipeline.filter(pl.col("is_merged"))
        .join(review_counts, on="pull_request_id", how="left")
        .join(inline_counts, on="pull_request_id", how="left")
        .with_columns(
            pl.col("n_review_events_by_others").fill_null(0),
            pl.col("n_issue_comments_by_others").fill_null(0),
            pl.col("n_inline_comments_by_others").fill_null(0),
        )
    )

    _SIGNALS = [
        ("n_review_events_by_others", "REVIEW_* events / merged PR"),
        ("n_issue_comments_by_others", "non-author top-level comments"),
        ("n_inline_comments_by_others", "inline comments-by-others"),
    ]

    def _fit_nb_mle(vals: np.ndarray) -> tuple[float, float]:
        """Negative-binomial MLE on {0, 1, 2, ...}. Returns (r, p)."""
        _mean = float(vals.mean())
        _var = float(vals.var())
        if _var > _mean:  # MoM seed
            _p0 = _mean / _var
            _r0 = _mean * _p0 / max(1 - _p0, 1e-9)
        else:  # under-dispersed → seed at geom (r=1)
            _r0 = 1.0
            _p0 = 1.0 / (1.0 + _mean) if _mean > 0 else 0.5

        def _neg_ll(params: np.ndarray) -> float:
            _r, _p = params
            if _r <= 0 or _p <= 0 or _p >= 1:
                return 1e12
            return float(-nbinom.logpmf(vals, n=_r, p=_p).sum())

        _res = minimize(
            _neg_ll, [_r0, _p0], method="Nelder-Mead", options={"xatol": 1e-6}
        )
        return float(_res.x[0]), float(_res.x[1])

    def _draw_count(ax, vals: np.ndarray, title: str) -> None:
        if vals.size == 0:
            ax.set_title(f"{title} (empty)")
            return
        _p99 = int(np.ceil(np.percentile(vals, 99)))
        _cutoff = max(5, _p99)
        _clipped = np.minimum(vals.astype(int), _cutoff)
        _xs = np.arange(0, _cutoff + 1)
        _counts = np.bincount(_clipped, minlength=_cutoff + 1)
        ax.bar(_xs, _counts, color="#4a90d9", edgecolor="white")
        if vals.mean() > 0:
            _r, _p = _fit_nb_mle(vals)
            _expected = nbinom.pmf(_xs, n=_r, p=_p) * vals.size
            # rightmost bin is pooled "≥ cutoff" → survival mass.
            _expected[-1] = nbinom.sf(_cutoff - 1, n=_r, p=_p) * vals.size
            ax.plot(
                _xs,
                _expected,
                color="#c63",
                marker="o",
                markersize=4,
                linewidth=1.5,
                label=f"NB MLE: r={_r:.2f}, p={_p:.3f}, mean={vals.mean():.2f}",
            )
            ax.legend(fontsize=7)
        ax.set_yscale("log")
        _labels = [str(i) for i in _xs[:-1]] + [f"{_cutoff}+"]
        ax.set_xticks(_xs)
        ax.set_xticklabels(_labels, rotation=0, fontsize=7)
        ax.set_xlabel("count")
        _disp = float(vals.var() / vals.mean()) if vals.mean() > 0 else 0.0
        ax.set_title(
            f"{title}\nmedian={int(np.median(vals))}, "
            f"p90={int(np.percentile(vals, 90))}, "
            f"%zero={100 * (vals == 0).mean():.0f}%, "
            f"var/mean={_disp:.1f}"
        )

    review_activity_fig, _axes = plt.subplots(1, 3, figsize=(15, 4))
    for _ax, (_col, _title) in zip(_axes, _SIGNALS):
        _draw_count(_ax, _merged.get_column(_col).to_numpy(), _title)
    _axes[0].set_ylabel("merged PRs (log scale)")
    review_activity_fig.tight_layout()
    review_activity_fig
    return (review_activity_fig,)


@app.cell
def _(mo):
    mo.md("""
    ## 7. Path classification

    Among **merged** PRs, how did the final sign-off happen?

    - **bors** — MM applied → RTM applied → merge, `delegated` never applied
    - **delegated** — `delegated` ever applied (≈ author runs `bors r+`
      themselves; per Session 13, 76 % of delegated PRs are
      author-self-merged)
    - **mm_no_rtm** — MM applied but no RTM (rare; admin-merged or
      pre-bors-r+ workflow)
    - **direct** — merged without MM (most pre-cutover PRs in the
      all-time cohort)
    """)
    return


@app.cell
def _(mo, np, pl, pr_pipeline):
    """Counts + TTM medians per merge_path. Useful for spotting the
    delegated vs bors latency split called out in Session 13."""
    _merged = pr_pipeline.filter(pl.col("is_merged"))
    path_table = (
        _merged.group_by("merge_path")
        .agg(
            pl.len().cast(pl.Int64).alias("n_merged"),
            (pl.col("seconds_open_to_merged").median() / 86400.0)
            .round(2)
            .alias("median_ttm_days"),
            (pl.col("seconds_open_to_merged").quantile(0.9) / 86400.0)
            .round(2)
            .alias("p90_ttm_days"),
            (pl.col("seconds_maintainer_merge_to_ready_to_merge").median() / 86400.0)
            .round(2)
            .alias("median_mm_to_rtm_days"),
        )
        .sort("n_merged", descending=True)
    )
    _ = np  # quiet the linter — kept available for future ad-hoc additions
    mo.md("**Path counts and TTM medians (merged PRs only):**")
    path_table
    return (path_table,)


@app.cell
def _(mo):
    mo.md("""
    ## 8. Notes & caveats

    - **Cohort scope.** The `maintainer-merge` label only exists from
      2024-02-15. For the `all-time` cohort the early years contribute
      heavily to the `direct` (no MM) merge path; for headline numbers
      use `post-MM`.
    - **`delegated` is a path tag, not a stage.** A delegated PR may also
      receive `ready-to-merge` (typical when the author runs `bors r+`
      themselves). The Sankey treats `delegated` as taking precedence
      over `bors r+` in path attribution; the path-count table mirrors
      that.
    - **Stage deltas null on non-monotonic events.** If MM and RTM are
      applied in reverse order (rare; usually `bors r+` without a prior
      `maintainer merge` comment), the MM→RTM delta is null rather than
      negative so log-scale plots stay clean.
    - **Topic area is taken at merge time** via `labels_active_at`. PRs
      with multiple active `t-*` labels at merge contribute to each;
      the per-area slice uses the alphabetically-first label as
      canonical to avoid double-counting.
    - **Court-vs-author latency split** of the `first review → MM` stage
      is deferred to Story B (latency decomposition). Here we surface
      only queue-cycle *count* as the queue-aware signal.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Export

    Pick a base folder (server-side, on the machine running marimo),
    name the run, then click **Save outputs**. If you don't click a
    folder in the browser, the run falls back to the default base
    `_site/exports/` (under the repo root). Files land in
    `<base>/<run name>/`:

    - `1b__<focus>.png` — one image per focus-dropdown state of §1b
      (kaleido is required for the Sankey PNG; if it fails the cell
      falls back to a self-contained HTML).
    - `1c__<node>.png` — one image per breakdown node of §1c.
    - `2_per-stage-distributions.png`, `3_stage-share.png`,
      `4_queue-cycles.png`, `6_review-activity.png` — current state of
      the §2/§3/§4/§6 plots.
    - `cohort_summary.csv`, `3_stage_medians.csv`, `4_cycle_table.csv`,
      `5_slice_by_*.csv`, `7_path_table.csv` — supporting tables.
    - `settings.json` — cohort + filter state at save time.

    The save loop honours whatever filters / cohort / sankey-height you
    have set above, so iterating on a scenario is: adjust filters →
    type a new run name → click Save.
    """)
    return


@app.cell(hide_code=True)
def _(Path, is_wasm, mo):
    """Picker UI: a server-side folder browser, a run-name text input,
    and the Save button. `restrict_navigation=False` so the picker can
    point anywhere on disk; `_site/exports/` (under the repo root) is
    the default starting directory because it's already gitignored.

    `save_default_base` is exported so the save cell can fall back to
    it when the user hasn't clicked a folder in the browser
    (`mo.ui.file_browser` requires an explicit click on an entry to
    register a selection; `initial_path` alone doesn't count).

    Disabled in the WASM build: there's no filesystem to browse or write
    to, and the Sankey→PNG path needs kaleido (no Pyodide build). Viewers
    can right-click any figure to save it instead."""
    if is_wasm:
        save_btn = None
        save_default_base = None
        save_dir_picker = None
        save_run_name = None
        _ui = mo.callout(
            mo.md(
                "Offline figure/table export is disabled in the browser build "
                "(no filesystem, and the Sankey PNG export needs kaleido). "
                "Right-click any figure to save it as PNG."
            ),
            kind="info",
        )
    else:
        _repo_root = Path(__file__).resolve().parents[1]
        save_default_base = _repo_root / "_site" / "exports"
        save_default_base.mkdir(parents=True, exist_ok=True)
        save_dir_picker = mo.ui.file_browser(
            initial_path=save_default_base,
            selection_mode="directory",
            multiple=False,
            restrict_navigation=False,
            label=f"Base folder (pick one, or leave unselected to use {save_default_base})",
        )
        save_run_name = mo.ui.text(
            value="run",
            label="Run name (subfolder)",
            placeholder="e.g. post-mm-default",
            full_width=False,
        )
        save_btn = mo.ui.run_button(label="Save outputs", kind="success")
        _ui = mo.vstack([save_dir_picker, save_run_name, save_btn])
    _ui
    return save_btn, save_default_base, save_dir_picker, save_run_name


@app.cell(hide_code=True)
def _(
    available_pr_types,
    available_topics,
    cohort,
    cohort_summary,
    cycle_count_fig,
    cycle_table,
    focus_nodes,
    is_wasm,
    json,
    make_breakdown_fig,
    make_focus_sankey_static,
    mo,
    path_table,
    plt,
    pr_type_checks,
    re,
    review_activity_fig,
    sankey_height,
    save_btn,
    save_default_base,
    save_dir_picker,
    save_run_name,
    show_cycle_branches,
    slice_by_author_cohort,
    slice_by_lines_bucket,
    slice_by_pr_type,
    slice_by_topic_area,
    stage_dist_fig,
    stage_medians,
    stage_share_fig,
    topic_checks,
):
    """Write all selected outputs to `<base>/<name>/` when the Save
    button is clicked. The cell short-circuits via `mo.stop` while the
    button is idle, so it costs nothing on every other re-render.

    Plotly → PNG goes through kaleido (Chromium-backed). If that fails
    on this machine — kaleido not installed, no Chrome available, etc.
    — the Sankey falls back to self-contained HTML so the export still
    completes. Disabled entirely in the WASM build (no filesystem / no
    kaleido); the stop below runs before any `save_btn` (None there) use."""
    mo.stop(
        is_wasm,
        mo.md("_Offline export is disabled in the browser build._"),
    )
    mo.stop(
        not save_btn.value,
        mo.md("_Pick a base folder, name the run, then click **Save outputs**._"),
    )

    # `mo.ui.file_browser` only registers a selection when the user
    # clicks a directory entry — `initial_path` alone doesn't count.
    # Fall back to `save_default_base` when nothing is selected so the
    # button "just works" with the default `_site/exports/` location.
    _entries = save_dir_picker.value
    if _entries:
        _base = save_dir_picker.path(index=0)
    else:
        _base = save_default_base
    _name = (save_run_name.value or "run").strip() or "run"
    _out = _base / _name
    _out.mkdir(parents=True, exist_ok=True)

    def _slug(s):
        return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower() or "untitled"

    _written = []
    _errors = []

    def _save_plotly(fig, name):
        png_path = _out / f"{name}.png"
        try:
            fig.write_image(
                str(png_path),
                width=1400,
                height=int(sankey_height.value) + 40,
                scale=2,
            )
            _written.append(png_path.name)
        except Exception as e:
            # kaleido / Chrome unavailable — write a self-contained HTML
            # so each focus state is still preserved as a distinct file.
            html_path = _out / f"{name}.html"
            fig.write_html(str(html_path), include_plotlyjs="cdn")
            _written.append(f"{html_path.name}  (PNG failed: {type(e).__name__})")
            _errors.append((name, repr(e)))

    def _save_mpl(fig, name):
        png_path = _out / f"{name}.png"
        fig.savefig(png_path, dpi=200, bbox_inches="tight")
        _written.append(png_path.name)

    # §1b: one figure per focus-dropdown state (None = "All segments")
    for _fn in [None] + focus_nodes:
        _label = "all-segments" if _fn is None else _slug(_fn)
        _save_plotly(make_focus_sankey_static(_fn), f"1b__{_label}")

    # §1c: one figure per breakdown node
    for _node in focus_nodes:
        _fig = make_breakdown_fig(_node)
        _save_mpl(_fig, f"1c__{_slug(_node)}")
        plt.close(_fig)

    # §2 / §3 / §4 / §6 — already built; just dump current state
    _save_mpl(stage_dist_fig, "2_per-stage-distributions")
    _save_mpl(stage_share_fig, "3_stage-share")
    _save_mpl(cycle_count_fig, "4_queue-cycles")
    _save_mpl(review_activity_fig, "6_review-activity")

    # Tables → CSV
    for _csv_name, _df in (
        ("cohort_summary", cohort_summary),
        ("3_stage_medians", stage_medians),
        ("4_cycle_table", cycle_table),
        ("5_slice_by_pr_type", slice_by_pr_type),
        ("5_slice_by_lines_bucket", slice_by_lines_bucket),
        ("5_slice_by_topic_area", slice_by_topic_area),
        ("5_slice_by_author_cohort", slice_by_author_cohort),
        ("7_path_table", path_table),
    ):
        _path = _out / f"{_csv_name}.csv"
        _df.write_csv(_path)
        _written.append(_path.name)

    # settings.json — enough to reproduce the cohort exactly
    _settings = {
        "cohort": cohort.value,
        "show_cycle_branches": bool(show_cycle_branches.value),
        "sankey_height": int(sankey_height.value),
        "topics_selected": [
            t for t, v in zip(available_topics, topic_checks.value) if v
        ],
        "pr_types_selected": [
            t for t, v in zip(available_pr_types, pr_type_checks.value) if v
        ],
    }
    (_out / "settings.json").write_text(json.dumps(_settings, indent=2))
    _written.append("settings.json")

    _err_block = ""
    if _errors:
        _err_block = "\n\n**Warnings:**\n" + "\n".join(
            f"- `{n}`: {msg}" for n, msg in _errors
        )

    mo.md(
        f"### Wrote {len(_written)} files to `{_out}`\n\n"
        + "\n".join(f"- `{f}`" for f in _written)
        + _err_block
    )
    return


if __name__ == "__main__":
    app.run()
