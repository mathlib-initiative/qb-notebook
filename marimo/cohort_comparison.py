"""Story A2 — cohort comparison."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")

# Side-by-side version of `anatomy_of_a_merge.py`: instead of one cohort at a
# time, define up to six and compare their funnels and per-stage duration
# distributions in a single view. Run with
# `uv run marimo edit marimo/cohort_comparison.py`.
#
# The cohort machinery (specs, filtering, aggregates) lives in
# `qb_notebook.cohorts`; this file is layout, controls, and plotting.


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
        # parquet). No plotly here — every figure in this notebook is
        # matplotlib. tzdata: Pyodide ships no system zoneinfo database, so
        # any materialization of tz-aware datetimes raises
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


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Cohort comparison

    `anatomy_of_a_merge.py` reconstructs the milestone timeline —
    **opened → first review → `maintainer-merge` → `ready-to-merge` →
    merged** — for *one* cohort at a time. This notebook runs the same
    reconstruction over **several cohorts at once** and puts them
    side by side, so questions of the form "has X changed?" or "does
    Y behave differently from Z?" are one control-panel edit rather
    than two notebook sessions and a screenshot.

    Each cohort is an independent set of filters:

    - **when** the PR was opened — a preset window, or a free-form date
      range,
    - **which topic** (`t-*`) labels it ever carried,
    - **which PR type** its title parses to (conventional-commit
      prefix),
    - **which size bucket** (added + deleted lines) it falls in,
    - **whether the author** was posting their first PR.

    Cohorts are free to overlap, nest, or partition — nothing checks or
    assumes otherwise. Three views follow: the **funnel** (what share of
    each cohort reached each milestone), the **per-stage duration
    distributions** overlaid on shared log bins, and the **stage mix**
    (where the median PR's time goes).

    A caveat that applies throughout: cohorts drawn from different eras
    are not like-for-like experiments. Mathlib's review workflow itself
    changed — bors from 2022-08, the `maintainer-merge` label only from
    2024-02-15 — so pre-2024 cohorts have structurally empty
    maintainer-merge stages, and recent windows are right-censored (PRs
    opened near the snapshot haven't finished their lifecycle yet).
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

    from datetime import date, timedelta

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    # Match the 3x plotly modebar PNG resolution used by the sibling
    # notebooks (default dpi is 100). marimo scales the displayed image
    # width inversely so the figure looks the same on screen but the
    # right-click-saved PNG is at the higher pixel density.
    plt.rcParams["figure.dpi"] = 300

    from qb_notebook.cohorts import (
        AUTHOR_ANY,
        AUTHOR_FIRST_TIME,
        AUTHOR_RETURNING,
        DEFAULT_STAGES,
        MAINTAINER_FIRST_SECONDS,
        STAGE_FIRST_TOUCH_TO_MM,
        TOPIC_NONE,
        CohortSpec,
        Stage,
        cohort_frames,
        milestone_summary,
        stage_quantiles,
        stage_series,
        with_unique_names,
    )
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
        MATHLIB_LABEL_RETIRED_AT,
        label_intervals,
        pipeline_stages,
    )
    from qb_notebook.wasm_io import load_slimmed_data

    return (
        AUTHOR_ANY,
        AUTHOR_FIRST_TIME,
        AUTHOR_RETURNING,
        CohortSpec,
        DEFAULT_LINES_BREAKS,
        DEFAULT_STAGES,
        MAINTAINER_FIRST_SECONDS,
        MATHLIB_LABEL_RETIRED_AT,
        Path,
        STAGE_FIRST_TOUCH_TO_MM,
        Stage,
        TOPIC_NONE,
        author_cohort,
        bucket_labels,
        cohort_frames,
        date,
        expr_merged_at_effective,
        expr_merged_to_master,
        label_intervals,
        load_pr_interval_data,
        load_slimmed_data,
        milestone_summary,
        np,
        pipeline_stages,
        pl,
        plt,
        pr_type,
        pr_type_order,
        size_buckets,
        stage_quantiles,
        stage_series,
        timedelta,
        with_unique_names,
    )


@app.cell(hide_code=True)
def _(Path, is_wasm, load_pr_interval_data, load_slimmed_data, mo, pl):
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
    return asof, events, prs_raw, users


@app.cell(hide_code=True)
def _(
    author_cohort,
    expr_merged_at_effective,
    expr_merged_to_master,
    pl,
    pr_type,
    prs_raw,
    size_buckets,
    users,
):
    """Filter to human PRs against `master`, attach shape + cohort columns,
    and derive a bors-aware `merged_at_effective` (null for non-merges).
    Identical to the `anatomy_of_a_merge.py` enrichment — the cohort filters
    downstream read `pr_type` / `lines_bucket` / `is_first_pr` from here.

    Note that `is_first_pr` is relative to *this* frame (every human PR to
    master), not to any one cohort's window: "first-time author" means the
    author's first mathlib PR, so a first-timer stays a first-timer no matter
    which window you slice."""
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
    )
    return (prs_enriched,)


@app.cell(hide_code=True)
def _(MATHLIB_LABEL_RETIRED_AT, asof, events, label_intervals, pl):
    """`t-*` label intervals over the full timeline (open intervals run to
    `asof`). Same convention as `anatomy_of_a_merge.py`: intervals are *not*
    clamped at PR close, because GitHub doesn't auto-remove labels on close.

    The topic filter only asks "did this PR ever carry label L", so the
    interval ends don't matter here — but reusing the same reconstruction
    keeps the cohort definitions comparable with the other notebooks."""
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
    return (t_intervals,)


@app.cell(hide_code=True)
def _(events, pipeline_stages, pl, prs_enriched):
    """Per-PR milestone frame for the **whole corpus**, computed once.

    `pipeline_stages` is a pure per-PR transform — a PR's first-touch / MM /
    RTM timestamps don't depend on which other PRs are in the frame — so
    running it once over every PR and slicing the result per cohort gives
    exactly the same numbers as re-running it per cohort, at a fraction of
    the cost. That matters here: the cohort controls refire on every edit,
    and this cell deliberately doesn't depend on them."""
    _attrs = prs_enriched.select(
        pl.col("id").alias("pull_request_id"),
        "number",
        "pr_type",
        "lines_bucket",
        "is_first_pr",
        "is_merged",
        "is_closed",
    )
    pr_pipeline_all = pipeline_stages(
        prs_enriched,
        events,
        pr_merged_col="merged_at_effective",
    ).join(_attrs, on="pull_request_id", how="inner")
    return (pr_pipeline_all,)


@app.cell(hide_code=True)
def _(
    DEFAULT_LINES_BREAKS,
    TOPIC_NONE,
    bucket_labels,
    pl,
    pr_type_order,
    prs_enriched,
    t_intervals,
):
    """Filter menus + the data's own PR-open span (which bounds the date
    pickers). Menu options are sourced from the full mathlib history rather
    than any one cohort, so they don't shift as cohorts are edited."""
    available_topics = sorted(
        t_intervals.get_column("label_name").unique().to_list()
    ) + [TOPIC_NONE]
    available_pr_types = pr_type_order()
    available_size_buckets = bucket_labels(DEFAULT_LINES_BREAKS)

    _span = prs_enriched.select(
        pl.col("gh_created_at").min().alias("lo"),
        pl.col("gh_created_at").max().alias("hi"),
    ).row(0)
    data_lo_day, data_hi_day = _span[0].date(), _span[1].date()
    return (
        available_pr_types,
        available_size_buckets,
        available_topics,
        data_hi_day,
        data_lo_day,
    )


@app.cell(hide_code=True)
def _(asof, date, timedelta):
    """Cohort presets, the slot palette, and the slot count.

    Presets map to **inclusive** day bounds; `None` means "open on this
    side", resolved against the data's own span when the spec is built.
    Relative windows are anchored to `asof` (the data snapshot), not
    `date.today()` — same reasoning as `anatomy_of_a_merge.py`: this
    notebook is published as a frozen WASM export, so a wall-clock anchor
    would drift past the last observed event.

    `PRESET_CUSTOM` is the escape hatch: pick it and the slot's date-range
    picker takes over. The preset dropdown never rewrites the picker, so
    editing one cohort can't disturb another's dates.

    Colors are the first six slots of the validated categorical palette
    (worst adjacent CVD ΔE 9.1, normal-vision ΔE 19.6), and are assigned by
    **slot index**, so a cohort keeps its color when others are added or
    removed. Line styles repeat the identity as a second, color-free
    channel."""
    _snapshot_day = asof.date()
    PRESET_CUSTOM = "custom"
    COHORT_PRESETS = {
        "post_mm_to_90d": (
            "post-MM → 90d ago",
            date(2024, 2, 15),
            _snapshot_day - timedelta(days=91),
        ),
        "last_90d": ("last 90 days", _snapshot_day - timedelta(days=90), None),
        "last_365d": ("last 365 days", _snapshot_day - timedelta(days=365), None),
        "post_mm": ("post-MM (2024-02-15+)", date(2024, 2, 15), None),
        "pre_mm": ("pre-MM (pre-2024-02-15)", None, date(2024, 2, 14)),
        "pre_bors": ("pre-bors (pre-2022-08-01)", None, date(2022, 7, 31)),
        "bors_pre_mm": (
            "bors → pre-MM (2022-08-01 to 2024-02-14)",
            date(2022, 8, 1),
            date(2024, 2, 14),
        ),
        "2024": ("2024 (post-MM)", date(2024, 2, 15), date(2024, 12, 31)),
        "2025": ("2025", date(2025, 1, 1), date(2025, 12, 31)),
        "2026": ("2026 (to snapshot)", date(2026, 1, 1), None),
        "all": ("all-time", None, None),
        PRESET_CUSTOM: ("custom (use the date range below)", None, None),
    }

    MAX_COHORTS = 6
    # Categorical slots 1-6; see references/palette.md in the dataviz skill.
    COHORT_COLORS = (
        "#2a78d6",  # blue
        "#eb6834",  # orange
        "#1baf7a",  # aqua
        "#eda100",  # yellow
        "#e87ba4",  # magenta
        "#008300",  # green
    )
    COHORT_LINESTYLES = ("-", "--", "-.", ":", (0, (3, 1, 1, 1)), (0, (5, 2)))
    # Default preset per slot: the first two reproduce the "has the recent
    # quarter drifted from the post-MM baseline?" comparison this notebook
    # was extracted from; the rest are year-over-year windows.
    SLOT_PRESETS = (
        "post_mm_to_90d",
        "last_90d",
        "2024",
        "2025",
        "2026",
        "all",
    )
    return (
        COHORT_COLORS,
        COHORT_LINESTYLES,
        COHORT_PRESETS,
        MAX_COHORTS,
        PRESET_CUSTOM,
        SLOT_PRESETS,
    )


@app.cell(hide_code=True)
def _(mo):
    """Add / remove buttons (click counters, same pattern as the All/None
    buttons in `anatomy_of_a_merge.py`) plus the chart-level options.

    The counters only change *how many* cohort slots are rendered — the slot
    controls themselves are built once, below, and are never rebuilt. That's
    deliberate: marimo resets a UI element when its defining cell re-runs, so
    a control panel that rebuilt on every add/remove would wipe the filters
    you'd just set up on the other cohorts."""
    add_cohort_btn = mo.ui.button(
        label="＋ add cohort", value=0, on_click=lambda v: v + 1
    )
    remove_cohort_btn = mo.ui.button(
        label="− remove cohort", value=0, on_click=lambda v: v + 1
    )
    panel_mode = mo.ui.radio(
        options={
            "histogram (share of PRs per bin)": "hist",
            "ECDF (cumulative share)": "ecdf",
        },
        value="histogram (share of PRs per bin)",
        label="Stage panels",
    )
    exclude_maintainer_first = mo.ui.checkbox(
        value=True,
        label="Stage 2: drop maintainer-first sign-offs (< 60 s)",
    )
    return (
        add_cohort_btn,
        exclude_maintainer_first,
        panel_mode,
        remove_cohort_btn,
    )


@app.cell(hide_code=True)
def _(MAX_COHORTS, add_cohort_btn, remove_cohort_btn):
    """How many cohort slots are live. Clamped to [2, MAX_COHORTS] — one
    cohort is `anatomy_of_a_merge.py`'s job, and past six the overlaid
    distributions stop being readable (and the palette stops guaranteeing
    colorblind-safe separation)."""
    n_cohorts = min(
        MAX_COHORTS,
        max(2, 2 + add_cohort_btn.value - remove_cohort_btn.value),
    )
    return (n_cohorts,)


@app.cell(hide_code=True)
def _(
    AUTHOR_ANY,
    AUTHOR_FIRST_TIME,
    AUTHOR_RETURNING,
    COHORT_PRESETS,
    MAX_COHORTS,
    SLOT_PRESETS,
    available_pr_types,
    available_size_buckets,
    available_topics,
    data_hi_day,
    data_lo_day,
    mo,
):
    """The per-slot controls, built once for all `MAX_COHORTS` slots.

    One `mo.ui.array` per control kind rather than one array of dictionaries
    per slot: marimo only synchronizes UI elements that are bound to a global
    name (or wrapped in an array / dictionary), and flat arrays keep that
    binding one level deep. `cohort_*_ui[i]` renders slot `i`'s widget;
    `cohort_*_ui.value[i]` reads it from another cell.

    This cell must not depend on `n_cohorts` — see the add/remove cell for
    why."""
    _preset_options = {label: key for key, (label, _, _) in COHORT_PRESETS.items()}
    _author_options = {
        "any author": AUTHOR_ANY,
        "first-time authors": AUTHOR_FIRST_TIME,
        "returning authors": AUTHOR_RETURNING,
    }

    cohort_name_ui = mo.ui.array(
        [
            mo.ui.text(value="", placeholder="(preset name)", label="name")
            for _ in range(MAX_COHORTS)
        ]
    )
    cohort_preset_ui = mo.ui.array(
        [
            mo.ui.dropdown(
                options=_preset_options,
                value=COHORT_PRESETS[_key][0],
                label="window",
            )
            for _key in SLOT_PRESETS[:MAX_COHORTS]
        ]
    )
    cohort_range_ui = mo.ui.array(
        [
            mo.ui.date_range(
                start=data_lo_day,
                stop=data_hi_day,
                value=(data_lo_day, data_hi_day),
                label="custom range",
            )
            for _ in range(MAX_COHORTS)
        ]
    )
    cohort_topic_ui = mo.ui.array(
        [
            mo.ui.multiselect(options=available_topics, value=[], label="topics")
            for _ in range(MAX_COHORTS)
        ]
    )
    cohort_type_ui = mo.ui.array(
        [
            mo.ui.multiselect(options=available_pr_types, value=[], label="PR types")
            for _ in range(MAX_COHORTS)
        ]
    )
    cohort_size_ui = mo.ui.array(
        [
            mo.ui.multiselect(
                options=available_size_buckets, value=[], label="size (lines)"
            )
            for _ in range(MAX_COHORTS)
        ]
    )
    cohort_author_ui = mo.ui.array(
        [
            mo.ui.dropdown(options=_author_options, value="any author", label="authors")
            for _ in range(MAX_COHORTS)
        ]
    )
    return (
        cohort_author_ui,
        cohort_name_ui,
        cohort_preset_ui,
        cohort_range_ui,
        cohort_size_ui,
        cohort_topic_ui,
        cohort_type_ui,
    )


@app.cell(hide_code=True)
def _(
    COHORT_COLORS,
    COHORT_PRESETS,
    CohortSpec,
    PRESET_CUSTOM,
    cohort_author_ui,
    cohort_name_ui,
    cohort_preset_ui,
    cohort_range_ui,
    cohort_size_ui,
    cohort_topic_ui,
    cohort_type_ui,
    data_hi_day,
    data_lo_day,
    n_cohorts,
    with_unique_names,
):
    """Read the live slots into `CohortSpec`s.

    Preset bounds are clamped into the data's own span so an open-ended or
    future-dated preset (e.g. `2026` against an older export) resolves to a
    real window instead of running past the snapshot. A preset entirely
    outside the span clamps to an inverted window, which selects nothing —
    that shows up honestly as `n = 0` in the summary rather than being
    silently widened.

    An empty multiselect means "no filter on this dimension" (not "nothing
    selected"), which is what makes a default slot the whole window."""
    _specs = []
    for _i in range(n_cohorts):
        _key = cohort_preset_ui.value[_i]
        if _key == PRESET_CUSTOM:
            _lo, _hi = cohort_range_ui.value[_i]
            _default_name = f"{_lo.isoformat()} → {_hi.isoformat()}"
        else:
            _label, _preset_lo, _preset_hi = COHORT_PRESETS[_key]
            _lo = data_lo_day if _preset_lo is None else max(_preset_lo, data_lo_day)
            _hi = data_hi_day if _preset_hi is None else min(_preset_hi, data_hi_day)
            _default_name = _label
        _specs.append(
            CohortSpec(
                name=(cohort_name_ui.value[_i] or "").strip() or _default_name,
                start=_lo,
                end=_hi,
                topics=tuple(cohort_topic_ui.value[_i]),
                pr_types=tuple(cohort_type_ui.value[_i]),
                lines_buckets=tuple(cohort_size_ui.value[_i]),
                author_cohort=cohort_author_ui.value[_i],
                color=COHORT_COLORS[_i % len(COHORT_COLORS)],
            )
        )
    cohort_specs = with_unique_names(_specs)
    return (cohort_specs,)


@app.cell(hide_code=True)
def _(
    add_cohort_btn,
    cohort_author_ui,
    cohort_name_ui,
    cohort_preset_ui,
    cohort_range_ui,
    cohort_size_ui,
    cohort_specs,
    cohort_topic_ui,
    cohort_type_ui,
    data_hi_day,
    data_lo_day,
    exclude_maintainer_first,
    mo,
    n_cohorts,
    panel_mode,
    remove_cohort_btn,
):
    """Render the control panel: one stacked block per live cohort slot.

    Stacked rather than tabbed or folded into an accordion, on purpose:
    both are UI state, and marimo rebuilds this cell's output on every
    control edit — so a tab strip would bounce back to cohort 1 and an
    accordion would snap shut each time you touched a filter."""
    _blocks = []
    for _i in range(n_cohorts):
        _spec = cohort_specs[_i]
        _blocks.append(
            mo.vstack(
                [
                    mo.md(
                        f'<span style="color:{_spec.color};font-size:1.4em">■</span> '
                        f"**{_i + 1}. {_spec.name}** — <small>{_spec.describe()}</small>"
                    ),
                    mo.hstack(
                        [
                            cohort_name_ui[_i],
                            cohort_preset_ui[_i],
                            cohort_range_ui[_i],
                            cohort_author_ui[_i],
                        ],
                        justify="start",
                        wrap=True,
                    ),
                    mo.hstack(
                        [
                            cohort_topic_ui[_i],
                            cohort_type_ui[_i],
                            cohort_size_ui[_i],
                        ],
                        justify="start",
                        wrap=True,
                    ),
                ],
                gap=0.25,
            )
        )

    mo.vstack(
        [
            mo.md(f"### Cohorts ({n_cohorts})"),
            mo.hstack(
                [add_cohort_btn, remove_cohort_btn],
                justify="start",
                wrap=True,
            ),
            mo.md(
                f"Empty topic / PR-type / size selections mean **no filter** on "
                f"that dimension. The custom date range applies only when the "
                f"window is set to *custom*. Data covers "
                f"{data_lo_day.isoformat()} → {data_hi_day.isoformat()}."
            ),
            mo.vstack(_blocks, gap=0.75),
            mo.md("**Chart options**"),
            mo.hstack(
                [panel_mode, exclude_maintainer_first],
                justify="start",
                wrap=True,
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(cohort_frames, cohort_specs, pr_pipeline_all, t_intervals):
    """Slice the corpus-wide milestone frame once per cohort."""
    frames = cohort_frames(
        pr_pipeline_all,
        cohort_specs,
        t_intervals=t_intervals,
    )
    return (frames,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 1. Funnel — what share of each cohort reached each milestone

    Milestone shares are of the **cohort total** (every PR matching the
    filters, merged or not), so they read as survival: the drop between
    two bars is what fell out in between. `merged` / `closed unmerged` /
    `still open` partition the cohort exactly.

    Two structural asymmetries to keep in mind when reading across
    cohorts:

    - **`maintainer-merge` only exists from 2024-02-15**, so any cohort
      reaching further back has a mechanically empty MM bar (and an
      empty stage-2/3 panel below).
    - **Recent cohorts are right-censored.** A window that runs to the
      snapshot contains PRs that simply haven't finished yet, which
      depresses `merged` and inflates `still open` relative to an
      older window of the same length.
    """)
    return


@app.cell(hide_code=True)
def _(cohort_specs, frames, milestone_summary, mo, pl):
    """Per-cohort summary table. `milestone_summary` returns fractions; we
    format them for display here and keep the raw frame for the chart."""
    cohort_summary = milestone_summary(frames)

    _pct_cols = [c for c in cohort_summary.columns if c.startswith("pct_")]
    _display = cohort_summary.with_columns(
        [(pl.col(c) * 100).round(1).alias(c) for c in _pct_cols]
        + [
            pl.col("median_ttm_days").round(2),
            pl.col("p90_ttm_days").round(2),
        ]
    ).rename({c: c.replace("pct_", "%_") for c in _pct_cols})

    mo.vstack(
        [
            mo.md(
                "### Cohort summary — counts, milestone shares (%), and "
                "time-to-merge (days, merged PRs only)"
            ),
            _display,
            mo.md(
                "\n".join(f"- **{_s.name}** — {_s.describe()}" for _s in cohort_specs)
            ),
        ]
    )
    return (cohort_summary,)


@app.cell(hide_code=True)
def _(COHORT_LINESTYLES, cohort_specs, cohort_summary, np, plt):
    """Grouped bars: milestone share per cohort. Values are direct-labeled
    only up to three cohorts — past that the labels collide, and the table
    above carries the exact numbers."""
    _MILESTONES = [
        ("pct_first_touch", "reached\nfirst review"),
        ("pct_maintainer_merge", "reached\nmaintainer-merge"),
        ("pct_ready_to_merge", "reached\nready-to-merge"),
        ("pct_merged", "merged"),
        ("pct_closed_unmerged", "closed\nunmerged"),
        ("pct_still_open", "still open"),
    ]
    _n = len(cohort_specs)
    _x = np.arange(len(_MILESTONES))
    _slot = 0.82 / _n

    funnel_fig, _ax = plt.subplots(figsize=(12, 4.6))
    for _i, _spec in enumerate(cohort_specs):
        _row = cohort_summary.filter(
            cohort_summary.get_column("cohort") == _spec.name
        ).row(0, named=True)
        _vals = [100 * (_row[_col] or 0.0) for _col, _ in _MILESTONES]
        _offset = (_i - (_n - 1) / 2) * _slot
        # Bar drawn narrower than its slot: the gap between adjacent bars is
        # surface, not a shared edge.
        _bars = _ax.bar(
            _x + _offset,
            _vals,
            width=_slot * 0.86,
            color=_spec.color,
            label=f"{_spec.name} (n={_row['n_prs']:,})",
            zorder=2,
        )
        if _n <= 3:
            _ax.bar_label(_bars, fmt="%.0f", fontsize=7, padding=2, color="#333")

    _ax.set_xticks(_x)
    _ax.set_xticklabels([_label for _, _label in _MILESTONES], fontsize=9)
    _ax.set_ylabel("share of cohort (%)")
    _ax.set_ylim(0, 105)
    _ax.grid(axis="y", color="#e6e6e3", linewidth=0.8, zorder=0)
    _ax.set_axisbelow(True)
    for _side in ("top", "right"):
        _ax.spines[_side].set_visible(False)
    _ax.legend(fontsize=8, ncols=min(3, _n), framealpha=0.9)
    _ax.set_title("Milestone reach by cohort")
    # Line styles carry cohort identity in the distribution panels below;
    # bars use position + legend, so nothing here depends on color alone.
    _ = COHORT_LINESTYLES
    funnel_fig.tight_layout()
    funnel_fig
    return (funnel_fig,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 2. Per-stage duration distributions

    Each panel is one stage of the pipeline — the delta between two
    consecutive milestones — with every cohort overlaid on **shared**
    log-spaced bins, so the panels compare shapes rather than binning
    artifacts. A stage delta is null when either endpoint is missing or
    the endpoints are out of order, so a panel only shows the subset of
    each cohort that actually traversed that stage — often far below
    the cohort size, and listed per cohort in the table below the
    panels. Zero-length deltas are dropped (nothing sits at 0 on a log
    axis).

    Dashed vertical rules mark each cohort's median. Switch the panels
    between the binned share and the ECDF with the control above: the
    histogram shows where the mass sits (bimodality, bot-latency
    spikes), the ECDF is easier to read for a shift in the middle of
    the distribution when several cohorts overlap.

    **Stage 2 (`first review → maintainer-merge`) is bimodal.** For
    ~45 % of post-MM PRs the very first non-author, non-bot event *is*
    the sign-off, so the bot applies the label within ~10–20 s and the
    stage collapses to the bot-latency floor. The checkbox above drops
    those maintainer-first PRs (< 60 s) so the panel reads as "how long
    the review cycle actually ran"; untick it to see both regimes.
    """)
    return


@app.cell(hide_code=True)
def _(
    DEFAULT_STAGES,
    MAINTAINER_FIRST_SECONDS,
    STAGE_FIRST_TOUCH_TO_MM,
    Stage,
    exclude_maintainer_first,
):
    """The stage list both the panels and the quantile table run on, with the
    maintainer-first floor patched into stage 2 when requested."""
    stages = tuple(
        Stage(
            _s.column,
            f"{_s.label} (≥ 60 s)",
            MAINTAINER_FIRST_SECONDS,
        )
        if (
            exclude_maintainer_first.value
            and _s.column == STAGE_FIRST_TOUCH_TO_MM.column
        )
        else _s
        for _s in DEFAULT_STAGES
    )
    return (stages,)


@app.cell(hide_code=True)
def _(
    COHORT_LINESTYLES,
    cohort_specs,
    frames,
    np,
    panel_mode,
    plt,
    stage_series,
    stages,
):
    """One panel per stage, every cohort overlaid; the last grid cell holds
    the shared legend (cohort colors also carry a line style, so identity
    survives a colorblind or greyscale reading)."""
    _mode = panel_mode.value

    def _draw(ax, series, title):
        _values = [_v for _, _v, _, _ in series if _v.size]
        if not _values or sum(_v.size for _v in _values) < 5:
            ax.set_title(f"{title}\n(insufficient data)", fontsize=9)
            ax.set_xscale("log")
            return
        _pool = np.concatenate(_values)
        _lo, _hi = float(_pool.min()), float(_pool.max())
        if _hi <= _lo:  # single distinct duration — widen so bins are valid
            _lo, _hi = _lo * 0.9, _hi * 1.1
        _edges = np.logspace(np.log10(_lo), np.log10(_hi), 41)

        for _name, _v, _color, _ls in series:
            if not _v.size:
                continue
            if _mode == "ecdf":
                _xs = np.sort(_v)
                _ys = np.arange(1, _xs.size + 1) / _xs.size
                ax.step(
                    _xs, _ys, where="post", color=_color, linestyle=_ls, linewidth=1.6
                )
            else:
                _counts, _ = np.histogram(_v, bins=_edges)
                ax.step(
                    _edges,
                    np.r_[_counts, _counts[-1]] / _v.size,
                    where="post",
                    color=_color,
                    linestyle=_ls,
                    linewidth=1.4,
                )
            ax.axvline(
                float(np.median(_v)),
                color=_color,
                linestyle="--",
                linewidth=0.9,
                alpha=0.85,
            )

        ax.set_xscale("log")
        ax.set_xlim(_lo, _hi)
        ax.set_title(title, fontsize=9)
        ax.set_xlabel("duration (days, log scale)", fontsize=8)
        ax.set_ylabel(
            "cumulative share" if _mode == "ecdf" else "share of PRs / bin", fontsize=8
        )
        ax.tick_params(labelsize=7)
        ax.grid(color="#eeeeec", linewidth=0.7)
        ax.set_axisbelow(True)
        for _side in ("top", "right"):
            ax.spines[_side].set_visible(False)

    stage_dist_fig, _axes = plt.subplots(2, 3, figsize=(14, 7.5))
    _flat = _axes.ravel()
    _legend_handles = []
    for _panel, (_ax, _stage) in enumerate(zip(_flat, stages)):
        _series = []
        for _i, _spec in enumerate(cohort_specs):
            _v = stage_series(frames[_spec.name], _stage).to_numpy()
            _ls = COHORT_LINESTYLES[_i % len(COHORT_LINESTYLES)]
            _series.append((_spec.name, _v, _spec.color, _ls))
            if _panel == 0:
                _legend_handles.append(
                    plt.Line2D(
                        [],
                        [],
                        color=_spec.color,
                        linestyle=_ls,
                        linewidth=1.8,
                        label=f"{_spec.name}  (n={frames[_spec.name].height:,})",
                    )
                )
        # No pooled n in the title: cohorts may overlap, so summing their
        # traversal counts would double-count PRs. Per-cohort n lives in
        # the legend (cohort size) and the table below (per stage).
        _draw(_ax, _series, _stage.label)

    for _ax in _flat[len(stages) :]:
        _ax.axis("off")
    _flat[-1].legend(
        handles=_legend_handles,
        loc="center",
        fontsize=9,
        frameon=False,
        title="cohort (n = PRs in cohort)",
        title_fontsize=9,
    )
    stage_dist_fig.suptitle(
        "Per-stage durations by cohort — "
        + ("ECDF" if _mode == "ecdf" else "share of PRs per log bin")
        + "; dashed rules are medians"
    )
    stage_dist_fig.tight_layout()
    stage_dist_fig
    return (stage_dist_fig,)


@app.cell(hide_code=True)
def _(cohort_specs, frames, mo, pl, stage_quantiles, stages):
    """Median / p90 per cohort × stage, pivoted so cohorts sit side by side.
    This is the table view for the panels above — every duration plotted
    there is readable here as a number (and the relief for the two palette
    slots that sit under 3:1 contrast on a light surface)."""
    stage_table = stage_quantiles(frames, stages=stages)

    _wide = (
        stage_table.with_columns(
            pl.col("p50_days").round(2),
            pl.col("p90_days").round(2),
        )
        .pivot(on="cohort", index="stage", values=["n", "p50_days", "p90_days"])
        # Keep the funnel order of `stages` rather than pivot's sort order.
        .join(
            pl.DataFrame(
                {"stage": [_s.label for _s in stages], "_order": range(len(stages))}
            ),
            on="stage",
            how="left",
        )
        .sort("_order")
        .drop("_order")
    )
    # Pivot emits metric-major columns (every cohort's n, then every
    # cohort's p50, ...); regroup them cohort-major so one cohort's three
    # numbers sit together. Names are filtered against the frame so this
    # survives polars' single-vs-multi-value pivot naming.
    _ordered = ["stage"] + [
        _col
        for _spec in cohort_specs
        for _metric in ("n", "p50_days", "p90_days")
        if (_col := f"{_metric}_{_spec.name}") in _wide.columns
    ]
    # Anything the name-matching missed still gets shown, rather than
    # silently disappearing from the table.
    _wide = _wide.select(_ordered + [_c for _c in _wide.columns if _c not in _ordered])

    mo.vstack(
        [
            mo.md(
                "### Stage durations — n traversed, median and p90 (days)\n\n"
                "Column suffixes are cohort names."
            ),
            _wide,
        ]
    )
    return (stage_table,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 3. Stage mix — where the median PR's time goes

    The four sequential stages stacked per cohort, each at its own
    median. This is the "which stage dominates, and does that ranking
    hold across cohorts?" view: a cohort can have the same total time
    to merge as another while spending it in a completely different
    place.

    **The bars are a sum of medians, not the median of a sum** — the
    stages have different populations (a PR that never got a
    `maintainer-merge` label contributes to stage 1 but not stage 2),
    and medians aren't additive. Read the segment *proportions*, not the
    bar total; the true end-to-end time-to-merge is the
    `median_ttm_days` column in §1's table.

    This section deliberately ignores the stage-2 checkbox and uses the
    **unfiltered** stage-2 population, so the decomposition stays
    comparable with `anatomy_of_a_merge.py` §2. Dropping maintainer-first
    sign-offs here would inflate stage 2 into the whole bar: it would be
    conditioned on the slow ~15 % of PRs while every other segment stayed
    unconditioned.
    """)
    return


@app.cell(hide_code=True)
def _(DEFAULT_STAGES, cohort_specs, frames, np, plt, stage_quantiles):
    """Horizontal stacked bars of the per-stage medians, one row per cohort.

    Recomputed off `DEFAULT_STAGES` rather than reusing the `stages` /
    `stage_table` pair from §2: the stage-2 floor belongs to the
    distribution panels, not to a decomposition where every other segment
    is unconditioned (see the note above).

    Segment colors are an ordinal one-hue ramp (blue 250 → 550), light to
    dark in funnel order — the segments are ordered stages, not independent
    categories, so a categorical palette would be the wrong encoding."""
    _SEGMENT_COLORS = ("#86b6ef", "#5598e7", "#2a78d6", "#1c5cab")
    _sequential = [_s for _s in DEFAULT_STAGES if _s.column != "seconds_open_to_merged"]
    _mix_table = stage_quantiles(frames, stages=_sequential, quantiles=(0.5,))

    _lookup = {
        (_row["cohort"], _row["stage"]): _row["p50_days"]
        for _row in _mix_table.iter_rows(named=True)
    }
    _names = [_spec.name for _spec in cohort_specs]
    _y = np.arange(len(_names))

    stage_mix_fig, _ax = plt.subplots(figsize=(12, 0.7 * len(_names) + 2.6))
    _left = np.zeros(len(_names))
    for _si, _stage in enumerate(_sequential):
        _vals = np.array(
            [float(_lookup.get((_name, _stage.label)) or 0.0) for _name in _names]
        )
        _ax.barh(
            _y,
            _vals,
            left=_left,
            height=0.62,
            color=_SEGMENT_COLORS[_si % len(_SEGMENT_COLORS)],
            edgecolor="white",
            linewidth=1.4,
            label=_stage.label,
            zorder=2,
        )
        _left = _left + _vals

    _totals = _left
    for _i, _name in enumerate(_names):
        _running = 0.0
        for _si, _stage in enumerate(_sequential):
            _v = float(_lookup.get((_name, _stage.label)) or 0.0)
            # Only label a segment wide enough to hold the text.
            if _totals[_i] and _v / _totals[_i] >= 0.09:
                _ax.text(
                    _running + _v / 2,
                    _i,
                    f"{_v:.2f}d",
                    ha="center",
                    va="center",
                    fontsize=7.5,
                    color="#0b0b0b" if _si < 2 else "white",
                )
            _running += _v
        _ax.text(
            _totals[_i] * 1.01,
            _i,
            f"  Σ {_totals[_i]:.2f}d",
            va="center",
            fontsize=8,
            color="#52514e",
        )

    _ax.set_yticks(_y)
    _ax.set_yticklabels(_names, fontsize=9)
    _ax.invert_yaxis()
    _ax.set_xlabel("summed median stage duration (days)")
    _ax.set_xlim(0, max(_totals.max() * 1.18, 1e-9))
    _ax.grid(axis="x", color="#e6e6e3", linewidth=0.8, zorder=0)
    _ax.set_axisbelow(True)
    for _side in ("top", "right", "left"):
        _ax.spines[_side].set_visible(False)
    # Legend below the axes: at six cohorts an in-axes legend lands on the
    # bottom bar.
    _ax.legend(
        fontsize=8,
        ncols=min(4, len(_sequential)),
        loc="upper center",
        bbox_to_anchor=(0.5, -0.12 - 0.6 / len(_names)),
        frameon=False,
    )
    _ax.set_title("Stage mix by cohort (sum of per-stage medians)")
    stage_mix_fig.tight_layout()
    stage_mix_fig
    return (stage_mix_fig,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Reading the comparison honestly

    Three failure modes worth naming, because every one of them has a
    plausible-looking chart attached:

    1. **Right-censoring.** Any window running to the snapshot is
       missing the slow tail of its own PRs — they haven't merged yet.
       Compare closed windows (e.g. two full quarters) when the metric
       is duration.
    2. **Composition shifts.** A cohort's median can move because the
       *mix* changed (more `feat:`, bigger PRs, more first-timers), not
       because review got slower. The per-cohort type / size / author
       filters exist to hold that constant: set two cohorts to the same
       size bucket and PR type, and vary only the window.
    3. **Regime changes.** `maintainer-merge` (2024-02) and bors
       (2022-08) both redraw the pipeline. Stages that didn't exist in
       a cohort's era read as "no data", not as "instant".
    """)
    return


if __name__ == "__main__":
    app.run()
