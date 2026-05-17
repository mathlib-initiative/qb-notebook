"""Story A — Anatomy of a merge."""

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
    # Anatomy of a merge

    For each PR in the selected cohort we reconstruct a five-stage
    milestone timeline:

    1. **opened** — `gh_created_at`
    2. **first touch** — earliest non-author, non-bot
       `REVIEW_*` / `ISSUE_COMMENTED` event (`first_review_touch`)
    3. **maintainer-merge applied** — first `LABELED(maintainer-merge)`
       (~99 % attributed to a human via the bot-trigger heuristic)
    4. **ready-to-merge applied** — first `LABELED(ready-to-merge)`
       (bors r+ accepted; `delegated` label tracked alongside)
    5. **merged** — bors-aware effective merge timestamp
       (`expr_merged_at_effective`)

    The lifecycle Sankey shows how the cohort fans out across these
    milestones plus the three terminal states (merged, closed unmerged,
    still open). For each stage we then plot the duration distribution
    on log-spaced bins — PR open-durations are close to log-normal
    (see `pr_open_durations.ipynb`), so log binning + lognormal-fit
    overlays make the shape readable.

    All cohort-dependent cells refire on the dropdown change.
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
    import plotly.graph_objects as go
    import polars as pl
    from scipy.stats import lognorm

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
        label_intervals,
        labels_active_at,
        load_pr_interval_data,
        lognorm,
        np,
        pipeline_stages,
        pl,
        plt,
        pr_type,
        pr_type_order,
        queue_window_intervals,
        size_buckets,
        timezone,
    )


@app.cell
def _(Path, datetime, load_pr_interval_data, pl, timezone):
    """Load parquet + join `core_user` so PRs carry `author_login`."""
    _data_dir = Path(__file__).resolve().parents[1] / "data"
    data = load_pr_interval_data(_data_dir)
    events = data["events"]
    prs_raw = data["prs"]
    queue_windows = data["queue_windows"]
    inline_comments = data.get("inline_comments")

    _users = pl.read_parquet(_data_dir / "core_user.parquet")
    users = _users.select(
        pl.col("id").alias("author_id"),
        pl.col("github_login").alias("author_login"),
    )
    asof = datetime.now(tz=timezone.utc)
    return asof, events, inline_comments, prs_raw, queue_windows, users


@app.cell
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


@app.cell
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


@app.cell
def _(mo):
    """Cohort dropdown. Post-MM is the default since the `maintainer-merge`
    label only exists from 2024-02-15, so the headline funnel is only
    meaningful for PRs whose lifecycle could have included that stage.
    All-time is included for the long-baseline comparison."""
    cohort = mo.ui.dropdown(
        options={
            "post-MM (2024-02-15+)": "post_mm",
            "2024 (post-MM)": "2024",
            "2025": "2025",
            "all-time": "all",
        },
        value="post-MM (2024-02-15+)",
        label="Cohort",
    )

    show_cycle_branches = mo.ui.checkbox(
        value=False,
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


@app.cell
def _(cohort, datetime, pl, prs_enriched, timezone):
    """Apply the cohort filter on `gh_created_at`."""
    _COHORT_BOUNDS = {
        "post_mm": (datetime(2024, 2, 15, tzinfo=timezone.utc), None),
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
    cohort_label = cohort.value
    return cohort_label, prs_cohort


@app.cell
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


@app.cell
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


@app.cell
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


@app.cell
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

    summary = pl.DataFrame(
        [
            {
                "milestone": "opened (cohort total)",
                "count": _n,
                "pct_of_cohort": "100.0%",
            },
            {
                "milestone": "first non-author touch",
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
    summary
    return


@app.cell
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


@app.cell
def _(go, pr_pipeline, sankey_height, show_cycle_branches):
    """Build the Sankey from per-PR path classifications.

    Each PR contributes one path = sequence of nodes; the function below
    accumulates per-edge counts and hands them to plotly. The cycle-branch
    toggle inserts "2 queue cycles" / "3+ queue cycles" nodes after
    `1 queue cycle` for PRs that needed more than one review round —
    `1 queue cycle` itself (the first-touch milestone) stands in for
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
            steps.append("closed unmerged")
        else:
            steps.append("still open")
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
        "closed unmerged",
        "still open",
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
        "closed unmerged": "#a33",
        "still open": "#bbb",
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
        "1 queue cycle": 0.14,
        "2 queue cycles": 0.30,
        "3+ queue cycles": 0.46,
        "maintainer-merge": 0.62,
        "bors r+": 0.78,
        "delegated": 0.78,
        "merged": 0.999,
        "closed unmerged": 0.999,
        "still open": 0.999,
    }
    _NODE_Y = {
        "opened": 0.5,
        "1 queue cycle": 0.05,
        "2 queue cycles": 0.05,
        "3+ queue cycles": 0.05,
        "maintainer-merge": 0.22,
        "bors r+": 0.50,
        "delegated": 0.72,
        "merged": 0.30,
        "closed unmerged": 0.72,
        "still open": 0.95,
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

    sankey_fig = go.Figure(
        data=[
            go.Sankey(
                # `freeform` lets dragged nodes stay where you put them
                # instead of snapping back to the column grid; combined
                # with the taller figure + extra padding it gives enough
                # room to hand-tune label overlaps in the cycle column.
                arrangement="freeform",
                node=dict(
                    label=_nodes,
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
        title="Lifecycle flow — counts of PRs traversing each segment",
        font=dict(size=12),
        height=int(sankey_height.value),
        margin=dict(l=10, r=10, t=60, b=20),
    )
    sankey_fig
    return


@app.cell
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


@app.cell
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
            steps.append("closed unmerged")
        else:
            steps.append("still open")
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
        "closed unmerged",
        "still open",
    ]
    _present = {n for edge in _edge_counts for n in edge}
    _nodes = [n for n in _node_order if n in _present]
    _node_idx = {n: i for i, n in enumerate(_nodes)}

    _node_colors_map = {
        "opened": "#888",
        "1 queue cycle": "#4a90d9",
        "2 queue cycles": "#6ea7d8",
        "3+ queue cycles": "#3f7fbe",
        "maintainer-merge": "#c63",
        "bors r+": "#73a946",
        "delegated": "#9d72c7",
        "merged": "#2c7a2c",
        "closed unmerged": "#a33",
        "still open": "#bbb",
    }

    # Explicit node positions (x = column, y = vertical) to control
    # ribbon paths — same layout as §1; see that cell for the rationale.
    _NODE_X = {
        "opened": 0.001,
        "1 queue cycle": 0.14,
        "2 queue cycles": 0.30,
        "3+ queue cycles": 0.46,
        "maintainer-merge": 0.62,
        "bors r+": 0.78,
        "delegated": 0.78,
        "merged": 0.999,
        "closed unmerged": 0.999,
        "still open": 0.999,
    }
    _NODE_Y = {
        "opened": 0.5,
        "1 queue cycle": 0.05,
        "2 queue cycles": 0.05,
        "3+ queue cycles": 0.05,
        "maintainer-merge": 0.22,
        "bors r+": 0.50,
        "delegated": 0.72,
        "merged": 0.30,
        "closed unmerged": 0.72,
        "still open": 0.95,
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
        n_in = _sum_into(node)
        n_out = _sum_from(node)
        if n_in == 0:
            return f"<b>{node}</b> — {n_out:,} out"
        if n_out == 0:
            return f"<b>{node}</b> — {n_in:,} in"
        return f"<b>{node}</b> — {n_in:,} in · {n_out:,} out"

    def _node_edges(node: str) -> set:
        return {(a, b) for (a, b) in _edges_list if a == node or b == node}

    _n_opened = _sum_from("opened")
    _focuses = [
        (
            "All segments",
            set(_edges_list),
            f"<b>Full lifecycle</b> — {_n_opened:,} PRs",
        ),
    ]
    for _n in _nodes:
        _focuses.append((_n, _node_edges(_n), _node_title(_n)))

    _DIM = "rgba(200,200,200,0.18)"
    _BRIGHT = "rgba(74,144,217,0.5)"

    def _colors_for(focus_edges: set) -> list[str]:
        return [_BRIGHT if (a, b) in focus_edges else _DIM for (a, b) in _edges_list]

    sankey_focus_fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="freeform",
                node=dict(
                    label=_nodes,
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
        title=_focuses[0][2],
        updatemenus=[
            dict(
                buttons=_buttons,
                direction="down",
                showactive=True,
                x=0.01,
                y=1.18,
                xanchor="left",
                yanchor="top",
                pad=dict(r=10, t=10),
                bgcolor="white",
                bordercolor="#bbb",
            )
        ],
        height=int(sankey_height.value),
        margin=dict(l=10, r=10, t=100, b=20),
        font=dict(size=12),
    )
    sankey_focus_fig
    return


@app.cell
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
    """)
    return


@app.cell
def _(lognorm, np, pl, plt, pr_pipeline):
    """4-panel log-binned histogram + lognormal fit, one panel per stage.
    Returns the figure as the cell output."""
    _STAGES = [
        ("seconds_open_to_first_touch", "open → first touch"),
        (
            "seconds_first_touch_to_maintainer_merge",
            "first touch → maintainer-merge",
        ),
        (
            "seconds_maintainer_merge_to_ready_to_merge",
            "MM → ready-to-merge",
        ),
        ("seconds_ready_to_merge_to_merged", "RTM → merged"),
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

    _fig, _axes = plt.subplots(2, 2, figsize=(13, 8))
    for _ax, (_col, _title) in zip(_axes.ravel(), _STAGES):
        _vals = (
            pr_pipeline.filter(pl.col(_col).is_not_null())
            .select((pl.col(_col) / 86400.0).alias("days"))
            .get_column("days")
            .to_numpy()
        )
        _draw(_ax, _vals, _title)
    _fig.suptitle("Per-stage duration distributions (log bins + lognormal fit)")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Stage share of total time-to-merge

    For merged PRs only, decompose each PR's open→merged duration into
    the four stage segments. The stacked horizontal bar below shows
    cohort-level shares at the median PR (i.e., the median of each
    stage divided by the sum of medians) so the numbers don't blow up
    when a single PR spends 100× the median in one stage.
    """)
    return


@app.cell
def _(np, pl, plt, pr_pipeline):
    """Stage shares of TTM at the median, plus a per-stage median table."""
    _STAGE_COLS = [
        ("open → first touch", "seconds_open_to_first_touch", "#4a90d9"),
        ("first touch → MM", "seconds_first_touch_to_maintainer_merge", "#c63"),
        ("MM → RTM", "seconds_maintainer_merge_to_ready_to_merge", "#73a946"),
        ("RTM → merged", "seconds_ready_to_merge_to_merged", "#9d72c7"),
    ]

    _merged_only = pr_pipeline.filter(pl.col("is_merged"))
    _rows = []
    _med_days = []
    for _label, _col, _color in _STAGE_COLS:
        _vals = (
            _merged_only.filter(pl.col(_col).is_not_null())
            .select((pl.col(_col) / 86400.0).alias("days"))
            .get_column("days")
            .to_numpy()
        )
        _med = float(np.median(_vals)) if _vals.size else 0.0
        _p90 = float(np.percentile(_vals, 90)) if _vals.size else 0.0
        _med_days.append(_med)
        _rows.append(
            {
                "stage": _label,
                "n_prs_with_stage": int(_vals.size),
                "median_days": round(_med, 2),
                "p90_days": round(_p90, 2),
            }
        )

    stage_medians = pl.DataFrame(_rows)

    _fig, _ax = plt.subplots(figsize=(11, 1.6))
    _total = sum(_med_days) or 1.0
    _left = 0.0
    for (_label, _col, _color), _val in zip(_STAGE_COLS, _med_days):
        _share = _val / _total
        _ax.barh([0], [_share], left=_left, color=_color, edgecolor="white")
        _ax.text(
            _left + _share / 2,
            0,
            f"{_label}\n{_val:.2f}d ({_share:.0%})",
            ha="center",
            va="center",
            fontsize=8,
            color="white" if _share > 0.1 else "black",
        )
        _left += _share
    _ax.set_xlim(0, 1)
    _ax.set_yticks([])
    _ax.set_xlabel("Share of summed median stage durations")
    _ax.set_title(f"Stage share of merged-PR TTM (median total: {_total:.2f} days)")
    _fig.tight_layout()
    _fig
    return (stage_medians,)


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
    _x = _raw_counts.get_column("n_queue_cycles_before_mm").to_numpy()
    _y = _raw_counts.get_column("n_prs").to_numpy()

    _fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(13, 4))
    _ax1.bar(_x, _y, color="#4a90d9", edgecolor="white")
    _ax1.set_xlabel("queue cycles before MM")
    _ax1.set_ylabel("merged PRs")
    _ax1.set_title("Cycle count distribution (merged)")

    _bucket_x = cycle_table.get_column("cycle_bucket").to_list()
    _ttm = cycle_table.get_column("median_ttm_days").to_numpy()
    _s2 = cycle_table.get_column("median_stage2_days").to_numpy()
    _xs = np.arange(len(_bucket_x))
    _ax2.bar(_xs - 0.2, _ttm, width=0.4, label="median TTM", color="#73a946")
    _ax2.bar(_xs + 0.2, _s2, width=0.4, label="median stage 2 (touch→MM)", color="#c63")
    _ax2.set_xticks(_xs)
    _ax2.set_xticklabels(_bucket_x)
    _ax2.set_xlabel("queue-cycle bucket")
    _ax2.set_ylabel("days (median)")
    _ax2.set_title("TTM and stage-2 latency by cycle bucket")
    _ax2.legend()
    _fig.tight_layout()
    _fig
    return (cycle_table,)


@app.cell
def _(cycle_table, mo):
    mo.md("**Cycle-bucket detail (merged PRs):**")
    cycle_table
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
            ("seconds_open_to_first_touch", "open→touch"),
            ("seconds_first_touch_to_maintainer_merge", "touch→MM"),
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
    mo.md("**By PR type:**")
    slice_stage_medians(
        pr_pipeline, slice_col="pr_type", slice_order=list(pr_type_order())
    )
    return


@app.cell
def _(lines_bucket_order, mo, pr_pipeline, slice_stage_medians):
    mo.md("**By lines bucket:**")
    slice_stage_medians(
        pr_pipeline, slice_col="lines_bucket", slice_order=lines_bucket_order
    )
    return


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
    topic_table = slice_stage_medians(
        _enriched.filter(pl.col("topic_area").is_in(_top_areas)),
        slice_col="topic_area",
        slice_order=_top_areas,
    )
    mo.md("**By topic area (top 10 by merged-PR count):**")
    topic_table
    return


@app.cell
def _(mo, pl, pr_pipeline, slice_stage_medians):
    """First PR vs returning. Session 11 showed first-PRs get *faster*
    median first touch; the downstream stages tell whether the funnel
    closes that gap or reopens one."""
    _df = pr_pipeline.with_columns(
        pl.when(pl.col("is_first_pr"))
        .then(pl.lit("first PR"))
        .otherwise(pl.lit("returning"))
        .alias("author_cohort_label")
    )
    mo.md("**By author cohort:**")
    slice_stage_medians(
        _df,
        slice_col="author_cohort_label",
        slice_order=["first PR", "returning"],
    )
    return


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
    dataset offers (comment bodies aren't exported). Distributions are
    log-binned over `1 + count` so PRs with zero comments still appear
    in the leftmost bin.
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
def _(inline_counts, np, pl, plt, pr_pipeline, review_counts):
    """3-panel review-activity distribution (merged PRs only)."""
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

    _fig, _axes = plt.subplots(1, 3, figsize=(15, 4))
    for _ax, (_col, _title) in zip(_axes, _SIGNALS):
        _vals = _merged.get_column(_col).to_numpy()
        _x = 1.0 + _vals.astype(float)  # log-binnable
        _lo = 1.0
        _hi = max(_x.max(), 2.0)
        _edges = np.logspace(np.log10(_lo), np.log10(_hi), 30)
        _ax.hist(_x, bins=_edges, color="#4a90d9", edgecolor="white")
        _ax.set_xscale("log")
        _ax.set_xlabel("1 + count (log scale)")
        _ax.set_title(
            f"{_title}\nmedian={int(np.median(_vals))}, p90={int(np.percentile(_vals, 90))}, "
            f"%zero={100 * (_vals == 0).mean():.0f}%"
        )
    _axes[0].set_ylabel("merged PRs")
    _fig.tight_layout()
    _fig
    return


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
    return


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
    - **Court-vs-author latency split** of the `first touch → MM` stage
      is deferred to Story B (latency decomposition). Here we surface
      only queue-cycle *count* as the queue-aware signal.
    """)
    return


if __name__ == "__main__":
    app.run()
