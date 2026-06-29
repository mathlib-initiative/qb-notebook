"""Theme 2 — Reviewer & maintainer load."""

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
    # Reviewer & maintainer load

    Who is doing the review work, how concentrated is it, and is the
    active-reviewer pool growing or shrinking?

    `maintainer-merge` and `ready-to-merge` are bot-applied labels;
    we attribute each one back to the human whose comment triggered it
    (most recent non-bot timeline event on the same PR within a
    configurable window). The bors-trigger proxy uses the same logic
    for `ready-to-merge`.
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

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    from qb_notebook.assignments import (
        classify_assignment_events,
        review_request_responses,
    )
    from qb_notebook.data_io import load_pr_interval_data
    from qb_notebook.pr_shape import author_cohort, pr_type, size_buckets
    from qb_notebook.review_states import (
        attribute_label_events,
        first_review_touch,
    )
    from qb_notebook.teams import load as load_teams
    from qb_notebook.wasm_io import load_slimmed_data, load_teams_snapshot

    return (
        Path,
        attribute_label_events,
        author_cohort,
        classify_assignment_events,
        first_review_touch,
        load_pr_interval_data,
        load_slimmed_data,
        load_teams,
        load_teams_snapshot,
        np,
        pl,
        plt,
        pr_type,
        review_request_responses,
        size_buckets,
    )


@app.cell
def _(
    Path,
    is_wasm,
    load_pr_interval_data,
    load_slimmed_data,
    mo,
    pl,
):
    # core_user maps author_id -> github_login so first_review_touch can
    # compare actor vs author. In WASM it ships as a slimmed table under
    # public/ (see scripts/export_wasm_data.py); locally it's read directly
    # off disk (not a load_pr_interval_data key).
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


@app.cell
def _(Path, is_wasm, load_teams, load_teams_snapshot, mo):
    # In WASM, team membership loads from the teams.json snapshot shipped under
    # public/ (see scripts/build_wasm_site.write_teams_snapshot); locally it
    # reads the sibling leanprover-community.github.io checkout, falling
    # through gracefully if that checkout is missing.
    if is_wasm:
        teams = load_teams_snapshot(str(mo.notebook_location() / "public"))
        teams_status = mo.md(
            f"Loaded team snapshot — "
            f"{len(teams.reviewers)} reviewers, "
            f"{len(teams.maintainers)} maintainers."
        )
    else:
        _candidate = (
            Path(__file__).resolve().parents[2] / "leanprover-community.github.io"
        )
        if _candidate.exists():
            teams = load_teams(_candidate, warn_on_unmatched=False)
            teams_status = mo.md(
                f"Loaded teams from `{_candidate}` — "
                f"{len(teams.reviewers)} reviewers, "
                f"{len(teams.maintainers)} maintainers, "
                f"{len(teams.unmatched)} unmatched."
            )
        else:
            teams = None
            teams_status = mo.callout(
                mo.md(
                    f"Sibling checkout `{_candidate}` not found — "
                    "team-membership overlays disabled."
                ),
                kind="warn",
            )
    teams_status
    return (teams,)


@app.cell
def _(mo):
    window_minutes = mo.ui.slider(
        start=1, stop=30, step=1, value=10, label="Attribution window (minutes)"
    )
    rolling_days = mo.ui.slider(
        start=7, stop=180, step=7, value=28, label="Rolling-window size (days)"
    )
    # `top_n` drives the disabled §2/§4 per-reviewer charts; kept defined (so
    # those sections re-enable cleanly) but left out of the visible controls.
    top_n = mo.ui.slider(start=5, stop=50, step=1, value=20, label="Top-N reviewers")
    mo.hstack([window_minutes, rolling_days])
    return rolling_days, top_n, window_minutes


@app.cell
def _(attribute_label_events, events, window_minutes):
    """Run the attribution heuristic for both labels."""
    _window_seconds = int(window_minutes.value) * 60
    signoff_attr = attribute_label_events(
        events, "maintainer-merge", window_seconds=_window_seconds
    )
    bors_attr = attribute_label_events(
        events, "ready-to-merge", window_seconds=_window_seconds
    )
    return bors_attr, signoff_attr


@app.cell
def _(bors_attr, mo, np, pl, signoff_attr):
    """Coverage summary up front so the heuristic's reliability is visible."""

    def _stats(df: pl.DataFrame, label: str) -> dict:
        n = df.height
        n_attr = df.filter(pl.col("attributed")).height
        gaps = df.filter(pl.col("attributed"))["gap_seconds"].to_numpy()
        return {
            "label": label,
            "events": n,
            "attributed": n_attr,
            "pct_attributed": round(100 * n_attr / max(n, 1), 1),
            "median_gap_s": int(np.median(gaps)) if gaps.size else 0,
            "p90_gap_s": int(np.percentile(gaps, 90)) if gaps.size else 0,
        }

    coverage = pl.DataFrame(
        [_stats(signoff_attr, "maintainer-merge"), _stats(bors_attr, "ready-to-merge")]
    )
    mo.md(
        "### Attribution coverage\n"
        "Fraction of label events that map to a non-bot trigger event "
        "(`ISSUE_COMMENTED` / `REVIEW_*`) on the same PR within the "
        "configured window. The gap distribution shows how tight the "
        "trigger→label timing typically is."
    )
    coverage
    return


@app.cell
def _(bors_attr, pl, plt, signoff_attr):
    """Gap-seconds histogram — concentration near zero validates the heuristic."""
    _fig, _axes = plt.subplots(1, 2, figsize=(12, 3.5), sharey=True)
    for _ax, _df, _title in (
        (_axes[0], signoff_attr, "maintainer-merge"),
        (_axes[1], bors_attr, "ready-to-merge"),
    ):
        _gaps = _df.filter(pl.col("attributed"))["gap_seconds"].to_numpy()
        _ax.hist(_gaps, bins=60, color="#6aa3d8")
        _ax.set_xlabel("trigger → label gap (s)")
        _ax.set_title(_title)
    _axes[0].set_ylabel("Events")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1. Active reviewer count over time

    For each day, count distinct attributed reviewers who triggered a
    `maintainer-merge` in the trailing rolling window. Tracks whether
    the active-reviewer pool is growing or shrinking, independent of
    total volume.
    """)
    return


@app.cell
def _(pl):
    def rolling_distinct_actors(df: pl.DataFrame, window_days: int) -> pl.DataFrame:
        """Per calendar day, count distinct `actor` values in the trailing window."""
        if df.height == 0:
            return pl.DataFrame(
                {"day": [], "actors": []},
                schema={"day": pl.Date, "actors": pl.Int64},
            )
        df_day = df.with_columns(pl.col("at").dt.date().alias("day")).select(
            ["day", "actor"]
        )
        min_day = df_day["day"].min()
        max_day = df_day["day"].max()
        days_df = pl.DataFrame(
            {"day": pl.date_range(min_day, max_day, interval="1d", eager=True)}
        )
        per_actor_day = df_day.unique()
        joined = days_df.join(per_actor_day, how="cross").filter(
            (pl.col("day_right") <= pl.col("day"))
            & (pl.col("day_right") > pl.col("day").dt.offset_by(f"-{window_days}d"))
        )
        return (
            joined.group_by("day")
            .agg(pl.col("actor").n_unique().alias("actors"))
            .sort("day")
        )

    return (rolling_distinct_actors,)


@app.cell
def _(pl, rolling_days, rolling_distinct_actors, signoff_attr):
    _signoff_human = signoff_attr.filter(pl.col("attributed")).select(
        [pl.col("label_at").alias("at"), pl.col("inferred_actor").alias("actor")]
    )
    rolling_signoff = rolling_distinct_actors(_signoff_human, int(rolling_days.value))
    return (rolling_signoff,)


@app.cell
def _(plt, rolling_days, rolling_signoff):
    _fig, _ax = plt.subplots(figsize=(10, 4))
    _ax.plot(
        rolling_signoff["day"].to_numpy(),
        rolling_signoff["actors"].to_numpy(),
        color="#3a6",
    )
    _ax.set_title(
        f"Active reviewers — distinct attributed in trailing "
        f"{int(rolling_days.value)}d window"
    )
    _ax.set_xlabel("Date")
    _ax.set_ylabel("Distinct reviewers")
    _ax.grid(True, alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(pl, rolling_days, rolling_distinct_actors, signoff_attr, teams):
    """Same trailing-window distinct-reviewer count, split three ways by
    team membership so "is the bench growing or shrinking" can be read
    per tier (maintainer-team / reviewer-team / other contributors)."""
    if teams is None:
        rolling_by_team = None
    else:
        _maint = teams.maintainers
        _rev = teams.reviewers
        _signoff_human = signoff_attr.filter(pl.col("attributed")).select(
            [pl.col("label_at").alias("at"), pl.col("inferred_actor").alias("actor")]
        )
        _classified = _signoff_human.with_columns(
            pl.col("actor")
            .str.to_lowercase()
            .map_elements(
                lambda a: "maintainer"
                if a in _maint
                else ("reviewer" if a in _rev else "other"),
                return_dtype=str,
            )
            .alias("team")
        )
        _per_team = {}
        for _team in ("maintainer", "reviewer", "other"):
            _sub = _classified.filter(pl.col("team") == _team).select(["at", "actor"])
            _per_team[_team] = rolling_distinct_actors(_sub, int(rolling_days.value))
        rolling_by_team = _per_team
    return (rolling_by_team,)


@app.cell
def _(mo, plt, rolling_by_team, rolling_days):
    if rolling_by_team is None:
        rolling_team_view = mo.md(
            "_Team snapshot unavailable — skipping per-team active-reviewer chart._"
        )
    else:
        _team_colors = {"maintainer": "#3a6", "reviewer": "#6aa3d8", "other": "#888"}
        _fig, _ax = plt.subplots(figsize=(10, 4))
        for _team, _df in rolling_by_team.items():
            if _df.height == 0:
                continue
            _ax.plot(
                _df["day"].to_numpy(),
                _df["actors"].to_numpy(),
                color=_team_colors[_team],
                label=_team,
            )
        _ax.set_title(
            f"Active reviewers by team — distinct attributed in trailing "
            f"{int(rolling_days.value)}d window"
        )
        _ax.set_xlabel("Date")
        _ax.set_ylabel("Distinct reviewers")
        _ax.legend(title="Team", loc="upper left", fontsize=8)
        _ax.grid(True, alpha=0.3)
        _fig.tight_layout()
        rolling_team_view = _fig
    rolling_team_view
    return


# Sections 2 & 4 are disabled to avoid surfacing individual reviewer /
# maintainer logins (the per-reviewer bar charts + table, and the bors
# "maintainers with no observed trigger" name list). The aggregate
# sections (1, 3, 5, 6) and team-overlay counts stay. Re-enable a cell by
# dropping `disabled=True`.
@app.cell(disabled=True)
def _(mo):
    mo.md("""
    ## 2. Per-reviewer counts

    Top-N reviewers by attributed `maintainer-merge` triggers. The
    long-tail histogram (right) shows how many reviewers sit in each
    volume bucket overall.
    """)
    return


@app.cell(disabled=True)
def _(pl, signoff_attr):
    per_reviewer = (
        signoff_attr.filter(pl.col("attributed"))
        .group_by("inferred_actor")
        .agg(
            [
                pl.len().alias("triggers"),
                pl.col("pull_request_id").n_unique().alias("distinct_prs"),
                pl.col("label_at").min().alias("first_seen"),
                pl.col("label_at").max().alias("last_seen"),
                pl.col("gap_seconds").median().alias("median_gap_s"),
            ]
        )
        .sort("triggers", descending=True)
        .rename({"inferred_actor": "actor"})
    )
    return (per_reviewer,)


@app.cell(disabled=True)
def _(per_reviewer, plt, top_n):
    _top = per_reviewer.head(int(top_n.value)).reverse()
    _fig, (_ax1, _ax2) = plt.subplots(
        1, 2, figsize=(13, max(4, 0.25 * int(top_n.value)))
    )
    _ax1.barh(
        _top["actor"].to_numpy(),
        _top["triggers"].to_numpy(),
        color="#6aa3d8",
    )
    _ax1.set_title(f"Top {int(top_n.value)} reviewers — maintainer-merge triggers")
    _ax1.set_xlabel("Attributed triggers")
    _vals = per_reviewer["triggers"].to_numpy()
    _ax2.hist(_vals, bins=30, color="#aaa")
    _ax2.set_yscale("log")
    _ax2.set_title("Long-tail histogram (all reviewers)")
    _ax2.set_xlabel("Triggers per reviewer")
    _ax2.set_ylabel("Reviewers (log scale)")
    _fig.tight_layout()
    _fig
    return


@app.cell(disabled=True)
def _(per_reviewer, teams):
    """Per-reviewer table annotated with team membership."""
    if teams is None:
        reviewer_table = per_reviewer
    else:
        _maint = teams.maintainers
        _rev = teams.reviewers
        reviewer_table = per_reviewer.with_columns(
            per_reviewer["actor"]
            .map_elements(
                lambda a: "maintainer"
                if a.lower() in _maint
                else ("reviewer" if a.lower() in _rev else "other"),
                return_dtype=str,
            )
            .alias("team")
        )
    reviewer_table
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Concentration — Lorenz curve, Gini, and bus factor

    Yearly buckets. Lorenz curve plots cumulative share of reviewers
    (x) vs cumulative share of attributed triggers (y). Gini is in
    [0, 1] — 0 is perfect equality, 1 is one person doing everything.
    **Bus factor** = smallest N reviewers covering ≥50 % / ≥80 % of
    triggers.
    """)
    return


@app.cell
def _(np, pl, signoff_attr):
    def _gini(values: np.ndarray) -> float:
        if values.size == 0:
            return float("nan")
        v = np.sort(values.astype(float))
        n = v.size
        if v.sum() == 0:
            return 0.0
        idx = np.arange(1, n + 1)
        return float((2 * (idx * v).sum() - (n + 1) * v.sum()) / (n * v.sum()))

    def _coverage_bus_factor(values: np.ndarray, share: float) -> int:
        if values.size == 0:
            return 0
        v = np.sort(values.astype(float))[::-1]
        cum = np.cumsum(v)
        target = share * v.sum()
        return int(np.searchsorted(cum, target) + 1)

    yearly = (
        signoff_attr.filter(pl.col("attributed"))
        .with_columns(pl.col("label_at").dt.year().alias("year"))
        .group_by(["year", "inferred_actor"])
        .agg(pl.len().alias("triggers"))
        .sort(["year", "triggers"], descending=[False, True])
    )
    _rows = []
    for _year, _sub in yearly.group_by("year", maintain_order=True):
        _y = int(_year[0]) if isinstance(_year, tuple) else int(_year)
        _vals = _sub["triggers"].to_numpy()
        _rows.append(
            {
                "year": _y,
                "active_reviewers": int(_vals.size),
                "total_triggers": int(_vals.sum()),
                "gini": round(_gini(_vals), 3),
                "bus_factor_50": _coverage_bus_factor(_vals, 0.5),
                "bus_factor_80": _coverage_bus_factor(_vals, 0.8),
            }
        )
    concentration = pl.DataFrame(_rows).sort("year")
    return concentration, yearly


@app.cell
def _(concentration, mo):
    mo.md("### Yearly concentration")
    concentration
    return


@app.cell
def _(concentration, plt):
    _df = concentration.sort("year")
    _years = _df["year"].to_numpy()
    _fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(12, 4))
    _ax1.plot(_years, _df["gini"].to_numpy(), marker="o", color="#c33")
    _ax1.set_title("Gini coefficient over time")
    _ax1.set_xlabel("Year")
    _ax1.set_ylabel("Gini")
    _ax1.set_ylim(0, 1)
    _ax1.grid(True, alpha=0.3)
    _ax2.plot(
        _years, _df["bus_factor_50"].to_numpy(), marker="o", label="50 %", color="#3a6"
    )
    _ax2.plot(
        _years, _df["bus_factor_80"].to_numpy(), marker="o", label="80 %", color="#d80"
    )
    _ax2.set_title("Bus factor — reviewers covering X % of triggers")
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel("Reviewers")
    _ax2.legend()
    _ax2.grid(True, alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(np, plt, yearly):
    _years = sorted(yearly["year"].unique().to_list())
    _fig, _ax = plt.subplots(figsize=(7, 6))
    _ax.plot([0, 1], [0, 1], linestyle="--", color="#999", label="equality")
    _cmap = plt.get_cmap("viridis")
    for _i, _year in enumerate(_years):
        _vals = (
            yearly.filter(yearly["year"] == _year)["triggers"]
            .sort()
            .to_numpy()
            .astype(float)
        )
        if _vals.sum() == 0:
            continue
        _cum = np.cumsum(_vals) / _vals.sum()
        _x = np.arange(1, _vals.size + 1) / _vals.size
        _ax.plot(
            np.concatenate(([0], _x)),
            np.concatenate(([0], _cum)),
            color=_cmap(_i / max(len(_years) - 1, 1)),
            label=str(_year),
        )
    _ax.set_xlabel("Cumulative share of reviewers")
    _ax.set_ylabel("Cumulative share of triggers")
    _ax.set_title("Lorenz curves by year")
    _ax.legend(loc="upper left", fontsize=8)
    _ax.grid(True, alpha=0.3)
    _fig.tight_layout()
    _fig
    return


# Disabled — see the §2 note above (avoids surfacing individual logins).
@app.cell(disabled=True)
def _(mo):
    mo.md("""
    ## 4. Bors-trigger attribution

    `ready-to-merge` is bot-applied in response to a `bors r+` /
    `bors merge` comment, so we attribute it the same way. These are
    the humans actually triggering bors merges.
    """)
    return


@app.cell(disabled=True)
def _(bors_attr, pl, teams, top_n):
    per_bors = (
        bors_attr.filter(pl.col("attributed"))
        .group_by("inferred_actor")
        .agg(
            [
                pl.len().alias("ready_to_merge_triggers"),
                pl.col("pull_request_id").n_unique().alias("distinct_prs"),
            ]
        )
        .sort("ready_to_merge_triggers", descending=True)
        .rename({"inferred_actor": "actor"})
    )
    if teams is not None:
        _maint = teams.maintainers
        _rev = teams.reviewers
        per_bors = per_bors.with_columns(
            per_bors["actor"]
            .map_elements(
                lambda a: "maintainer"
                if a.lower() in _maint
                else ("reviewer" if a.lower() in _rev else "other"),
                return_dtype=str,
            )
            .alias("team")
        )
    per_bors_top = per_bors.head(int(top_n.value))
    return per_bors, per_bors_top


@app.cell(disabled=True)
def _(per_bors_top, plt):
    _top = per_bors_top.reverse()
    _fig, _ax = plt.subplots(figsize=(8, max(4, 0.25 * _top.height)))
    if "team" in _top.columns:
        _colors = {"maintainer": "#3a6", "reviewer": "#6aa3d8", "other": "#bbb"}
        _bar_colors = [_colors[t] for t in _top["team"].to_list()]
    else:
        _bar_colors = "#6aa3d8"
    _ax.barh(
        _top["actor"].to_numpy(),
        _top["ready_to_merge_triggers"].to_numpy(),
        color=_bar_colors,
    )
    _ax.set_xlabel("Attributed bors triggers")
    _ax.set_title("Top bors-trigger actors")
    _fig.tight_layout()
    _fig
    return


@app.cell(disabled=True)
def _(mo, per_bors, pl, teams):
    if teams is None or per_bors.height == 0:
        coverage_table = mo.md(
            "_Team snapshot unavailable — skipping team-overlap coverage table._"
        )
    else:
        _maint_set = teams.maintainers
        _seen_maintainers = {
            a.lower() for a in per_bors["actor"].to_list() if a.lower() in _maint_set
        }
        _unseen = sorted(_maint_set - _seen_maintainers)
        _by_team = per_bors.group_by("team").agg(
            [
                pl.len().alias("actors"),
                pl.col("ready_to_merge_triggers").sum().alias("triggers"),
            ]
        )
        coverage_table = mo.vstack(
            [
                mo.md(
                    f"**{len(_seen_maintainers)} / {len(_maint_set)}** maintainers "
                    "have triggered a bors merge in the attributed window."
                ),
                _by_team,
                mo.md(
                    "Maintainers with no observed bors trigger: "
                    + (", ".join(f"`{a}`" for a in _unseen) if _unseen else "_none_")
                ),
            ]
        )
    coverage_table
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. First-touch latency

    Time from PR open to the first non-author non-bot review-or-comment
    event. Closer to the metric authors feel than the Theme-2
    attribution-based metrics, which measure when the review loop
    *closes*.

    Two variants:

    - **broad** — counts `REVIEW_*` and `ISSUE_COMMENTED` as touches.
    - **strict** — `REVIEW_*` only (excludes top-level comments).
    """)
    return


@app.cell
def _(author_cohort, pr_type, prs_raw, size_buckets, users):
    """Join author_login + apply pr_shape decorators (Session 8 pattern)."""
    _prs_with_author = prs_raw.join(users, on="author_id", how="left")
    prs_shaped = pr_type(author_cohort(size_buckets(_prs_with_author)))
    prs_shaped_slim = prs_shaped.select(
        "id",
        "gh_created_at",
        "author_login",
        "lines_bucket",
        "pr_type",
        "is_first_pr",
        "merged_at",
        "state",
    )
    return (prs_shaped_slim,)


@app.cell
def _(events, first_review_touch, pl, prs_shaped_slim):
    """Compute both touch variants per PR and join back onto the shaped frame."""
    _strict_types = ("REVIEW_APPROVED", "REVIEW_COMMENTED", "REVIEW_CHANGES_REQUESTED")
    _broad = first_review_touch(prs_shaped_slim, events)
    _strict = first_review_touch(prs_shaped_slim, events, event_types=_strict_types)
    first_touch = (
        prs_shaped_slim.join(
            _broad.rename(
                {
                    "first_touch_at": "broad_at",
                    "first_touch_actor": "broad_actor",
                    "first_touch_event_type": "broad_type",
                    "first_touch_seconds_from_open": "broad_seconds",
                }
            ),
            left_on="id",
            right_on="pull_request_id",
            how="left",
        )
        .join(
            _strict.rename(
                {
                    "first_touch_at": "strict_at",
                    "first_touch_actor": "strict_actor",
                    "first_touch_event_type": "strict_type",
                    "first_touch_seconds_from_open": "strict_seconds",
                }
            ),
            left_on="id",
            right_on="pull_request_id",
            how="left",
        )
        .with_columns(
            (pl.col("broad_seconds") / 86400.0).alias("broad_days"),
            (pl.col("strict_seconds") / 86400.0).alias("strict_days"),
        )
    )
    return (first_touch,)


@app.cell
def _(first_touch, mo, pl):
    """Coverage: how many PRs ever got a qualifying touch?"""
    _total = first_touch.height
    _has_broad = first_touch.filter(pl.col("broad_at").is_not_null()).height
    _has_strict = first_touch.filter(pl.col("strict_at").is_not_null()).height
    _merged_no_broad = first_touch.filter(
        pl.col("broad_at").is_null() & pl.col("merged_at").is_not_null()
    ).height
    _closed_no_broad = first_touch.filter(
        pl.col("broad_at").is_null()
        & pl.col("merged_at").is_null()
        & (pl.col("state") == "closed")
    ).height
    first_touch_coverage = pl.DataFrame(
        [
            {"variant": "all PRs", "n": _total, "pct": 100.0},
            {
                "variant": "broad touch present",
                "n": _has_broad,
                "pct": round(100.0 * _has_broad / max(_total, 1), 1),
            },
            {
                "variant": "strict touch present",
                "n": _has_strict,
                "pct": round(100.0 * _has_strict / max(_total, 1), 1),
            },
            {
                "variant": "merged but no broad touch",
                "n": _merged_no_broad,
                "pct": round(100.0 * _merged_no_broad / max(_total, 1), 1),
            },
            {
                "variant": "closed-unmerged, no broad touch",
                "n": _closed_no_broad,
                "pct": round(100.0 * _closed_no_broad / max(_total, 1), 1),
            },
        ]
    )
    mo.md("### Coverage")
    first_touch_coverage
    return


@app.cell
def _(first_touch, np, pl, plt):
    """Distribution of first-touch latency (broad vs strict), ≤7d zoom."""
    _broad = first_touch.filter(
        pl.col("broad_days").is_not_null() & (pl.col("broad_days") <= 7)
    )["broad_days"].to_numpy()
    _strict = first_touch.filter(
        pl.col("strict_days").is_not_null() & (pl.col("strict_days") <= 7)
    )["strict_days"].to_numpy()
    _fig, _ax = plt.subplots(figsize=(10, 4))
    _bins = np.linspace(0, 7, 71)
    _ax.hist(_broad, bins=_bins, color="#6aa3d8", alpha=0.55, label="broad")
    _ax.hist(_strict, bins=_bins, color="#c63", alpha=0.55, label="strict")
    _ax.set_xlabel("Days from PR open → first touch")
    _ax.set_ylabel("PRs")
    _ax.set_title("First-touch latency (≤7d zoom)")
    _ax.legend()
    _ax.grid(True, alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(first_touch, np, pl):
    """Headline percentiles (hours) for both variants."""

    def _pcts(arr: np.ndarray) -> dict:
        if arr.size == 0:
            return {
                "n": 0,
                "median_h": None,
                "p75_h": None,
                "p90_h": None,
                "p99_h": None,
            }
        return {
            "n": int(arr.size),
            "median_h": round(float(np.median(arr) * 24), 2),
            "p75_h": round(float(np.percentile(arr, 75) * 24), 2),
            "p90_h": round(float(np.percentile(arr, 90) * 24), 2),
            "p99_h": round(float(np.percentile(arr, 99) * 24), 2),
        }

    _broad_arr = first_touch.filter(pl.col("broad_days").is_not_null())[
        "broad_days"
    ].to_numpy()
    _strict_arr = first_touch.filter(pl.col("strict_days").is_not_null())[
        "strict_days"
    ].to_numpy()
    first_touch_pcts = pl.DataFrame(
        [
            {"variant": "broad", **_pcts(_broad_arr)},
            {"variant": "strict", **_pcts(_strict_arr)},
        ]
    )
    first_touch_pcts
    return


@app.cell
def _(first_touch, mo, pl, plt):
    """Monthly trend of broad first-touch latency by gh_created_at cohort."""
    _df = (
        first_touch.filter(pl.col("broad_seconds").is_not_null())
        .with_columns(pl.col("gh_created_at").dt.truncate("1mo").alias("month"))
        .group_by("month")
        .agg(
            pl.len().alias("n"),
            pl.col("broad_seconds").median().alias("median_s"),
            pl.col("broad_seconds").quantile(0.9).alias("p90_s"),
        )
        .sort("month")
        .with_columns(
            (pl.col("median_s") / 3600.0).alias("median_h"),
            (pl.col("p90_s") / 3600.0).alias("p90_h"),
        )
        .filter(pl.col("n") >= 50)
    )
    _fig, _ax = plt.subplots(figsize=(11, 4))
    _ax.plot(
        _df["month"].to_numpy(),
        _df["median_h"].to_numpy(),
        label="median",
        color="#3a6",
    )
    _ax.plot(
        _df["month"].to_numpy(),
        _df["p90_h"].to_numpy(),
        label="p90",
        color="#c63",
    )
    _ax.set_yscale("log")
    _ax.set_ylabel("Hours (log)")
    _ax.set_xlabel("PR-open month")
    _ax.set_title("First-touch latency by cohort month (broad)")
    _ax.legend()
    _ax.grid(True, alpha=0.3, which="both")
    _fig.tight_layout()
    mo.md("### Monthly trend (broad)")
    _fig
    return


@app.cell
def _(first_touch, mo, np, pl):
    """Cuts by lines_bucket / pr_type / is_first_pr (broad variant)."""

    def _summary(df: pl.DataFrame, key: str) -> pl.DataFrame:
        rows = []
        for _name, _sub in df.group_by(key, maintain_order=False):
            _vals = _sub.filter(pl.col("broad_seconds").is_not_null())[
                "broad_seconds"
            ].to_numpy()
            if _vals.size == 0:
                continue
            rows.append(
                {
                    key: (_name[0] if isinstance(_name, tuple) else _name),
                    "n": int(_vals.size),
                    "median_h": round(float(np.median(_vals) / 3600.0), 2),
                    "p90_h": round(float(np.percentile(_vals, 90) / 3600.0), 2),
                }
            )
        return pl.DataFrame(rows).sort("median_h")

    cuts_lines = _summary(first_touch, "lines_bucket")
    cuts_type = _summary(first_touch, "pr_type")
    cuts_first = _summary(first_touch, "is_first_pr")
    mo.vstack(
        [
            mo.md("### By PR shape (broad)"),
            mo.md("**By `lines_bucket`**"),
            cuts_lines,
            mo.md("**By `pr_type`**"),
            cuts_type,
            mo.md("**By `is_first_pr`**"),
            cuts_first,
        ]
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## 6. Subpopulation responsiveness — requests & manual assignments

    Reviewers aren't interchangeable. This section asks whether
    *cohorts* of reviewers differ systematically in how they respond
    to review requests and manual assignments. The framing is
    deliberately about subpopulations, not individuals — we want to
    spot patterns ("maintainers respond ~3× faster than non-team
    reviewers on average") rather than rank people.

    A **reviewer profile** is built per login from the signals already
    in this notebook:

    - **team_bucket** — `maintainer` if the login is in the
      `maintainers` team, else `reviewer` if in the `reviewers` team,
      else `other`. Requires the sibling teams checkout — falls back
      to `unknown` if missing.
    - **volume_tier** — `top` (≥ p90 of lifetime attributed MM
      triggers, min 10), `mid` (≥ p50, min 3), `low` (the rest).
      Reviewers with zero attributed triggers are bucketed as
      `inactive` and dropped from the cuts.
    - **tenure_year** — calendar year of the reviewer's first
      attributed MM trigger; null for reviewers we've never seen
      trigger MM (typically external folks getting review requests).

    For each axis we look at:

    - **Review-request responsiveness** — fraction of
      `REVIEW_REQUESTED` events directed at this cohort that drew a
      same-reviewer `REVIEW_*` response, plus median response latency.
    - **Manual-assignment follow-through** — for `ASSIGNED` events
      placed by non-bot actors, fraction where the *assignee* ended
      up being the attributed `maintainer-merge` trigger. (Bot
      assignments are excluded; the policy outcome is about whether
      manually-picked reviewers actually drive the PR through.)

    A **self-vs-other request cut** at the bottom compares requests
    where the requester equals the reviewer (the reviewer requesting
    themselves) against the cross-actor majority — a proxy for "do
    reviewers respond better when they nominated themselves?"
    """)
    return


@app.cell
def _(np, pl, signoff_attr, teams):
    """Per-reviewer profile: team_bucket, volume_tier, tenure_year.

    Built from `signoff_attr` (attributed maintainer-merge triggers)
    so volume tiers are calibrated on actual review activity rather
    than arbitrary headcount. Logins are normalised to lowercase to
    intersect cleanly with the teams sets (which are lowercased)."""
    _attr = signoff_attr.filter(pl.col("attributed")).with_columns(
        pl.col("inferred_actor").str.to_lowercase().alias("login")
    )

    _per_actor = _attr.group_by("login").agg(
        pl.len().alias("n_signoffs"),
        pl.col("label_at").min().dt.year().alias("first_signoff_year"),
    )

    if _per_actor.is_empty():
        _signoff_counts = np.array([], dtype=float)
    else:
        _signoff_counts = _per_actor.get_column("n_signoffs").to_numpy()
    _p50 = float(np.percentile(_signoff_counts, 50)) if _signoff_counts.size else 0.0
    _p90 = float(np.percentile(_signoff_counts, 90)) if _signoff_counts.size else 0.0

    _maintainer_set = teams.maintainers if teams is not None else frozenset()
    _reviewer_set = teams.reviewers if teams is not None else frozenset()

    reviewer_profile = _per_actor.with_columns(
        pl.when(pl.col("login").is_in(_maintainer_set))
        .then(pl.lit("maintainer"))
        .when(pl.col("login").is_in(_reviewer_set))
        .then(pl.lit("reviewer"))
        .when(pl.lit(teams is None))
        .then(pl.lit("unknown"))
        .otherwise(pl.lit("other"))
        .alias("team_bucket"),
        pl.when(pl.col("n_signoffs") >= max(_p90, 10))
        .then(pl.lit("top"))
        .when(pl.col("n_signoffs") >= max(_p50, 3))
        .then(pl.lit("mid"))
        .otherwise(pl.lit("low"))
        .alias("volume_tier"),
        pl.col("first_signoff_year").alias("tenure_year"),
    )
    return (reviewer_profile,)


@app.cell
def _(mo, pl, reviewer_profile):
    """Headline profile breakdown — how many reviewers per cohort?"""
    _by_team = reviewer_profile.group_by("team_bucket").agg(
        pl.len().alias("n_reviewers"),
        pl.col("n_signoffs").sum().alias("total_signoffs"),
    )
    _by_volume = reviewer_profile.group_by("volume_tier").agg(
        pl.len().alias("n_reviewers"),
        pl.col("n_signoffs").sum().alias("total_signoffs"),
    )
    mo.hstack(
        [
            mo.vstack([mo.md("**By team**"), _by_team]),
            mo.vstack([mo.md("**By volume tier**"), _by_volume]),
        ],
        gap=1,
    )
    return


@app.cell
def _(events, pl, review_request_responses, reviewer_profile):
    """Per-request response frame joined to the reviewer profile.

    The join key is the lowercased `requested_reviewer_login`. Requests
    targeted at logins we have no profile for (external folks) get the
    `team_bucket = "other"`, `volume_tier = "inactive"` defaults so
    they remain countable as a residual cohort."""
    _rr = review_request_responses(events).filter(
        pl.col("requested_reviewer_login").is_not_null()
    )
    _rr = _rr.with_columns(
        pl.col("requested_reviewer_login").str.to_lowercase().alias("login")
    )
    request_profile = _rr.join(reviewer_profile, on="login", how="left").with_columns(
        pl.col("team_bucket").fill_null("other"),
        pl.col("volume_tier").fill_null("inactive"),
    )
    return (request_profile,)


@app.cell
def _(mo, pl, request_profile):
    """Three responsiveness cuts: team / volume / tenure.

    Each row shows N requests in the cohort, response rate, and
    median latency for the responded subset. Cohorts with fewer than
    25 requests are dropped so cuts don't surface noise rows."""

    def _cut(group_col: str, min_n: int = 25):
        out = (
            request_profile.group_by(group_col)
            .agg(
                pl.len().alias("n_requests"),
                pl.col("responded").sum().alias("n_responded"),
                pl.col("response_gap_seconds")
                .filter(pl.col("responded"))
                .median()
                .alias("median_gap_s"),
                pl.col("response_gap_seconds")
                .filter(pl.col("responded"))
                .quantile(0.9)
                .alias("p90_gap_s"),
            )
            .with_columns(
                (pl.col("n_responded") / pl.col("n_requests"))
                .round(3)
                .alias("response_rate"),
                (pl.col("median_gap_s") / 3600.0).round(1).alias("median_h"),
                (pl.col("p90_gap_s") / 3600.0).round(1).alias("p90_h"),
            )
            .filter(pl.col("n_requests") >= min_n)
            .drop("median_gap_s", "p90_gap_s")
            .sort(group_col)
        )
        return out

    _by_team = _cut("team_bucket")
    _by_volume = _cut("volume_tier")
    _by_tenure = _cut("tenure_year")
    mo.vstack(
        [
            mo.md("### Review-request response rate by reviewer subpopulation"),
            mo.md("**By team_bucket**"),
            _by_team,
            mo.md("**By volume_tier** (calibrated on lifetime attributed sign-offs)"),
            _by_volume,
            mo.md(
                "**By tenure_year** (calendar year of first attributed sign-off). "
                "Null rows = reviewers we never saw trigger MM."
            ),
            _by_tenure,
        ]
    )
    return


@app.cell
def _(mo, pl, request_profile):
    """Self-requested vs other-requested cut.

    Self-requested events are rare (~1.5% of total) but informative —
    a reviewer requesting themselves is a strong signal of intent."""
    _classified = request_profile.with_columns(
        pl.when(pl.col("requested_by").str.to_lowercase() == pl.col("login"))
        .then(pl.lit("self"))
        .otherwise(pl.lit("other"))
        .alias("requester_kind")
    )
    _by_kind = (
        _classified.group_by(["requester_kind", "team_bucket"])
        .agg(
            pl.len().alias("n_requests"),
            pl.col("responded").sum().alias("n_responded"),
            pl.col("response_gap_seconds")
            .filter(pl.col("responded"))
            .median()
            .alias("median_gap_s"),
        )
        .with_columns(
            (pl.col("n_responded") / pl.col("n_requests"))
            .round(3)
            .alias("response_rate"),
            (pl.col("median_gap_s") / 3600.0).round(1).alias("median_h"),
        )
        .filter(pl.col("n_requests") >= 10)
        .drop("median_gap_s")
        .sort(["requester_kind", "team_bucket"])
    )
    mo.vstack(
        [
            mo.md("### Self-requested vs other-requested"),
            mo.md(
                "Cells with fewer than 10 requests are dropped. The `self` rows are "
                "small but show whether self-nomination correlates with a different "
                "response profile."
            ),
            _by_kind,
        ]
    )
    return


@app.cell
def _(
    classify_assignment_events,
    events,
    pl,
    reviewer_profile,
    signoff_attr,
):
    """Manual-assignment follow-through by reviewer subpopulation.

    For each manually-applied ASSIGNED event (kind != bot), did the
    assignee end up being the attributed `maintainer-merge` trigger
    for that PR? Stratified by the *assignee's* profile, since the
    policy question is about whether the assigned reviewer drives the
    PR through.

    A PR can have multiple manual assignments. We treat each
    (assignee, PR) pair as one observation."""
    _manual = (
        classify_assignment_events(events)
        .filter(pl.col("kind") != "bot")
        .filter(pl.col("assignee_login").is_not_null())
        .select("pull_request_id", "assignee_login", "kind")
        .unique()
        .with_columns(pl.col("assignee_login").str.to_lowercase().alias("login"))
    )

    _mm_trigger = (
        signoff_attr.filter(pl.col("attributed"))
        .sort("label_at")
        .group_by("pull_request_id", maintain_order=True)
        .agg(
            pl.col("inferred_actor")
            .first()
            .str.to_lowercase()
            .alias("mm_trigger_login")
        )
    )

    assignment_followthrough = (
        _manual.join(_mm_trigger, on="pull_request_id", how="left")
        .join(reviewer_profile, on="login", how="left")
        .with_columns(
            pl.col("team_bucket").fill_null("other"),
            pl.col("volume_tier").fill_null("inactive"),
            (pl.col("login") == pl.col("mm_trigger_login"))
            .fill_null(False)
            .alias("triggered_mm"),
        )
    )
    return (assignment_followthrough,)


@app.cell
def _(assignment_followthrough, mo, pl):
    """Manual-assignment follow-through cuts."""

    def _cut(group_col: str, min_n: int = 20):
        out = (
            assignment_followthrough.group_by(group_col)
            .agg(
                pl.len().alias("n_assignments"),
                pl.col("triggered_mm").sum().alias("n_followed_through"),
            )
            .with_columns(
                (pl.col("n_followed_through") / pl.col("n_assignments"))
                .round(3)
                .alias("followthrough_rate")
            )
            .filter(pl.col("n_assignments") >= min_n)
            .sort(group_col)
        )
        return out

    _by_team = _cut("team_bucket")
    _by_volume = _cut("volume_tier")
    _by_kind = _cut("kind")
    mo.vstack(
        [
            mo.md(
                "### Manual-assignment follow-through rate by assignee subpopulation"
            ),
            mo.md(
                "Per (assignee, PR) pair. `followthrough_rate` = fraction "
                "where the manually-assigned reviewer ended up being the "
                "attributed MM trigger for that PR."
            ),
            mo.md("**By assignee `team_bucket`**"),
            _by_team,
            mo.md("**By assignee `volume_tier`**"),
            _by_volume,
            mo.md(
                "**By assignment `kind`** — `self` = the assignee assigned "
                "themselves; `other_human` = a maintainer or third party did."
            ),
            _by_kind,
        ]
    )
    return


@app.cell
def _(asof, mo, window_minutes):
    mo.md(
        f"_Attribution window: {int(window_minutes.value)} minutes. "
        f"Data asof {asof.isoformat(timespec='seconds')}._"
    )
    return


if __name__ == "__main__":
    app.run()
