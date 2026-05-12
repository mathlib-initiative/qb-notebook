"""Theme 2 — Reviewer & maintainer load."""

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

    from qb_notebook.data_io import load_pr_interval_data
    from qb_notebook.review_states import attribute_label_events
    from qb_notebook.teams import load as load_teams

    return (
        Path,
        attribute_label_events,
        datetime,
        load_pr_interval_data,
        load_teams,
        np,
        pl,
        plt,
        timezone,
    )


@app.cell
def _(Path, datetime, load_pr_interval_data, timezone):
    _data_dir = Path(__file__).resolve().parents[1] / "data"
    data = load_pr_interval_data(_data_dir)
    events = data["events"]
    asof = datetime.now(tz=timezone.utc)
    return asof, events


@app.cell
def _(Path, load_teams, mo):
    _candidate = Path(__file__).resolve().parents[2] / "leanprover-community.github.io"
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
    top_n = mo.ui.slider(start=5, stop=50, step=1, value=20, label="Top-N reviewers")
    mo.hstack([window_minutes, rolling_days, top_n])
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


@app.cell
def _(mo):
    mo.md("""
    ## 2. Per-reviewer counts

    Top-N reviewers by attributed `maintainer-merge` triggers. The
    long-tail histogram (right) shows how many reviewers sit in each
    volume bucket overall.
    """)
    return


@app.cell
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


@app.cell
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


@app.cell
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


@app.cell
def _(mo):
    mo.md("""
    ## 4. Bors-trigger attribution

    `ready-to-merge` is bot-applied in response to a `bors r+` /
    `bors merge` comment, so we attribute it the same way. These are
    the humans actually triggering bors merges.
    """)
    return


@app.cell
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


@app.cell
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


@app.cell
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
def _(asof, mo, window_minutes):
    mo.md(
        f"_Attribution window: {int(window_minutes.value)} minutes. "
        f"Data asof {asof.isoformat(timespec='seconds')}._"
    )
    return


if __name__ == "__main__":
    app.run()
