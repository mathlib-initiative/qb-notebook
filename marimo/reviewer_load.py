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
    from qb_notebook.pr_shape import author_cohort, pr_type, size_buckets
    from qb_notebook.review_states import (
        attribute_label_events,
        first_review_touch,
    )
    from qb_notebook.teams import load as load_teams

    return (
        Path,
        attribute_label_events,
        author_cohort,
        datetime,
        first_review_touch,
        load_pr_interval_data,
        load_teams,
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
    events = data["events"]
    prs_raw = data["prs"]
    # core_user isn't part of load_pr_interval_data; map author_id ->
    # github_login here so first_review_touch can compare actor vs author.
    _users = pl.read_parquet(_data_dir / "core_user.parquet")
    users = _users.select(
        pl.col("id").alias("author_id"),
        pl.col("github_login").alias("author_login"),
    )
    asof = datetime.now(tz=timezone.utc)
    return asof, events, prs_raw, users


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
def _(asof, mo, window_minutes):
    mo.md(
        f"_Attribution window: {int(window_minutes.value)} minutes. "
        f"Data asof {asof.isoformat(timespec='seconds')}._"
    )
    return


if __name__ == "__main__":
    app.run()
