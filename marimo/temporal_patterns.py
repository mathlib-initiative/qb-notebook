"""Temporal patterns — time-of-day, weekday, monthly seasonality (UTC)."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")

# Run interactively:  uv run marimo edit marimo/temporal_patterns.py
# Serve read-only:    uv run marimo run  marimo/temporal_patterns.py
#
# Four sections:
#   §1 Hour × weekday heatmaps for opens / first-touches / sign-offs / merges
#   §2 First-touch latency by PR open hour-of-day + weekday
#   §3 Monthly seasonality, YoY overlay + seasonal index
#   §4 Author vs first-reviewer activity-window overlap


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
    # Temporal patterns — UTC

    When (on the UTC clock) do mathlib4 PR-review events happen?

    - **§1** — hour-of-day × weekday heatmaps for four event streams:
      PR opens, first non-author touches, `maintainer-merge` sign-offs,
      and bors merges. Same axes per panel so the daily / weekly rhythms
      can be compared directly.
    - **§2** — first-touch latency cut by the *open* hour and weekday.
      Does opening on a Friday afternoon cost a weekend before anyone
      looks?
    - **§3** — monthly seasonality with year-over-year overlay so trend
      (Theme-2 growth) doesn't masquerade as seasonality. (Empirically:
      no summer dip, but **Nov-Dec are the busiest months by a wide
      margin**, opposite the intuitive holiday hypothesis.)
    - **§4** — per-actor inferred UTC active window (cheap timezone
      proxy) and per-PR overlap between the author's window and the
      first reviewer's window.

    Everything is plotted in UTC — that's what the queueboard ingest
    carries. A CET / CEST overlay would help (much of mathlib's
    maintainer team lives in Europe) but is left as a follow-up;
    interpretive notes in the headline cells call out the natural
    "Europe-daytime" range (~07–17 UTC summer, 08–18 UTC winter).
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
    from qb_notebook.review_states import (
        attribute_label_events,
        first_review_touch,
    )
    from qb_notebook.temporal import (
        MONTH_LABELS,
        WEEKDAY_LABELS,
        actor_activity_window,
        hour_set_overlap,
        weekday_hour_histogram,
        with_temporal_columns,
    )
    from qb_notebook.wasm_io import load_slimmed_data

    return (
        MONTH_LABELS,
        Path,
        WEEKDAY_LABELS,
        actor_activity_window,
        attribute_label_events,
        datetime,
        first_review_touch,
        hour_set_overlap,
        load_pr_interval_data,
        load_slimmed_data,
        merged_prs_frame,
        np,
        pl,
        plt,
        timezone,
        weekday_hour_histogram,
        with_temporal_columns,
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
    # core_user join so first_review_touch / activity-window analyses can
    # compare author identity vs event-actor identity. In WASM it ships as a
    # slimmed table under public/ (see scripts/export_wasm_data.py); locally
    # it's read directly off disk (it's not a load_pr_interval_data key).
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
    asof = datetime.now(tz=timezone.utc)
    return asof, events, prs_raw, users


@app.cell
def _(prs_raw, users):
    prs_with_author = prs_raw.join(users, on="author_id", how="left")
    return (prs_with_author,)


# ============================================================================
# §1. Hour-of-day × weekday heatmaps
# ============================================================================


@app.cell
def _(mo):
    mo.md("""
    ## 1. Hour-of-day × weekday heatmaps

    Four panels, one per event stream, counts over the full project
    history (UTC). The vertical axis is weekday (Mon → Sun); horizontal
    is UTC hour 0 → 23. Brighter cells = more events.

    - **PR opens** — `prs.gh_created_at`. Author behavior; widest
      audience (~38k PRs).
    - **First touches** — earliest non-author, non-bot review/comment
      event per PR (broad variant from Session 11). Reviewer responsiveness.
    - **`maintainer-merge` sign-offs** — `LABELED(maintainer-merge)`
      events directly. Reviewer "approve" rhythm.
    - **Bors merges** — `merged_at_effective` on merged PRs. End-of-pipe
      cadence.
    """)
    return


@app.cell
def _(
    attribute_label_events,
    events,
    first_review_touch,
    merged_prs_frame,
    pl,
    prs_with_author,
    weekday_hour_histogram,
):
    opens_hist = weekday_hour_histogram(
        prs_with_author.select("gh_created_at"),
        ts_col="gh_created_at",
    )

    _ft = first_review_touch(prs_with_author, events)
    touches_hist = weekday_hour_histogram(
        _ft.select("first_touch_at"),
        ts_col="first_touch_at",
    )

    mm_attr = attribute_label_events(events, "maintainer-merge")
    mm_hist = weekday_hour_histogram(
        mm_attr.select(pl.col("label_at").alias("ts")),
        ts_col="ts",
    )

    _merged = merged_prs_frame(prs_with_author)
    merges_hist = weekday_hour_histogram(
        _merged.select("merged_at_effective"),
        ts_col="merged_at_effective",
    )
    return mm_attr, mm_hist, merges_hist, opens_hist, touches_hist


@app.cell
def _(
    WEEKDAY_LABELS,
    merges_hist,
    mm_hist,
    np,
    opens_hist,
    plt,
    touches_hist,
):
    def _to_grid(hist):
        arr = np.zeros((7, 24), dtype=float)
        for row in hist.iter_rows(named=True):
            arr[int(row["weekday"]), int(row["hour_utc"])] = row["count"]
        return arr

    _panels = [
        ("PR opens", _to_grid(opens_hist)),
        ("First touches (broad)", _to_grid(touches_hist)),
        ("maintainer-merge labels", _to_grid(mm_hist)),
        ("Bors merges", _to_grid(merges_hist)),
    ]
    _fig, _axes = plt.subplots(2, 2, figsize=(13, 7), sharex=True, sharey=True)
    for _ax, (_title, _arr) in zip(_axes.flat, _panels):
        _im = _ax.imshow(_arr, aspect="auto", cmap="magma")
        _ax.set_title(f"{_title} (n={int(_arr.sum()):,})")
        _ax.set_xticks(range(0, 24, 2))
        _ax.set_yticks(range(7))
        _ax.set_yticklabels(WEEKDAY_LABELS)
        plt.colorbar(_im, ax=_ax, fraction=0.04, pad=0.02)
    for _ax in _axes[-1]:
        _ax.set_xlabel("hour (UTC)")
    for _ax in _axes[:, 0]:
        _ax.set_ylabel("weekday")
    _fig.suptitle("Event volume by UTC weekday × hour")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ### Hour-of-day rollups (counts marginalised over weekday)

    Compresses each panel above to a 24-cell line so the daily rhythm
    is easier to read. Normalised to a daily mean of 1 — points above
    1.0 are busier-than-average hours.
    """)
    return


@app.cell
def _(merges_hist, mm_hist, np, opens_hist, plt, touches_hist):
    def _per_hour_share(hist):
        arr = np.zeros(24, dtype=float)
        for row in hist.iter_rows(named=True):
            arr[int(row["hour_utc"])] += row["count"]
        mean = arr.mean()
        return arr / mean if mean > 0 else arr

    _streams = [
        ("PR opens", opens_hist, "tab:blue"),
        ("First touches", touches_hist, "tab:orange"),
        ("maintainer-merge", mm_hist, "tab:green"),
        ("Bors merges", merges_hist, "tab:red"),
    ]
    _fig, _ax = plt.subplots(figsize=(11, 4))
    for _name, _hist, _color in _streams:
        _ax.plot(range(24), _per_hour_share(_hist), label=_name, color=_color)
    _ax.axhline(1.0, color="black", linestyle="--", linewidth=0.6)
    _ax.set_xticks(range(0, 24, 2))
    _ax.set_xlabel("hour (UTC)")
    _ax.set_ylabel("count / mean hourly count")
    _ax.set_title("Hour-of-day rhythm by stream (1.0 = uniform-day average)")
    _ax.legend()
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(merges_hist, mm_hist, np, opens_hist, pl, touches_hist):
    """Quantify the daily desert and busiest hour per stream."""

    def _stream_summary(name, hist):
        per_hour = np.zeros(24)
        for row in hist.iter_rows(named=True):
            per_hour[int(row["hour_utc"])] += row["count"]
        total = per_hour.sum()
        mean = per_hour.mean()
        busiest = int(np.argmax(per_hour))
        quietest = int(np.argmin(per_hour))
        # Hours falling below 50 % of the mean -> "desert" hours.
        n_desert = int((per_hour < 0.5 * mean).sum())
        peak_ratio = float(per_hour[busiest] / mean) if mean > 0 else 0.0
        trough_ratio = float(per_hour[quietest] / mean) if mean > 0 else 0.0
        return {
            "stream": name,
            "n": int(total),
            "busiest_hour": busiest,
            "peak_x_mean": round(peak_ratio, 2),
            "quietest_hour": quietest,
            "trough_x_mean": round(trough_ratio, 2),
            "n_desert_hours_<0.5x": n_desert,
        }

    hour_rollup = pl.DataFrame(
        [
            _stream_summary("PR opens", opens_hist),
            _stream_summary("First touches", touches_hist),
            _stream_summary("maintainer-merge", mm_hist),
            _stream_summary("Bors merges", merges_hist),
        ]
    )
    hour_rollup
    return (hour_rollup,)


@app.cell
def _(merges_hist, mm_hist, np, opens_hist, pl, touches_hist):
    """Weekday rollups: weekend share per stream."""

    def _weekday_summary(name, hist):
        per_day = np.zeros(7)
        for row in hist.iter_rows(named=True):
            per_day[int(row["weekday"])] += row["count"]
        total = per_day.sum()
        weekday = per_day[:5].sum()
        weekend = per_day[5:].sum()
        # Per-weekday-day baseline (5 weekdays): % vs that uniform expectation.
        return {
            "stream": name,
            "n": int(total),
            "weekday_share": round(float(weekday / total), 3) if total else 0.0,
            "weekend_share": round(float(weekend / total), 3) if total else 0.0,
            "sat_share": round(float(per_day[5] / total), 3) if total else 0.0,
            "sun_share": round(float(per_day[6] / total), 3) if total else 0.0,
        }

    weekday_rollup = pl.DataFrame(
        [
            _weekday_summary("PR opens", opens_hist),
            _weekday_summary("First touches", touches_hist),
            _weekday_summary("maintainer-merge", mm_hist),
            _weekday_summary("Bors merges", merges_hist),
        ]
    )
    weekday_rollup
    return (weekday_rollup,)


# ============================================================================
# §2. First-touch latency by open hour / weekday
# ============================================================================


@app.cell
def _(mo):
    mo.md("""
    ## 2. First-touch latency by PR open hour / weekday

    Reuses Session 11's `first_review_touch` per-PR output. For each
    PR with a broad first-touch, bin by the **open** time's UTC weekday
    and hour, then report the median first-touch wait in hours. The
    heatmap surfaces "open-on-Friday-after-five gets ignored over the
    weekend" and similar patterns; the side-by-side weekday-only
    summary is the headline.
    """)
    return


@app.cell
def _(events, first_review_touch, pl, prs_with_author, with_temporal_columns):
    _ft = first_review_touch(prs_with_author, events)
    _open_decorated = (
        with_temporal_columns(
            prs_with_author.select("id", "gh_created_at"),
            "gh_created_at",
        )
        .rename(
            {
                "hour_utc": "open_hour_utc",
                "weekday": "open_weekday",
                "is_weekend": "open_is_weekend",
                "month": "open_month",
                "year": "open_year",
                "year_month": "open_year_month",
            }
        )
        .select(
            "id",
            "gh_created_at",
            "open_hour_utc",
            "open_weekday",
            "open_is_weekend",
            "open_month",
            "open_year",
            "open_year_month",
        )
    )
    first_touch_open = (
        _ft.join(_open_decorated, left_on="pull_request_id", right_on="id", how="left")
        .with_columns(
            (pl.col("first_touch_seconds_from_open") / 3600.0).alias(
                "first_touch_hours"
            )
        )
        .filter(pl.col("first_touch_hours").is_not_null())
    )
    return (first_touch_open,)


@app.cell
def _(WEEKDAY_LABELS, first_touch_open, np, pl, plt):
    _grid = (
        first_touch_open.group_by(["open_weekday", "open_hour_utc"])
        .agg(
            pl.median("first_touch_hours").alias("median_hours"),
            pl.len().alias("n"),
        )
        .sort(["open_weekday", "open_hour_utc"])
    )
    _arr = np.full((7, 24), np.nan)
    for _row in _grid.iter_rows(named=True):
        _arr[int(_row["open_weekday"]), int(_row["open_hour_utc"])] = _row[
            "median_hours"
        ]
    _fig, _ax = plt.subplots(figsize=(13, 4.5))
    _im = _ax.imshow(_arr, aspect="auto", cmap="viridis")
    _ax.set_xticks(range(0, 24, 2))
    _ax.set_yticks(range(7))
    _ax.set_yticklabels(WEEKDAY_LABELS)
    _ax.set_xlabel("open hour (UTC)")
    _ax.set_ylabel("open weekday")
    _ax.set_title("Median first-touch latency (hours) by PR open time")
    plt.colorbar(_im, ax=_ax, label="median hours")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(WEEKDAY_LABELS, first_touch_open, np, pl):
    """Weekday-of-open summary: median + p90 first-touch latency."""
    _weekday = (
        first_touch_open.group_by("open_weekday")
        .agg(
            pl.median("first_touch_hours").alias("median_h"),
            pl.col("first_touch_hours").quantile(0.9, "lower").alias("p90_h"),
            pl.len().alias("n"),
        )
        .sort("open_weekday")
        .with_columns(
            pl.col("open_weekday")
            .map_elements(lambda w: WEEKDAY_LABELS[int(w)], return_dtype=pl.String)
            .alias("weekday")
        )
        .select("weekday", "n", "median_h", "p90_h")
        .with_columns(
            pl.col("median_h").round(2),
            pl.col("p90_h").round(1),
        )
    )
    _ = np  # keep cell signature parallel; unused but reserves import
    weekday_latency = _weekday
    weekday_latency
    return (weekday_latency,)


@app.cell
def _(first_touch_open, pl):
    """Weekend-open vs weekday-open distribution headline."""
    weekend_vs_weekday = (
        first_touch_open.group_by("open_is_weekend")
        .agg(
            pl.median("first_touch_hours").alias("median_h"),
            pl.col("first_touch_hours").quantile(0.75, "lower").alias("p75_h"),
            pl.col("first_touch_hours").quantile(0.9, "lower").alias("p90_h"),
            pl.len().alias("n"),
        )
        .with_columns(
            pl.when(pl.col("open_is_weekend"))
            .then(pl.lit("weekend open"))
            .otherwise(pl.lit("weekday open"))
            .alias("bucket"),
            pl.col("median_h").round(2),
            pl.col("p75_h").round(1),
            pl.col("p90_h").round(1),
        )
        .select("bucket", "n", "median_h", "p75_h", "p90_h")
        .sort("bucket")
    )
    weekend_vs_weekday
    return (weekend_vs_weekday,)


# ============================================================================
# §3. Monthly seasonality
# ============================================================================


@app.cell
def _(mo):
    mo.md("""
    ## 3. Monthly seasonality

    The mathlib4 project has had substantial year-over-year growth
    (Theme 2's active-reviewer counts climb steadily), so a raw
    "events per calendar month" plot mostly shows trend. To separate
    seasonality from trend we:

    1. show year-over-year overlay lines (one line per year, x = month);
    2. compute a **seasonal index** — each month's mean share of its
       *year's* total, averaged across years. A value of 1.0 means
       "uniform"; <1.0 is a calendar dip, >1.0 is a peak.

    Empirical headline: across opens / first-touches / merges, **Nov
    and Dec are 1.8-2.2× the uniform expectation** (consistent
    end-of-year push) and **April is the trough at ~0.67×**. There is
    no clean summer dip — Aug is roughly average.
    """)
    return


@app.cell
def _(
    attribute_label_events,
    events,
    first_review_touch,
    merged_prs_frame,
    pl,
    prs_with_author,
    with_temporal_columns,
):
    # Per-stream monthly counts. Year and month columns added via the
    # temporal decorator; one frame per stream, schema (year, month, count).
    def _monthly_counts(df, ts_col):
        return (
            with_temporal_columns(df.select(ts_col).drop_nulls(), ts_col)
            .group_by(["year", "month"])
            .agg(pl.len().alias("count"))
            .sort(["year", "month"])
        )

    opens_monthly = _monthly_counts(prs_with_author, "gh_created_at")

    _ft_for_monthly = first_review_touch(prs_with_author, events).select(
        "first_touch_at"
    )
    touches_monthly = _monthly_counts(_ft_for_monthly, "first_touch_at")

    _mm_for_monthly = attribute_label_events(events, "maintainer-merge").select(
        "label_at"
    )
    mm_monthly = _monthly_counts(_mm_for_monthly, "label_at")

    _merged_for_monthly = merged_prs_frame(prs_with_author).select(
        "merged_at_effective"
    )
    merges_monthly = _monthly_counts(_merged_for_monthly, "merged_at_effective")

    return merges_monthly, mm_monthly, opens_monthly, touches_monthly


@app.cell
def _(MONTH_LABELS, opens_monthly, plt):
    """YoY overlay for PR opens — one line per year, x = month."""
    _years = sorted(opens_monthly["year"].unique().to_list())
    _cmap = plt.get_cmap("viridis", max(len(_years), 2))
    _fig, _ax = plt.subplots(figsize=(11, 4.5))
    for _i, _year in enumerate(_years):
        _sub = opens_monthly.filter(opens_monthly["year"] == _year).sort("month")
        _months = _sub["month"].to_list()
        _counts = _sub["count"].to_list()
        _ax.plot(_months, _counts, marker="o", label=str(_year), color=_cmap(_i))
    _ax.set_xticks(range(1, 13))
    _ax.set_xticklabels(MONTH_LABELS)
    _ax.set_xlabel("month")
    _ax.set_ylabel("PR opens / month")
    _ax.set_title("Monthly PR-open count by year (trend + seasonality)")
    _ax.legend(loc="upper left", fontsize=8, ncol=2)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(MONTH_LABELS, merges_monthly, mm_monthly, np, opens_monthly, pl, touches_monthly):
    """Seasonal index: mean of (month_count / year_total) across years.

    For each (year, month), compute month_count / year_total. Average
    across years to get an "average month-of-year share" net of trend.
    Multiplied by 12 so 1.0 = uniform-month expectation, >1.0 = peak.
    Years with fewer than 6 months of data are dropped to avoid biasing
    short tail-years.
    """

    def _seasonal_index(monthly_df):
        complete = (
            monthly_df.group_by("year")
            .agg(pl.len().alias("n_months"), pl.col("count").sum().alias("year_total"))
            .filter(pl.col("n_months") >= 6)
        )
        joined = monthly_df.join(complete, on="year", how="inner").with_columns(
            (pl.col("count") / pl.col("year_total")).alias("month_share")
        )
        index = (
            joined.group_by("month")
            .agg(
                (pl.col("month_share").mean() * 12).alias("seasonal_index"),
                pl.len().alias("n_years"),
            )
            .sort("month")
        )
        return index

    _streams = [
        ("PR opens", opens_monthly, "tab:blue"),
        ("First touches", touches_monthly, "tab:orange"),
        ("maintainer-merge", mm_monthly, "tab:green"),
        ("Bors merges", merges_monthly, "tab:red"),
    ]

    indices = {name: _seasonal_index(df) for name, df, _ in _streams}

    seasonal_index_table = pl.DataFrame(
        {
            "month": list(range(1, 13)),
            "month_label": list(MONTH_LABELS),
            **{
                name: [
                    round(
                        float(
                            indices[name]
                            .filter(pl.col("month") == m)["seasonal_index"]
                            .item()
                        ),
                        3,
                    )
                    if indices[name].filter(pl.col("month") == m).height
                    else None
                    for m in range(1, 13)
                ]
                for name, _, _ in _streams
            },
        }
    )
    _ = np  # unused but threaded through several cells
    seasonal_index_table
    return indices, seasonal_index_table


@app.cell
def _(MONTH_LABELS, indices, plt):
    """Plot of the seasonal index across the four streams."""
    _streams = [
        ("PR opens", "tab:blue"),
        ("First touches", "tab:orange"),
        ("maintainer-merge", "tab:green"),
        ("Bors merges", "tab:red"),
    ]
    _fig, _ax = plt.subplots(figsize=(11, 4.5))
    for _name, _color in _streams:
        _series = indices[_name].sort("month")
        _ax.plot(
            _series["month"].to_list(),
            _series["seasonal_index"].to_list(),
            marker="o",
            label=_name,
            color=_color,
        )
    _ax.axhline(1.0, color="black", linestyle="--", linewidth=0.6)
    _ax.set_xticks(range(1, 13))
    _ax.set_xticklabels(MONTH_LABELS)
    _ax.set_xlabel("month")
    _ax.set_ylabel("seasonal index (1.0 = uniform)")
    _ax.set_title("Seasonal index per stream, trend-removed (mean share × 12)")
    _ax.legend()
    _fig.tight_layout()
    _fig
    return


# ============================================================================
# §4. Author vs first-reviewer activity-window overlap
# ============================================================================


@app.cell
def _(mo):
    mo.md("""
    ## 4. Author vs first-reviewer activity-window overlap

    For each actor (PR author or event-actor) with at least 20
    timeline events, infer the best contiguous 8-hour UTC window.
    Then for each PR with both an author window and a first-touch
    actor window, count the hours of overlap (0 → 8). Median first-touch
    latency by overlap bucket surfaces whether timezone alignment
    actually matters in this dataset.

    Caveat: the inferred window is a coarse proxy and is biased toward
    "when is this person active on GitHub" rather than "what timezone
    are they in" — heavy late-night reviewers will look like they're
    several timezones off their nominal one. The cut is suggestive,
    not definitive.

    Empirical headline (surprising): **no clean relationship between
    author/reviewer overlap and first-touch latency**. Median
    first-touch hovers around 9-12 h across all overlap buckets
    (0 through 8 hours). The bimodal overlap distribution (large
    spike at 0 h, large spike at 6-8 h) shows real timezone segregation,
    but it doesn't translate to a latency penalty — likely because
    review on a long-running async queue isn't gated on synchronous
    availability.
    """)
    return


@app.cell
def _(actor_activity_window, events):
    actor_windows = actor_activity_window(
        events,
        window_hours=8,
        min_events=20,
    )
    actor_windows_summary = (
        actor_windows.sort("n_events", descending=True)
        .head(20)
        .select(
            "actor_login",
            "n_events",
            "peak_hour",
            "window_start",
            "window_end",
            "active_hours_share",
        )
    )
    actor_windows_summary
    return actor_windows, actor_windows_summary


@app.cell
def _(actor_windows, np, plt):
    """Distribution of peak hours across all qualifying actors."""
    _peaks = actor_windows["peak_hour"].to_list()
    _fig, _ax = plt.subplots(figsize=(11, 3.5))
    _ax.hist(_peaks, bins=range(25), edgecolor="black")
    _ax.set_xticks(range(0, 24, 2))
    _ax.set_xlabel("peak hour (UTC)")
    _ax.set_ylabel("number of actors")
    _ax.set_title(f"Per-actor peak UTC hour (n={len(_peaks)} actors with ≥20 events)")
    _ax.axvspan(
        7, 17, alpha=0.1, color="tab:orange", label="rough Europe daytime (07-17 UTC)"
    )
    _ax.legend()
    _ = np
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(actor_windows, events, first_review_touch, hour_set_overlap, pl, prs_with_author):
    """Per-PR author-vs-first-reviewer overlap. Drop PRs where either side
    is below the min_events threshold (i.e. no inferred window)."""
    _ft = first_review_touch(prs_with_author, events).select(
        "pull_request_id", "first_touch_actor", "first_touch_seconds_from_open"
    )
    _window_map = {
        row["actor_login"]: row["active_hours"]
        for row in actor_windows.iter_rows(named=True)
    }
    _pr_author = prs_with_author.select(
        pl.col("id").alias("pull_request_id"), "author_login"
    )
    _joined = _ft.join(_pr_author, on="pull_request_id", how="inner").filter(
        pl.col("author_login").is_not_null() & pl.col("first_touch_actor").is_not_null()
    )

    def _overlap(author, reviewer):
        a = _window_map.get(author.lower() if author else None)
        if a is None:
            # try original-cased key
            a = _window_map.get(author)
        r = _window_map.get(reviewer)
        if r is None and reviewer is not None:
            r = _window_map.get(reviewer.lower())
        if a is None or r is None:
            return None
        return hour_set_overlap(a, r)

    _overlap_rows = []
    for _row in _joined.iter_rows(named=True):
        _ov = _overlap(_row["author_login"], _row["first_touch_actor"])
        if _ov is None:
            continue
        _overlap_rows.append(
            {
                "pull_request_id": _row["pull_request_id"],
                "overlap_hours": _ov,
                "first_touch_hours": _row["first_touch_seconds_from_open"] / 3600.0,
            }
        )
    pr_overlap = pl.DataFrame(
        _overlap_rows,
        schema={
            "pull_request_id": pl.Int64,
            "overlap_hours": pl.Int64,
            "first_touch_hours": pl.Float64,
        },
    )
    return (pr_overlap,)


@app.cell
def _(pr_overlap, pl):
    """Distribution of overlap-hours and median first-touch latency by bucket."""
    overlap_summary = (
        pr_overlap.group_by("overlap_hours")
        .agg(
            pl.len().alias("n_prs"),
            pl.median("first_touch_hours").alias("median_first_touch_h"),
            pl.col("first_touch_hours")
            .quantile(0.9, "lower")
            .alias("p90_first_touch_h"),
        )
        .sort("overlap_hours")
        .with_columns(
            pl.col("median_first_touch_h").round(2),
            pl.col("p90_first_touch_h").round(1),
        )
    )
    overlap_summary
    return (overlap_summary,)


@app.cell
def _(overlap_summary, plt):
    _xs = overlap_summary["overlap_hours"].to_list()
    _ns = overlap_summary["n_prs"].to_list()
    _meds = overlap_summary["median_first_touch_h"].to_list()
    _fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(12, 4))
    _ax1.bar(_xs, _ns, edgecolor="black")
    _ax1.set_xlabel("overlap (hours, 0-8)")
    _ax1.set_ylabel("# PRs")
    _ax1.set_title("PR count by author × first-reviewer overlap")
    _ax2.plot(_xs, _meds, marker="o")
    _ax2.set_xlabel("overlap (hours)")
    _ax2.set_ylabel("median first-touch latency (hours)")
    _ax2.set_title("Median first-touch by overlap")
    _fig.tight_layout()
    _fig
    return


# ============================================================================
# Findings note (manually updated)
# ============================================================================


@app.cell
def _(mo):
    mo.md("""
    ## Notes

    Empirical findings on the current artifact live in
    `docs/review-analysis/sessions.md` under Session 14. The
    helper module `qb_notebook/temporal.py` (`with_temporal_columns`,
    `weekday_hour_histogram`, `actor_activity_window`,
    `hour_set_overlap`) is reusable from any other notebook that
    needs to cut on UTC time-of-day.

    Open follow-ups:

    - **Europe/Berlin overlay**: a secondary axis labelling each UTC
      hour with its CET/CEST clock time would make the headline
      heatmaps far easier to interpret. Punted to keep the helper
      surface UTC-only.
    - **Per-actor TZ inference**: §4 uses the empirical hour
      distribution as a proxy for "active hours" rather than
      attempting to infer a nominal timezone. A future variant could
      cluster actors into TZ bands and report latency by author-TZ ×
      reviewer-TZ matrices.
    - **Holiday calendar overlay**: §3 surfaces a Dec/Jan dip but
      doesn't annotate specific holidays. A small `holidays` package
      integration could mark Christmas / New Year / common European
      summer-vacation weeks.
    """)
    return


if __name__ == "__main__":
    app.run()
