"""Personal logs — mathlib4 PRs mentioned on Zulip over time."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import sys
    from pathlib import Path

    _repo_root = Path(__file__).resolve().parents[1]
    if str(_repo_root) not in sys.path:
        sys.path.insert(0, str(_repo_root))

    import matplotlib.pyplot as plt
    import polars as pl

    from qb_notebook import zulip_io

    CACHE_PATH = zulip_io.default_cache_path(zulip_io.DEFAULT_CHANNEL)
    return CACHE_PATH, pl, plt, zulip_io


@app.cell
def _():
    # Chart chrome and series colors from the validated light-mode palette
    # (see the dataviz reference palette). BAR/LINE are two steps of one blue
    # ramp — the daily count and its smoothed version are the same quantity, so
    # they share a hue rather than reading as two series.
    INK_PRIMARY = "#0b0b0b"
    INK_MUTED = "#898781"
    GRID = "#e1e0d9"
    AXIS = "#c3c2b7"
    BAR = "#86b6ef"
    LINE = "#2a78d6"
    return AXIS, BAR, GRID, INK_MUTED, INK_PRIMARY, LINE


@app.cell
def _(AXIS, GRID, INK_MUTED, INK_PRIMARY):
    def style_axes(ax, *, ylabel=None, title=None):
        """Recessive grid and axes, ink-colored text; no chartjunk."""
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for _side in ("top", "right"):
            ax.spines[_side].set_visible(False)
        for _side in ("left", "bottom"):
            ax.spines[_side].set_color(AXIS)
        ax.tick_params(colors=INK_MUTED, labelsize=9)
        if ylabel:
            ax.set_ylabel(ylabel, color=INK_PRIMARY, fontsize=10)
        if title:
            ax.set_title(title, color=INK_PRIMARY, fontsize=11, loc="left")
        ax.set_ylim(bottom=0)
        return ax

    return (style_axes,)


@app.cell
def _(mo):
    mo.md("""
    # Personal logs — mathlib4 PRs mentioned over time

    Counts links to `github.com/leanprover-community/mathlib4/pull/<n>` in the
    **`personal logs`** channel. Messages are read as Zulip's *rendered HTML*, so
    linkifier shorthand (`#12345`, `mathlib#12345`, `!4#12345`) is already
    expanded to real links and counts the same as a pasted URL.
    """)
    return


@app.cell
def _(mo):
    sync_button = mo.ui.run_button(label="Sync from Zulip")
    mo.hstack(
        [sync_button, mo.md("*fetches messages newer than the local cache*")],
        justify="start",
        gap=1,
    )
    return (sync_button,)


@app.cell
def _(CACHE_PATH, sync_button, zulip_io):
    if sync_button.value:
        messages = zulip_io.sync_channel_messages(cache_path=CACHE_PATH)
    else:
        messages = zulip_io.load_cached_messages(CACHE_PATH)
    return (messages,)


@app.cell
def _(messages, zulip_io):
    # Extracted before the guard below so the warning cell can tell "no cache" from
    # "cache, but nothing links a mathlib4 PR" — everything downstream needs at
    # least one reference (a zero-row frame divides by zero and lays out a
    # zero-panel grid), so the empty case has to stop the DAG either way.
    all_references = zulip_io.extract_pr_references(messages)
    return (all_references,)


@app.cell
def _(CACHE_PATH, all_references, messages, mo):
    # Displaying the guidance is kept separate from halting the DAG: passing it as
    # mo.stop's `output` does not survive every runner, so this cell always owns
    # the message and the next one always owns the stop.
    if messages.is_empty():
        _guidance = mo.md(f"""
        **No cached messages.** Populate the cache with

        ```
        uv run python -m scripts.sync_zulip_logs
        ```

        or press **Sync from Zulip** above. Both need a `zuliprc` with bot
        credentials in `$ZULIPRC`, `./zuliprc` or `~/.zuliprc`.

        Expected cache path: `{CACHE_PATH}`
        """).callout(kind="warn")
    elif all_references.is_empty():
        _plural = "" if messages.height == 1 else "s"
        _guidance = mo.md(f"""
        **No mathlib4 PR links** in the {messages.height:,} cached
        message{_plural}, so there is nothing to plot. If the channel really
        should have some, the cache may be stale — press **Sync from Zulip**
        above, or re-pull it with
        `uv run python -m scripts.sync_zulip_logs --full-refresh` (the sync is
        incremental, so edits to already-cached messages are not picked up).
        """).callout(kind="warn")
    else:
        _guidance = None
    _guidance
    return


@app.cell
def _(all_references, mo):
    mo.stop(all_references.is_empty())

    references = all_references
    return (references,)


@app.cell
def _(messages, mo, references):
    mo.md(f"""
    **{messages.height:,}** messages ·
    **{references.height:,}** mathlib4 PR references ·
    **{references["pr_number"].n_unique():,}** distinct PRs ·
    **{references["sender_full_name"].n_unique()}** people ·
    {messages["sent_at"].min():%Y-%m-%d} – {messages["sent_at"].max():%Y-%m-%d}
    """)
    return


@app.cell
def _(mo):
    window = mo.ui.slider(
        start=1,
        stop=60,
        step=1,
        value=14,
        label="rolling window (days)",
        show_value=True,
    )
    metric = mo.ui.dropdown(
        options={
            "distinct PRs mentioned": "prs_mentioned",
            "mentions (message × PR pairs)": "mentions",
            "log messages with a PR link": "messages",
            "people mentioning a PR": "senders",
        },
        value="distinct PRs mentioned",
        label="count",
    )
    mo.hstack([window, metric], justify="start", gap=2)
    return metric, window


@app.cell
def _(metric, mo, references, window, zulip_io):
    daily = zulip_io.with_rolling_mean(
        zulip_io.pr_mentions_per_day(references),
        columns=(metric.value,),
        window=window.value,
    )
    mo.md(f"## §1 {metric.selected_key} per day")
    return (daily,)


@app.cell
def _(BAR, LINE, daily, metric, plt, style_axes, window):
    _avg_col = f"{metric.value}_{window.value}d_avg"
    _pdf = daily.to_pandas()

    _fig, _ax = plt.subplots(figsize=(9.5, 4.5))
    _ax.bar(
        _pdf["date"],
        _pdf[metric.value],
        width=1.0,
        color=BAR,
        label="daily count",
    )
    _ax.plot(
        _pdf["date"],
        _pdf[_avg_col],
        color=LINE,
        linewidth=2,
        label=f"{window.value}d average",
    )
    _ax.legend(frameon=False, fontsize=9)
    style_axes(
        _ax,
        ylabel=metric.selected_key,
        title=f"mathlib4 PRs mentioned in 'personal logs' — {metric.selected_key}",
    )
    _fig.autofmt_xdate(rotation=45)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(daily, metric, mo, pl, references, window):
    _avg_col = f"{metric.value}_{window.value}d_avg"
    # Drop the leading incomplete-window rows explicitly: a descending Polars sort
    # puts nulls *first* by default, so sorting alone would peak on a null.
    _peak = (
        daily.filter(pl.col(_avg_col).is_not_null())
        .sort(_avg_col, descending=True)
        .head(1)
    )
    if _peak.height:
        _peak_note = (
            f"Peak {window.value}-day average: **{_peak[_avg_col].item():.1f}/day** "
            f"around {_peak['date'].item():%Y-%m-%d}."
        )
    else:
        _peak_note = f"No complete {window.value}-day window in range yet."

    mo.md(f"""
    Quiet days are counted as explicit zeros before smoothing, so the average is
    per *calendar* day, not per posting day — logs land weekly-ish, which is why
    the daily bars are spiky and the smoothed line is the readable series. The
    first {window.value - 1} days have no average (incomplete window), and the
    right-hand edge is still filling in as people post.

    {_peak_note} Channel-wide mean:
    **{references.height / daily.height:.1f}** mentions/day.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## §2 Per person

    One panel per person rather than overlapping lines — with this many series a
    single chart is unreadable, and a shared y-axis makes the panels directly
    comparable. Ordered by distinct PRs mentioned; everyone outside the top 6
    is folded into **Other**.
    """)
    return


@app.cell
def _(metric, references, window, zulip_io):
    references_grouped = zulip_io.label_top_senders(references, limit=6)
    daily_by_sender = zulip_io.with_rolling_mean(
        zulip_io.pr_mentions_per_day(references_grouped, group_by=["sender_group"]),
        columns=(metric.value,),
        window=window.value,
        over=["sender_group"],
    )
    return daily_by_sender, references_grouped


@app.cell
def _():
    import math

    return (math,)


@app.cell
def _(
    BAR,
    LINE,
    daily_by_sender,
    math,
    metric,
    pl,
    plt,
    references_grouped,
    style_axes,
    window,
):
    _avg_col = f"{metric.value}_{window.value}d_avg"
    # Panel order: most distinct PRs first, "Other" pinned last.
    _groups = (
        references_grouped.group_by("sender_group")
        .agg(pl.col("pr_number").n_unique().alias("prs"))
        .with_columns((pl.col("sender_group") == "Other").alias("is_other"))
        .sort(["is_other", "prs"], descending=[False, True])
        .get_column("sender_group")
        .to_list()
    )

    _ncols = 2
    _nrows = math.ceil(len(_groups) / _ncols)
    _fig, _axes = plt.subplots(
        _nrows, _ncols, figsize=(10, 1.9 * _nrows), sharex=True, sharey=True
    )
    _flat = _axes.flatten()
    for _ax, _group in zip(_flat, _groups):
        _pdf = daily_by_sender.filter(pl.col("sender_group") == _group).to_pandas()
        # One series per panel: the title carries identity, so no legend.
        _ax.fill_between(_pdf["date"], _pdf[_avg_col], color=BAR, linewidth=0)
        _ax.plot(_pdf["date"], _pdf[_avg_col], color=LINE, linewidth=1.5)
        style_axes(_ax, title=_group)
    for _ax in _flat[len(_groups) :]:
        _ax.set_visible(False)

    # `sharex` labels only the bottom row, so a column whose bottom slot is empty
    # would end up with no date axis at all. Label the lowest *visible* panel of
    # each column instead (and rotate by hand — `autofmt_xdate` would undo this
    # by re-hiding every non-bottom-row label).
    for _col in range(_ncols):
        _column = [_flat[_i] for _i in range(_col, len(_flat), _ncols)]
        _labelled = [_ax for _ax in _column if _ax.get_visible()][-1:]
        for _ax in _labelled:
            _ax.tick_params(labelbottom=True)
            for _tick in _ax.get_xticklabels():
                _tick.set_rotation(45)
                _tick.set_horizontalalignment("right")

    _fig.supylabel(
        f"{metric.selected_key} ({window.value}d avg)", fontsize=10, color="#0b0b0b"
    )
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo, references, zulip_io):
    mo.vstack(
        [
            mo.md(
                """
                ## §3 Per-person totals

                `prs_mentioned` counts each PR once per person however often they
                cite it; `mentions` counts every (message, PR) pair.
                """
            ),
            mo.ui.table(zulip_io.mention_summary(references), selection=None),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
